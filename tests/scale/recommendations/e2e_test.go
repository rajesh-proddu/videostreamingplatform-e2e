package recommendations_scale

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// newScaleRecommendClient returns a RecommendClient with a generous timeout
// (5 min). The default cfg.HTTPTimeout is 30s, which is below a cold LLM
// generate.
func newScaleRecommendClient(baseURL string) *client.RecommendClient {
	return client.NewRecommendClient(baseURL, 5*time.Minute)
}

// seedUser inserts a watch-history of N synthetic videos for `user` and
// registers cleanup. It does NOT create real videos in metadataservice —
// recommendations retrieve will see the user_id has history (driving the
// "warm" path through the agent) but the video_ids dangle. That's the
// configuration we want for measuring agent throughput in isolation: the
// retrieve step pulls candidates from ES regardless.
func seedUser(t *testing.T, env *testutil.Env, user string, n int) {
	t.Helper()
	pg := env.PgVector(t)
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("scale-reco-hist-%s-%d", user, i)
	}
	env.SeedAndCleanupHistory(t, pg, user, ids)
}

// recommendOnce calls /recommend, returns latency and item count.
func recommendOnce(ctx context.Context, c *client.RecommendClient, userID string, k int) (time.Duration, int, error) {
	start := time.Now()
	resp, _, err := c.Recommend(ctx, &client.RecommendRequest{UserID: userID, Limit: k})
	d := time.Since(start)
	if err != nil {
		return d, 0, err
	}
	return d, len(resp.Recommendations), nil
}

func TestRecommend_E2E_SingleUser(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)

	user := testutil.UniqueID("scale-reco-user")
	seedUser(t, env, user, 5)

	rec := newScaleRecommendClient(env.Cfg.RecommendationServiceURL)

	n := 100
	if testing.Short() {
		n = 20
	}

	// Warmup — first call pays LLM model-load if not already warm.
	warmCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	if _, _, err := recommendOnce(warmCtx, rec, user, 10); err != nil {
		cancel()
		t.Fatalf("warmup recommend: %v", err)
	}
	cancel()

	lats := make([]time.Duration, 0, n)
	emptyCount := 0
	start := time.Now()
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		d, items, err := recommendOnce(ctx, rec, user, 10)
		cancel()
		if err != nil {
			t.Logf("recommend %d failed: %v", i, err)
			continue
		}
		if items == 0 {
			emptyCount++
		}
		lats = append(lats, d)
	}
	wall := time.Since(start)
	if len(lats) == 0 {
		t.Fatalf("no successful recommend calls")
	}
	s := summarize(lats)
	qps := float64(s.N) / wall.Seconds()
	t.Logf("[recommend_e2e_single] n=%d p50=%s p95=%s p99=%s max=%s qps=%.2f empty=%d",
		s.N, s.P50, s.P95, s.P99, s.Max, qps, emptyCount)
	if emptyCount > s.N/2 {
		t.Errorf("more than half responses empty (%d/%d) — recs service broken", emptyCount, s.N)
	}
}

func TestRecommend_E2E_Concurrent_8Users(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent 8-user test in -short mode")
	}
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)

	users := envIntOr("SCALE_RECO_USERS", 8)
	dur := envDurationOr("SCALE_RECO_DURATION", 60*time.Second)

	for i := 0; i < users; i++ {
		seedUser(t, env, fmt.Sprintf("scale-reco-conc-%d", i), 3)
	}

	rec := newScaleRecommendClient(env.Cfg.RecommendationServiceURL)

	// Warmup so the first user doesn't eat cold-load.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	_, _, _ = recommendOnce(ctx, rec, "scale-reco-conc-0", 10)
	cancel()

	type bucket struct {
		mu   sync.Mutex
		lats []time.Duration
	}
	buckets := make([]bucket, users)
	var totalCalls atomic.Int64
	var totalErrs atomic.Int64

	stop := time.Now().Add(dur)
	var wg sync.WaitGroup
	wallStart := time.Now()
	for u := 0; u < users; u++ {
		wg.Add(1)
		go func(uid int) {
			defer wg.Done()
			user := fmt.Sprintf("scale-reco-conc-%d", uid)
			for time.Now().Before(stop) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				d, _, err := recommendOnce(ctx, rec, user, 10)
				cancel()
				totalCalls.Add(1)
				if err != nil {
					totalErrs.Add(1)
					continue
				}
				buckets[uid].mu.Lock()
				buckets[uid].lats = append(buckets[uid].lats, d)
				buckets[uid].mu.Unlock()
			}
		}(u)
	}
	wg.Wait()
	wall := time.Since(wallStart)

	var allLats []time.Duration
	for u := 0; u < users; u++ {
		buckets[u].mu.Lock()
		allLats = append(allLats, buckets[u].lats...)
		s := summarize(buckets[u].lats)
		buckets[u].mu.Unlock()
		t.Logf("[recommend_e2e_conc user=%d] n=%d p50=%s p95=%s p99=%s", u, s.N, s.P50, s.P95, s.P99)
	}
	agg := summarize(allLats)
	qps := float64(agg.N) / wall.Seconds()
	t.Logf("[recommend_e2e_conc_total] users=%d dur=%s n=%d errs=%d p50=%s p95=%s p99=%s qps=%.2f",
		users, wall, agg.N, totalErrs.Load(), agg.P50, agg.P95, agg.P99, qps)
}

func TestRecommend_E2E_NewUserColdStart(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)

	rec := newScaleRecommendClient(env.Cfg.RecommendationServiceURL)

	n := 50
	if testing.Short() {
		n = 10
	}
	lats := make([]time.Duration, 0, n)
	emptyCount := 0
	for i := 0; i < n; i++ {
		userID := testutil.UniqueID("scale-reco-newuser")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		d, items, err := recommendOnce(ctx, rec, userID, 10)
		cancel()
		if err != nil {
			t.Logf("call %d: %v", i, err)
			continue
		}
		if items == 0 {
			emptyCount++
		}
		lats = append(lats, d)
	}
	if len(lats) == 0 {
		t.Fatalf("no successful recommend calls")
	}
	s := summarize(lats)
	t.Logf("[recommend_e2e_coldstart] n=%d p50=%s p95=%s p99=%s empty=%d (popular-fallback path)",
		s.N, s.P50, s.P95, s.P99, emptyCount)
}

func TestRecommend_E2E_LargeK(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large-k test in -short mode")
	}
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)

	user := testutil.UniqueID("scale-reco-largek")
	seedUser(t, env, user, 5)

	rec := newScaleRecommendClient(env.Cfg.RecommendationServiceURL)
	// Warmup.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	_, _, _ = recommendOnce(ctx, rec, user, 10)
	cancel()

	for _, k := range []int{5, 20, 50, 100} {
		lats := make([]time.Duration, 0, 10)
		itemsSeen := 0
		for i := 0; i < 10; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			d, items, err := recommendOnce(ctx, rec, user, k)
			cancel()
			if err != nil {
				t.Logf("k=%d call %d: %v", k, i, err)
				continue
			}
			lats = append(lats, d)
			itemsSeen = items
		}
		if len(lats) == 0 {
			t.Logf("[recommend_e2e_largek k=%d] no successful calls", k)
			continue
		}
		s := summarize(lats)
		t.Logf("[recommend_e2e_largek k=%d] n=%d p50=%s p95=%s p99=%s items_returned=%d",
			k, s.N, s.P50, s.P95, s.P99, itemsSeen)
	}
}

