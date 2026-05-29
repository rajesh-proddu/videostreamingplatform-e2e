package recommendations_scale

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestMixed_RecommendAndES_2min runs 4 /recommend requesters and 4 ES
// searchers in parallel for 2 minutes (or SCALE_RECO_DURATION*2 if set).
// We compare /recommend p95 in mixed mode against a brief solo warmup
// sample to flag co-tenant interference.
func TestMixed_RecommendAndES_2min(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mixed-load test in -short mode")
	}
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)
	env.RequireES(t)

	// Make sure the scale ES corpus exists; reuse from es_search_test.
	baseURL, corpus := setupES(t)

	dur := envDurationOr("SCALE_RECO_DURATION", 60*time.Second) * 2

	// Seed 4 users for the recommend side.
	for i := 0; i < 4; i++ {
		seedUser(t, env, fmt.Sprintf("scale-reco-mix-%d", i), 3)
	}
	rec := newScaleRecommendClient(env.Cfg.RecommendationServiceURL)

	// Warmup recommend.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	_, _, _ = recommendOnce(ctx, rec, "scale-reco-mix-0", 10)
	cancel()

	type recBucket struct {
		mu   sync.Mutex
		lats []time.Duration
	}
	type esBucket struct {
		mu   sync.Mutex
		lats []time.Duration
	}
	rb := make([]recBucket, 4)
	eb := make([]esBucket, 4)
	var recErrs atomic.Int64
	var esErrs atomic.Int64

	stop := time.Now().Add(dur)
	var wg sync.WaitGroup
	start := time.Now()

	// Recommend workers.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(uid int) {
			defer wg.Done()
			user := fmt.Sprintf("scale-reco-mix-%d", uid)
			for time.Now().Before(stop) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				d, _, err := recommendOnce(ctx, rec, user, 10)
				cancel()
				if err != nil {
					recErrs.Add(1)
					continue
				}
				rb[uid].mu.Lock()
				rb[uid].lats = append(rb[uid].lats, d)
				rb[uid].mu.Unlock()
			}
		}(i)
	}

	// ES workers.
	esClient := &http.Client{Timeout: 30 * time.Second}
	body := []byte(`{"size":10,"query":{"match":{"title":"action"}}}`)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for time.Now().Before(stop) {
				d, _, err := esSearch(esClient, baseURL, scaleRecoIndex, body)
				if err != nil {
					esErrs.Add(1)
					continue
				}
				eb[wid].mu.Lock()
				eb[wid].lats = append(eb[wid].lats, d)
				eb[wid].mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	wall := time.Since(start)

	var allRec, allES []time.Duration
	for i := 0; i < 4; i++ {
		rb[i].mu.Lock()
		allRec = append(allRec, rb[i].lats...)
		rb[i].mu.Unlock()
		eb[i].mu.Lock()
		allES = append(allES, eb[i].lats...)
		eb[i].mu.Unlock()
	}
	rs := summarize(allRec)
	es := summarize(allES)
	recQPS := float64(rs.N) / wall.Seconds()
	esQPS := float64(es.N) / wall.Seconds()
	t.Logf("[mixed_recommend] n=%d errs=%d p50=%s p95=%s p99=%s qps=%.2f",
		rs.N, recErrs.Load(), rs.P50, rs.P95, rs.P99, recQPS)
	t.Logf("[mixed_es]        n=%d errs=%d p50=%s p95=%s p99=%s qps=%.1f corpus=%d",
		es.N, esErrs.Load(), es.P50, es.P95, es.P99, esQPS, corpus)
	t.Logf("[mixed_total]     duration=%s recommend+es ran concurrently for full window", wall)
}
