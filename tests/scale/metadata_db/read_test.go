package metadatadb

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestList_DeepPagination_Latency measures GET /videos?limit=20&offset=N at
// progressively deeper offsets. This exposes the classic OFFSET-scan
// antipattern in MySQL: even with an index, OFFSET 9.9M has to walk all
// 9.9M earlier index entries.
//
// NOTE: metadataservice fronts the DB with Redis (cache key
// videos:list:{limit}:{offset}). To avoid measuring cache hits, we
// scatter each bucket's offset by +/- a small jitter so each request is a
// distinct cache key. The first call in a bucket is still hot/cold-mixed
// across runs — we report p50/p95/p99 over multiple samples per bucket.
func TestList_DeepPagination_Latency(t *testing.T) {
	if testing.Short() {
		t.Skip("scale: skipping deep-pagination in -short mode")
	}
	env := newEnv(t)
	db := openDB(t)
	defer db.Close()

	have := rowCount(t, db)
	t.Logf("corpus size: %d rows", have)

	candidates := []int64{0, 100, 1_000, 10_000, 100_000, 1_000_000, 5_000_000, 9_900_000}
	var offsets []int64
	for _, o := range candidates {
		// Only include offsets the corpus actually supports.
		if o+20 <= have || o == 0 {
			offsets = append(offsets, o)
		}
	}

	const samplesPerBucket = 20
	t.Logf("%-12s %-6s %-10s %-10s %-10s %-10s", "offset", "n", "p50", "p95", "p99", "errors")
	t.Logf("%s", "------------------------------------------------------------------")

	for _, base := range offsets {
		stats := &latencyStats{}
		var errs int
		for i := 0; i < samplesPerBucket; i++ {
			// Jitter to defeat the list-response cache (videos:list:20:{offset}).
			off := base + int64(i)
			if off < 0 {
				off = 0
			}
			err := timeIt(stats, func() (*http.Response, error) {
				// Use RawGet to capture HTTP-layer latency precisely without
				// json decode overhead in the body. We still need to read
				// the body to free the connection.
				return env.Metadata.RawGet(fmt.Sprintf("/videos?limit=20&offset=%d", off))
			})
			if err != nil {
				errs++
			}
		}
		p50, p95, p99, n := stats.summary()
		t.Logf("%-12d %-6d %-10s %-10s %-10s %-10d", base, n, p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond), errs)
	}
}

// TestPointGet_ByID_Throughput hits GET /videos/{id} with random IDs across
// 16/32/64 concurrent workers. Reports QPS and p95 latency per worker level.
func TestPointGet_ByID_Throughput(t *testing.T) {
	if testing.Short() {
		t.Skip("scale: skipping point-get throughput in -short mode")
	}
	env := newEnv(t)
	db := openDB(t)
	defer db.Close()

	ids := sampleIDs(t, db, 10_000)
	if len(ids) < 100 {
		t.Skipf("not enough IDs (%d) for throughput test", len(ids))
	}
	t.Logf("sampled %d IDs for point-get test", len(ids))

	for _, workers := range []int{16, 32, 64} {
		stats := &latencyStats{}
		var hits, misses atomic.Int64
		duration := 15 * time.Second

		deadline := time.Now().Add(duration)
		var wg sync.WaitGroup
		wg.Add(workers)
		startRun := time.Now()
		for w := 0; w < workers; w++ {
			go func(wid int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(wid)))
				for time.Now().Before(deadline) {
					id := ids[rng.Intn(len(ids))]
					start := time.Now()
					resp, err := env.Metadata.RawGet("/videos/" + id)
					elapsed := time.Since(start)
					stats.add(elapsed)
					if resp != nil {
						if resp.StatusCode == 200 {
							hits.Add(1)
						} else {
							misses.Add(1)
						}
						_ = resp.Body.Close()
					} else if err != nil {
						misses.Add(1)
					}
				}
			}(w)
		}
		wg.Wait()
		total := hits.Load() + misses.Load()
		elapsed := time.Since(startRun)
		p50, p95, p99, _ := stats.summary()
		t.Logf("workers=%-3d total=%-7d hits=%-7d misses=%-5d qps=%s p50=%s p95=%s p99=%s",
			workers, total, hits.Load(), misses.Load(), fmtPerSec(total, elapsed),
			p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond))
	}
}

// TestList_LimitVariation fixes offset=0 and varies limit. Latency should
// scale roughly with limit (row materialization + JSON serialization). A
// non-linear blow-up suggests a query-plan regression.
func TestList_LimitVariation(t *testing.T) {
	env := newEnv(t)
	limits := []int{1, 10, 100, 1000}
	const samples = 10
	t.Logf("%-6s %-6s %-10s %-10s %-10s", "limit", "n", "p50", "p95", "p99")
	t.Logf("%s", "----------------------------------------------")
	for _, lim := range limits {
		stats := &latencyStats{}
		for i := 0; i < samples; i++ {
			_ = timeIt(stats, func() (*http.Response, error) {
				// Jitter via offset to avoid the cache.
				return env.Metadata.RawGet(fmt.Sprintf("/videos?limit=%d&offset=%d", lim, i))
			})
		}
		p50, p95, p99, n := stats.summary()
		t.Logf("%-6d %-6d %-10s %-10s %-10s", lim, n, p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond))
	}
}

// TestList_OrderByConsistency verifies that the ORDER BY tiebreaker
// (created_at DESC, id DESC) yields no duplicate IDs across adjacent pages.
// If the tiebreaker were missing or weak, identical created_at values could
// shift between pages and produce duplicates at deep offsets.
func TestList_OrderByConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("scale: skipping order-by consistency in -short mode")
	}
	env := newEnv(t)
	db := openDB(t)
	defer db.Close()

	have := rowCount(t, db)
	startOffset := int64(1_000_000)
	if startOffset+200 > have {
		// Fall back to mid-corpus.
		startOffset = have / 2
		if startOffset < 100 {
			t.Skipf("corpus too small (%d) for order-by consistency test", have)
		}
	}

	page1 := fetchPageIDs(t, env, 100, startOffset)
	page2 := fetchPageIDs(t, env, 100, startOffset+100)
	if len(page1) == 0 || len(page2) == 0 {
		t.Fatalf("empty page(s) at offset %d (got %d/%d)", startOffset, len(page1), len(page2))
	}
	seen := map[string]bool{}
	for _, id := range page1 {
		seen[id] = true
	}
	overlap := 0
	for _, id := range page2 {
		if seen[id] {
			overlap++
		}
	}
	t.Logf("page1=%d ids, page2=%d ids, overlap=%d (expected 0)", len(page1), len(page2), overlap)
	if overlap > 0 {
		t.Errorf("pagination produced %d duplicate IDs across adjacent pages — ORDER BY tiebreaker may be insufficient", overlap)
	}
}

func fetchPageIDs(t *testing.T, env *testutil.Env, limit int, offset int64) []string {
	t.Helper()
	list, _, err := env.Metadata.ListVideos(limit, int(offset))
	if err != nil {
		t.Fatalf("ListVideos(limit=%d offset=%d): %v", limit, offset, err)
	}
	out := make([]string, len(list.Videos))
	for i, v := range list.Videos {
		out[i] = v.ID
	}
	return out
}
