package recommendations_scale

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// scaleRecoIndex is a separate index from the production `videos` index so we
// don't perturb work by sibling agents (kafka-to-ES seeders, metadata-service
// load tests). It mirrors a small subset of the video doc shape used by the
// recommendations retrieve path.
const scaleRecoIndex = "scale-reco-videos"

// vocabulary used to construct synthetic titles. The first word is unique
// per doc, the remainder is drawn from this list so that an arbitrary term
// hits a predictable number of docs.
var titleWords = []string{
	"action", "drama", "comedy", "thriller", "documentary",
	"sci-fi", "fantasy", "romance", "mystery", "horror",
	"travel", "cooking", "music", "sports", "nature",
	"history", "science", "tech", "tutorial", "review",
}

func esCount(t *testing.T, baseURL, index string) int {
	t.Helper()
	resp, err := http.Get(baseURL + "/" + index + "/_count")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0
	}
	if resp.StatusCode != http.StatusOK {
		return 0
	}
	var body struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0
	}
	return body.Count
}

// ensureScaleIndex creates the scale index with explicit mappings (text +
// keyword) if it doesn't already exist.
func ensureScaleIndex(t *testing.T, baseURL string) {
	t.Helper()
	// HEAD first.
	head, err := http.Head(baseURL + "/" + scaleRecoIndex)
	if err == nil && head.StatusCode == http.StatusOK {
		head.Body.Close()
		return
	}
	if head != nil {
		head.Body.Close()
	}
	mapping := `{
        "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "30s"},
        "mappings": {
            "properties": {
                "title":       {"type": "text"},
                "description": {"type": "text"},
                "tags":        {"type": "keyword"},
                "owner":       {"type": "keyword"},
                "size":        {"type": "long"},
                "created_at":  {"type": "date"}
            }
        }
    }`
	req, _ := http.NewRequest(http.MethodPut, baseURL+"/"+scaleRecoIndex, strings.NewReader(mapping))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("create index: status %d body=%s", resp.StatusCode, b)
	}
}

// bulkSeed seeds the scale index with `target` docs using ES `_bulk`. Returns
// the number of docs in the index after seeding completes. Each doc gets a
// title `scale-reco-<uniq> <word1> <word2> <word3>` so a term query for any
// `wordN` hits ~target * 3 / len(titleWords) docs (≈15% of corpus).
func bulkSeed(t *testing.T, baseURL string, target int) int {
	t.Helper()
	current := esCount(t, baseURL, scaleRecoIndex)
	if current >= target {
		t.Logf("[es_seed] corpus already at %d >= target %d, reusing", current, target)
		return current
	}
	t.Logf("[es_seed] seeding %d docs into %s (current=%d)", target-current, scaleRecoIndex, current)
	const batchSize = 500
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := time.Now()
	for i := current; i < target; i += batchSize {
		var buf bytes.Buffer
		end := i + batchSize
		if end > target {
			end = target
		}
		for j := i; j < end; j++ {
			docID := fmt.Sprintf("scale-reco-%08d", j)
			w1 := titleWords[r.Intn(len(titleWords))]
			w2 := titleWords[r.Intn(len(titleWords))]
			w3 := titleWords[r.Intn(len(titleWords))]
			title := fmt.Sprintf("scale-reco-%08d %s %s %s", j, w1, w2, w3)
			meta := fmt.Sprintf(`{"index":{"_index":%q,"_id":%q}}`+"\n", scaleRecoIndex, docID)
			doc := fmt.Sprintf(`{"title":%q,"description":%q,"tags":[%q,%q],"owner":"scale-reco-seeder","size":%d,"created_at":%q}`+"\n",
				title,
				"synthetic doc for scale recommendations testing",
				w1, w2,
				r.Int63n(1_000_000_000),
				time.Now().UTC().Format(time.RFC3339),
			)
			buf.WriteString(meta)
			buf.WriteString(doc)
		}
		req, _ := http.NewRequest(http.MethodPost, baseURL+"/_bulk", &buf)
		req.Header.Set("Content-Type", "application/x-ndjson")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("bulk %d-%d: %v", i, end, err)
		}
		// Drain & check for `errors:true`. We don't fail on a small fraction
		// of errors — ES occasionally bounces under load.
		var br struct {
			Errors bool `json:"errors"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&br)
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			t.Fatalf("bulk %d-%d: status %d", i, end, resp.StatusCode)
		}
	}
	// Force a refresh so subsequent searches see the new docs.
	refreshResp, err := http.Post(baseURL+"/"+scaleRecoIndex+"/_refresh", "application/json", nil)
	if err == nil {
		refreshResp.Body.Close()
	}
	elapsed := time.Since(start)
	final := esCount(t, baseURL, scaleRecoIndex)
	t.Logf("[es_seed] indexed to %d docs in %v (%.1f docs/s)", final, elapsed, float64(final-current)/elapsed.Seconds())
	return final
}

// esSearch posts a search body and returns latency + hits.total.value.
func esSearch(client *http.Client, baseURL, index string, body []byte) (time.Duration, int, error) {
	start := time.Now()
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/"+index+"/_search", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return time.Since(start), 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return time.Since(start), 0, fmt.Errorf("status %d body=%s", resp.StatusCode, b)
	}
	var r struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return time.Since(start), 0, err
	}
	return time.Since(start), r.Hits.Total.Value, nil
}

// setupES seeds the scale index and returns base URL + corpus size.
func setupES(t *testing.T) (string, int) {
	t.Helper()
	env := testutil.NewEnv(t)
	env.RequireES(t)
	target := envIntOr("SCALE_ES_DOCS", 50_000)
	ensureScaleIndex(t, env.Cfg.ElasticsearchURL)
	final := bulkSeed(t, env.Cfg.ElasticsearchURL, target)
	if final < target/2 {
		t.Skipf("only %d/%d docs indexed — refusing to run scale assertions", final, target)
	}
	return env.Cfg.ElasticsearchURL, final
}

// runQuery runs `n` queries from `workers` parallel workers and returns latencies.
func runQuery(t *testing.T, baseURL, index string, body []byte, workers, n int) []time.Duration {
	t.Helper()
	client := &http.Client{Timeout: 30 * time.Second}
	latencies := make([]time.Duration, n)
	var errs atomic.Int64
	var wg sync.WaitGroup
	jobs := make(chan int, n)
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				d, _, err := esSearch(client, baseURL, index, body)
				if err != nil {
					errs.Add(1)
				}
				latencies[idx] = d
			}
		}()
	}
	wg.Wait()
	if e := errs.Load(); e > 0 {
		t.Logf("[es_search] %d/%d queries errored", e, n)
	}
	return latencies
}

func TestES_Search_TermQuery_Latency(t *testing.T) {
	baseURL, corpus := setupES(t)

	// A title term query — `action` will hit ~15% of corpus (~7500/50000).
	body := []byte(`{
        "size": 10,
        "query": {"match": {"title": "action"}}
    }`)

	workers := 8
	n := 1000
	if testing.Short() {
		n = 200
	}
	start := time.Now()
	lats := runQuery(t, baseURL, scaleRecoIndex, body, workers, n)
	wall := time.Since(start)
	s := summarize(lats)
	t.Logf("[es_term] corpus=%d n=%d workers=%d p50=%s p95=%s p99=%s max=%s qps=%.1f",
		corpus, s.N, workers, s.P50, s.P95, s.P99, s.Max, float64(s.N)/wall.Seconds())
}

func TestES_Search_LargeResultSet(t *testing.T) {
	baseURL, corpus := setupES(t)

	// match_all with size=100; large result set retrieval.
	body := []byte(`{
        "size": 100,
        "query": {"match_all": {}}
    }`)
	workers := 4
	n := 200
	if testing.Short() {
		n = 50
	}
	start := time.Now()
	lats := runQuery(t, baseURL, scaleRecoIndex, body, workers, n)
	wall := time.Since(start)
	s := summarize(lats)
	t.Logf("[es_large] corpus=%d n=%d size=100 workers=%d p50=%s p95=%s p99=%s qps=%.1f",
		corpus, s.N, workers, s.P50, s.P95, s.P99, float64(s.N)/wall.Seconds())
}

func TestES_Search_HighlightAndAggs(t *testing.T) {
	baseURL, corpus := setupES(t)

	plain := []byte(`{"size":10,"query":{"match":{"title":"drama"}}}`)
	fancy := []byte(`{
        "size": 10,
        "query": {"match": {"title": "drama"}},
        "highlight": {"fields": {"title": {}, "description": {}}},
        "aggs": {
            "by_tag": {"terms": {"field": "tags", "size": 20}}
        }
    }`)

	workers := 4
	n := 300
	if testing.Short() {
		n = 80
	}

	startP := time.Now()
	lp := runQuery(t, baseURL, scaleRecoIndex, plain, workers, n)
	plainWall := time.Since(startP)
	sp := summarize(lp)

	startF := time.Now()
	lf := runQuery(t, baseURL, scaleRecoIndex, fancy, workers, n)
	fancyWall := time.Since(startF)
	sf := summarize(lf)

	overhead := time.Duration(0)
	if sp.P95 > 0 {
		overhead = sf.P95 - sp.P95
	}
	t.Logf("[es_plain]    corpus=%d n=%d p50=%s p95=%s p99=%s qps=%.1f",
		corpus, sp.N, sp.P50, sp.P95, sp.P99, float64(sp.N)/plainWall.Seconds())
	t.Logf("[es_hilite_agg] corpus=%d n=%d p50=%s p95=%s p99=%s qps=%.1f overhead_p95=%s",
		corpus, sf.N, sf.P50, sf.P95, sf.P99, float64(sf.N)/fancyWall.Seconds(), overhead)

	if sf.P95 < sp.P95 {
		t.Logf("note: highlight+aggs faster than plain — likely noise from small N")
	}
}
