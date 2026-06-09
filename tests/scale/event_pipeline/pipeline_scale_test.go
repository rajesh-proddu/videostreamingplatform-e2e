// Package event_pipeline holds scale tests for the platform's OWN Kafka
// producer/consumer code paths — not the broker.
//
// The sibling suite tests/scale/kafka_throughput constructs raw kafka-go
// writers/readers and benchmarks the broker ("can Kafka do 100k?"). These tests
// answer a different question: "does MY produce→consume code stay correct under
// sustained load?" They drive the real HTTP endpoints that make the platform
// publish events, and assert the real Python consumers drain them:
//
//	video→ES:       POST /videos  ─▶ metadataservice kafka.Producer ─▶ video-events
//	                                 ─▶ kafka-es-consumer ─▶ Elasticsearch
//	watch→Iceberg:  GET  /videos/{id}/download ─▶ dataservice kafka.Producer
//	                                 ─▶ watch-events ─▶ watch-history-consumer ─▶ Iceberg
//
// The headline is CORRECTNESS, not a rate: these hard-fail on event loss and on
// the consumer failing to drain within the grace window. The achieved rate is
// reported but only gated when SCALE_PIPELINE_MIN_RPS is set, because the local
// ceiling is metadataservice+MySQL and the single-replica Python consumer — not
// Kafka. Requires the full analytics stack up (services + Kafka + consumer + ES
// / Iceberg warehouse).
package event_pipeline

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func intEnv(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func durEnv(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func requireMetadata(t *testing.T, env *testutil.Env) {
	t.Helper()
	if code, err := env.Metadata.Health(); err != nil || code >= 500 {
		t.Skipf("metadata service unreachable at %s: code=%d err=%v", env.Cfg.MetadataServiceURL, code, err)
	}
}

// TestPipeline_VideoToES_Sustained drives POST /videos hard, then asserts the
// kafka-es-consumer indexed every created video into Elasticsearch (no loss),
// draining within the grace window.
func TestPipeline_VideoToES_Sustained(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireES(t)
	requireMetadata(t, env)

	videos := intEnv("SCALE_PIPELINE_VIDEOS", 2000)
	workers := intEnv("SCALE_PIPELINE_WORKERS", 32)
	minRPS := intEnv("SCALE_PIPELINE_MIN_RPS", 0)
	drainGrace := durEnv("SCALE_PIPELINE_DRAIN_GRACE", 60*time.Second)
	if testing.Short() {
		videos, workers, minRPS, drainGrace = 100, 8, 0, 20*time.Second
	}

	batchTag := "scalepipe" + strings.ReplaceAll(testutil.UniqueID(""), "-", "")

	// Canary: prove the video-events→ES path is live before driving load, so a
	// dead consumer fails fast and clearly instead of after the full grace wait.
	canaryESPipeline(t, env, batchTag)

	ids := make([]string, videos)
	var next, produceErrs int64
	produceStart := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&next, 1) - 1
				if i >= int64(videos) {
					return
				}
				v, _, err := env.Metadata.CreateVideo(&client.CreateVideoRequest{
					Title:       fmt.Sprintf("%s %d", batchTag, i),
					Description: "scale pipeline",
					SizeBytes:   1024,
				})
				if err != nil {
					atomic.AddInt64(&produceErrs, 1)
					continue
				}
				ids[i] = v.ID
			}
		}()
	}
	wg.Wait()
	produceElapsed := time.Since(produceStart)

	created := make([]string, 0, videos)
	for _, id := range ids {
		if id != "" {
			created = append(created, id)
		}
	}
	produced := len(created)
	t.Cleanup(func() { bulkDelete(env, created) })
	if produced == 0 {
		t.Fatalf("no videos created (all %d attempts failed)", videos)
	}
	produceRate := float64(produced) / produceElapsed.Seconds()

	// Drain: poll the EXACT id-count until it reaches `produced` or grace expires.
	deadline := time.Now().Add(drainGrace)
	var indexed int
	for {
		_ = env.ES.RefreshIndex()
		n, err := env.ES.CountByIDs(created)
		if err != nil {
			t.Fatalf("ES CountByIDs: %v", err)
		}
		indexed = n
		if indexed >= produced || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	e2eElapsed := time.Since(produceStart)
	consumeRate := float64(indexed) / e2eElapsed.Seconds()

	t.Logf("=== video→ES pipeline | producer=metadataservice consumer=kafka-es-consumer ===")
	t.Logf("  videos requested      = %d (%d workers)", videos, workers)
	t.Logf("  produced (HTTP 2xx)    = %d  (errs=%d)", produced, produceErrs)
	t.Logf("  produce elapsed/rate   = %s  %.0f videos/s", produceElapsed.Round(time.Millisecond), produceRate)
	t.Logf("  indexed in ES          = %d / %d", indexed, produced)
	t.Logf("  end-to-end elapsed     = %s  (%.0f events/s through the full pipeline)", e2eElapsed.Round(time.Millisecond), consumeRate)
	t.Logf("  drain grace            = %s   floor (MIN_RPS) = %d", drainGrace, minRPS)

	if indexed < produced {
		t.Fatalf("video-events→ES LOST events: produced=%d indexed=%d (missing %d) after %s drain grace — "+
			"consumer dropped messages or could not keep up", produced, indexed, produced-indexed, drainGrace)
	}
	if minRPS > 0 {
		if produceRate < float64(minRPS) {
			t.Fatalf("produce rate %.0f/s below floor %d/s", produceRate, minRPS)
		}
		if consumeRate < float64(minRPS) {
			t.Fatalf("end-to-end consume rate %.0f/s below floor %d/s", consumeRate, minRPS)
		}
	}
}

// TestPipeline_WatchToIceberg_Sustained drives GET /videos/{id}/download hard,
// then asserts the watch-history-consumer flushed parquet to Iceberg and caught
// up within the grace window.
//
// NOTE the assertion here is weaker than video→ES by nature: watch-events are
// append-only (a retry produces a duplicate row) and the consumer flushes in
// batches on idle, so we cannot count rows for an exact no-loss check from S3
// alone (that needs an Athena row count, out of local scope). We assert the
// consumer is alive and DRAINS — parquet grows under load then stabilizes — and
// report the achieved download/produce rate.
func TestPipeline_WatchToIceberg_Sustained(t *testing.T) {
	env := testutil.NewEnv(t)
	ice := env.IcebergS3(t) // skips if the warehouse is unreachable
	env.EnsureEntitled(t)

	watches := intEnv("SCALE_PIPELINE_WATCHES", 300)
	poolSize := intEnv("SCALE_PIPELINE_VIDEO_POOL", 20)
	workers := intEnv("SCALE_PIPELINE_WORKERS", 8)
	minRPS := intEnv("SCALE_PIPELINE_MIN_RPS", 0)
	drainGrace := durEnv("SCALE_PIPELINE_DRAIN_GRACE", 90*time.Second)
	if testing.Short() {
		watches, poolSize, workers, minRPS, drainGrace = 20, 4, 4, 0, 30*time.Second
	}

	// Setup: a small pool of uploaded, downloadable videos to drive watches against.
	pool := make([]string, 0, poolSize)
	for i := 0; i < poolSize; i++ {
		pool = append(pool, uploadDownloadableVideo(t, env, 4))
	}

	ctx := context.Background()
	// Canary: one download must produce a watch event the consumer flushes to Iceberg.
	canaryStart, err := ice.CountDataFiles(ctx)
	if err != nil {
		t.Fatalf("CountDataFiles: %v", err)
	}
	if _, _, err := env.Data.DownloadVideo(pool[0], "canary-user"); err != nil {
		t.Fatalf("canary download failed — dataservice not serving downloads: %v", err)
	}
	if _, err := ice.WaitForFileIncrease(ctx, canaryStart, 45*time.Second); err != nil {
		t.Fatalf("watch-events→Iceberg pipeline is NOT live (canary download produced no parquet in 45s) — "+
			"is watch-history-consumer running and consuming watch-events? %v", err)
	}

	// Drive the load.
	startCount, _ := ice.CountDataFiles(ctx)
	var next, dlErrs, okCount int64
	produceStart := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&next, 1) - 1
				if i >= int64(watches) {
					return
				}
				vid := pool[int(i)%len(pool)]
				if _, _, err := env.Data.DownloadVideo(vid, fmt.Sprintf("wuser-%d", i)); err != nil {
					atomic.AddInt64(&dlErrs, 1)
					continue
				}
				atomic.AddInt64(&okCount, 1)
			}
		}()
	}
	wg.Wait()
	produceElapsed := time.Since(produceStart)
	produced := atomic.LoadInt64(&okCount)
	if produced == 0 {
		t.Fatalf("no downloads succeeded (%d errors) — cannot exercise the watch pipeline", dlErrs)
	}
	produceRate := float64(produced) / produceElapsed.Seconds()

	// Drain: wait until the parquet file count stops growing (consumer caught up).
	endCount, stable := waitForStableFileCount(ctx, ice, drainGrace)
	e2eElapsed := time.Since(produceStart)

	t.Logf("=== watch→Iceberg pipeline | producer=dataservice consumer=watch-history-consumer ===")
	t.Logf("  downloads requested    = %d (%d workers, pool=%d videos)", watches, workers, len(pool))
	t.Logf("  produced (download 2xx)= %d  (errs=%d)", produced, dlErrs)
	t.Logf("  produce elapsed/rate   = %s  %.0f downloads/s", produceElapsed.Round(time.Millisecond), produceRate)
	t.Logf("  parquet files          = %d → %d  (stabilized=%v)", startCount, endCount, stable)
	t.Logf("  end-to-end elapsed     = %s", e2eElapsed.Round(time.Millisecond))
	t.Logf("  drain grace            = %s   floor (MIN_RPS) = %d", drainGrace, minRPS)

	if endCount <= startCount {
		t.Fatalf("watch-events→Iceberg produced NO new parquet after %d downloads (files %d→%d) — "+
			"watch-history-consumer is not draining", produced, startCount, endCount)
	}
	if !stable {
		t.Fatalf("watch-history-consumer did not catch up within %s drain grace (parquet still growing) — "+
			"the consumer is falling behind under this load", drainGrace)
	}
	if minRPS > 0 && produceRate < float64(minRPS) {
		t.Fatalf("download/produce rate %.0f/s below floor %d/s", produceRate, minRPS)
	}
}

// canaryESPipeline creates one video and fails loudly if it isn't indexed in ES
// within a short window — a fast, unambiguous "is the consumer alive?" check.
func canaryESPipeline(t *testing.T, env *testutil.Env, batchTag string) {
	t.Helper()
	v, _, err := env.Metadata.CreateVideo(&client.CreateVideoRequest{
		Title:       batchTag + " canary",
		Description: "canary",
		SizeBytes:   1,
	})
	if err != nil {
		t.Fatalf("canary CreateVideo failed — metadataservice not accepting writes: %v", err)
	}
	t.Cleanup(func() {
		if r, _ := env.Metadata.DeleteVideo(v.ID); r != nil {
			r.Body.Close()
		}
	})
	if _, err := env.ES.WaitForDoc(v.ID, true, 20*time.Second); err != nil {
		t.Fatalf("video-events→ES pipeline is NOT live (canary video %s never indexed in 20s) — "+
			"is kafka-es-consumer running and consuming video-events? %v", v.ID, err)
	}
}

// uploadDownloadableVideo creates a video and uploads a small object so it can be
// downloaded (which is what makes dataservice emit watch-events).
func uploadDownloadableVideo(t *testing.T, env *testutil.Env, sizeKB int) string {
	t.Helper()
	v := env.CreateTestVideo(t, testutil.UniqueTitle("wpipe"), int64(sizeKB*1024))
	init, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   v.ID,
		UserID:    "wpipe-uploader",
		TotalSize: int64(sizeKB * 1024),
	})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	if _, err := env.Data.UploadChunk(init.UploadID, 0, testutil.RandomBytes(sizeKB*1024)); err != nil {
		t.Fatalf("UploadChunk: %v", err)
	}
	if _, _, err := env.Data.CompleteUpload(init.UploadID); err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}
	return v.ID
}

// waitForStableFileCount polls the parquet count and reports the final count plus
// whether it stopped growing (quiet for `quiet`) before the grace expired.
func waitForStableFileCount(ctx context.Context, ice *client.IcebergS3Client, grace time.Duration) (int, bool) {
	const quiet = 10 * time.Second
	deadline := time.Now().Add(grace)
	last, _ := ice.CountDataFiles(ctx)
	lastChange := time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
		n, err := ice.CountDataFiles(ctx)
		if err != nil {
			continue
		}
		if n != last {
			last = n
			lastChange = time.Now()
		} else if time.Since(lastChange) >= quiet {
			return last, true
		}
	}
	return last, false
}

func bulkDelete(env *testutil.Env, ids []string) {
	const workers = 16
	ch := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range ch {
				if r, _ := env.Metadata.DeleteVideo(id); r != nil {
					r.Body.Close()
				}
			}
		}()
	}
	for _, id := range ids {
		ch <- id
	}
	close(ch)
	wg.Wait()
}
