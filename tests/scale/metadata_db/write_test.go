package metadatadb

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
)

// TestInsert_Throughput_HTTP runs N concurrent workers issuing POST /videos
// for SCALE_DURATION and reports inserts/sec + p95 latency. Created rows are
// cleaned up at the end (best-effort).
func TestInsert_Throughput_HTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("scale: skipping insert throughput in -short mode")
	}
	env := newEnv(t)
	dur := env.Cfg.ScaleDuration

	var idMu sync.Mutex
	var ids []string

	for _, workers := range []int{8, 16, 32} {
		stats := &latencyStats{}
		var ok, fail atomic.Int64
		startRun := time.Now()
		deadline := startRun.Add(dur)
		var wg sync.WaitGroup
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func(wid int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(wid)))
				for time.Now().Before(deadline) {
					title := fmt.Sprintf("scale-insert-%d-%d", wid, rng.Int63())
					start := time.Now()
					v, _, err := env.Metadata.CreateVideo(&client.CreateVideoRequest{
						Title:     title,
						SizeBytes: 1024,
					})
					stats.add(time.Since(start))
					if err != nil {
						fail.Add(1)
						continue
					}
					ok.Add(1)
					idMu.Lock()
					ids = append(ids, v.ID)
					idMu.Unlock()
				}
			}(w)
		}
		wg.Wait()
		elapsed := time.Since(startRun)
		p50, p95, p99, _ := stats.summary()
		t.Logf("workers=%-3d ok=%-7d fail=%-5d inserts=%s p50=%s p95=%s p99=%s",
			workers, ok.Load(), fail.Load(), fmtPerSec(ok.Load(), elapsed),
			p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond))
	}

	// Cleanup created rows.
	t.Logf("cleaning up %d inserted rows", len(ids))
	for _, id := range ids {
		resp, _ := env.Metadata.DeleteVideo(id)
		if resp != nil {
			_ = resp.Body.Close()
		}
	}
}

// TestUpdate_Throughput pre-samples 10k existing IDs and runs N workers
// issuing PUT /videos/{id} for SCALE_DURATION. Reports updates/sec +
// p95 latency.
func TestUpdate_Throughput(t *testing.T) {
	if testing.Short() {
		t.Skip("scale: skipping update throughput in -short mode")
	}
	env := newEnv(t)
	db := openDB(t)
	defer db.Close()

	ids := sampleIDs(t, db, 10_000)
	if len(ids) < 100 {
		t.Skipf("not enough IDs (%d) for update test", len(ids))
	}
	dur := env.Cfg.ScaleDuration

	for _, workers := range []int{8, 16, 32} {
		stats := &latencyStats{}
		var ok, fail atomic.Int64
		startRun := time.Now()
		deadline := startRun.Add(dur)
		var wg sync.WaitGroup
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func(wid int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(wid)))
				for time.Now().Before(deadline) {
					id := ids[rng.Intn(len(ids))]
					title := fmt.Sprintf("scale-upd-%d-%d", wid, rng.Int63())
					start := time.Now()
					_, _, err := env.Metadata.UpdateVideo(id, &client.UpdateVideoRequest{Title: title})
					stats.add(time.Since(start))
					if err != nil {
						fail.Add(1)
						continue
					}
					ok.Add(1)
				}
			}(w)
		}
		wg.Wait()
		elapsed := time.Since(startRun)
		p50, p95, p99, _ := stats.summary()
		t.Logf("workers=%-3d ok=%-7d fail=%-5d updates=%s p50=%s p95=%s p99=%s",
			workers, ok.Load(), fail.Load(), fmtPerSec(ok.Load(), elapsed),
			p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond))
	}
}

// TestDelete_Throughput creates 10k throwaway videos via the HTTP API,
// then deletes them concurrently. Reports deletes/sec.
func TestDelete_Throughput(t *testing.T) {
	if testing.Short() {
		t.Skip("scale: skipping delete throughput in -short mode")
	}
	env := newEnv(t)
	const totalToCreate = 10_000

	t.Logf("seeding %d throwaway videos for delete test", totalToCreate)
	createStart := time.Now()
	ids := make([]string, 0, totalToCreate)
	var idMu sync.Mutex
	createWorkers := 16
	idCh := make(chan int, totalToCreate)
	for i := 0; i < totalToCreate; i++ {
		idCh <- i
	}
	close(idCh)
	var createWG sync.WaitGroup
	createWG.Add(createWorkers)
	for w := 0; w < createWorkers; w++ {
		go func(wid int) {
			defer createWG.Done()
			for i := range idCh {
				v, _, err := env.Metadata.CreateVideo(&client.CreateVideoRequest{
					Title:     fmt.Sprintf("scale-del-%d-%d", wid, i),
					SizeBytes: 256,
				})
				if err != nil || v == nil {
					continue
				}
				idMu.Lock()
				ids = append(ids, v.ID)
				idMu.Unlock()
			}
		}(w)
	}
	createWG.Wait()
	t.Logf("created %d/%d in %s", len(ids), totalToCreate, time.Since(createStart).Round(time.Millisecond))

	if len(ids) < 100 {
		t.Skipf("only created %d videos — too few for delete test", len(ids))
	}

	// Now delete concurrently and measure.
	for _, workers := range []int{16, 32} {
		// Use only a slice for each pass to avoid double-delete.
		if len(ids) == 0 {
			break
		}
		stats := &latencyStats{}
		var ok, fail atomic.Int64
		batchSize := len(ids) / 2
		if workers == 32 {
			batchSize = len(ids)
		}
		batch := ids[:batchSize]
		ids = ids[batchSize:]
		idxCh := make(chan int, len(batch))
		for i := range batch {
			idxCh <- i
		}
		close(idxCh)

		startRun := time.Now()
		var wg sync.WaitGroup
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for i := range idxCh {
					start := time.Now()
					resp, err := env.Metadata.DeleteVideo(batch[i])
					stats.add(time.Since(start))
					if resp != nil {
						_ = resp.Body.Close()
					}
					if err != nil || (resp != nil && resp.StatusCode >= 400) {
						fail.Add(1)
						continue
					}
					ok.Add(1)
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(startRun)
		p50, p95, p99, _ := stats.summary()
		t.Logf("workers=%-3d count=%-7d ok=%-7d fail=%-5d deletes=%s p50=%s p95=%s p99=%s",
			workers, len(batch), ok.Load(), fail.Load(), fmtPerSec(ok.Load(), elapsed),
			p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond))
	}
}
