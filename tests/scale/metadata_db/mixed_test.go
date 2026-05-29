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

// TestMixedWorkload runs a 70/20/10 read/write/delete mix for 2 minutes at
// SCALE_WORKERS workers (default 32). Reports per-operation p95 latency
// and aggregate throughput. This is the closest to "production-like" of
// the scale tests.
func TestMixedWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("scale: skipping mixed workload in -short mode")
	}
	env := newEnv(t)
	db := openDB(t)
	defer db.Close()

	ids := sampleIDs(t, db, 10_000)
	if len(ids) < 200 {
		t.Skipf("not enough IDs (%d) for mixed workload", len(ids))
	}
	workers := env.Cfg.ScaleWorkers
	if workers <= 0 {
		workers = 32
	}
	duration := 2 * time.Minute
	if env.Cfg.ScaleDuration > 0 && env.Cfg.ScaleDuration < duration {
		duration = env.Cfg.ScaleDuration
	}

	readStats := &latencyStats{}
	writeStats := &latencyStats{}
	delStats := &latencyStats{}
	var (
		readOK, readFail   atomic.Int64
		writeOK, writeFail atomic.Int64
		delOK, delFail     atomic.Int64

		// Track ephemeral writes so we can clean up after the run.
		createdMu  sync.Mutex
		createdIDs []string
	)

	deadline := time.Now().Add(duration)
	startRun := time.Now()
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(wid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(wid)))
			for time.Now().Before(deadline) {
				r := rng.Intn(100)
				switch {
				case r < 70:
					// READ: 50/50 list-with-jitter vs point-get
					if rng.Intn(2) == 0 {
						off := rng.Intn(10_000)
						start := time.Now()
						resp, err := env.Metadata.RawGet(fmt.Sprintf("/videos?limit=20&offset=%d", off))
						readStats.add(time.Since(start))
						if resp != nil {
							_ = resp.Body.Close()
						}
						if err != nil || (resp != nil && resp.StatusCode >= 400) {
							readFail.Add(1)
						} else {
							readOK.Add(1)
						}
					} else {
						id := ids[rng.Intn(len(ids))]
						start := time.Now()
						resp, err := env.Metadata.RawGet("/videos/" + id)
						readStats.add(time.Since(start))
						if resp != nil {
							_ = resp.Body.Close()
						}
						if err != nil || (resp != nil && resp.StatusCode >= 400) {
							readFail.Add(1)
						} else {
							readOK.Add(1)
						}
					}
				case r < 90:
					// WRITE: 50% updates on existing IDs, 50% inserts
					if rng.Intn(2) == 0 {
						id := ids[rng.Intn(len(ids))]
						title := fmt.Sprintf("mix-upd-%d-%d", wid, rng.Int63())
						start := time.Now()
						_, _, err := env.Metadata.UpdateVideo(id, &client.UpdateVideoRequest{Title: title})
						writeStats.add(time.Since(start))
						if err != nil {
							writeFail.Add(1)
						} else {
							writeOK.Add(1)
						}
					} else {
						title := fmt.Sprintf("mix-ins-%d-%d", wid, rng.Int63())
						start := time.Now()
						v, _, err := env.Metadata.CreateVideo(&client.CreateVideoRequest{Title: title, SizeBytes: 512})
						writeStats.add(time.Since(start))
						if err != nil || v == nil {
							writeFail.Add(1)
						} else {
							writeOK.Add(1)
							createdMu.Lock()
							createdIDs = append(createdIDs, v.ID)
							createdMu.Unlock()
						}
					}
				default:
					// DELETE: prefer deleting ephemeral writes; if none queued, skip to a read.
					createdMu.Lock()
					var target string
					if n := len(createdIDs); n > 0 {
						target = createdIDs[n-1]
						createdIDs = createdIDs[:n-1]
					}
					createdMu.Unlock()
					if target == "" {
						// nothing to delete yet
						continue
					}
					start := time.Now()
					resp, err := env.Metadata.DeleteVideo(target)
					delStats.add(time.Since(start))
					if resp != nil {
						_ = resp.Body.Close()
					}
					if err != nil || (resp != nil && resp.StatusCode >= 400) {
						delFail.Add(1)
					} else {
						delOK.Add(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(startRun)

	rp50, rp95, rp99, _ := readStats.summary()
	wp50, wp95, wp99, _ := writeStats.summary()
	dp50, dp95, dp99, _ := delStats.summary()
	total := readOK.Load() + writeOK.Load() + delOK.Load()
	t.Logf("workers=%d duration=%s total_ops=%d throughput=%s", workers, elapsed.Round(time.Second), total, fmtPerSec(total, elapsed))
	t.Logf("%-8s %-8s %-8s %-10s %-10s %-10s", "op", "ok", "fail", "p50", "p95", "p99")
	t.Logf("%s", "----------------------------------------------------------")
	t.Logf("%-8s %-8d %-8d %-10s %-10s %-10s", "read", readOK.Load(), readFail.Load(), rp50.Round(time.Millisecond), rp95.Round(time.Millisecond), rp99.Round(time.Millisecond))
	t.Logf("%-8s %-8d %-8d %-10s %-10s %-10s", "write", writeOK.Load(), writeFail.Load(), wp50.Round(time.Millisecond), wp95.Round(time.Millisecond), wp99.Round(time.Millisecond))
	t.Logf("%-8s %-8d %-8d %-10s %-10s %-10s", "delete", delOK.Load(), delFail.Load(), dp50.Round(time.Millisecond), dp95.Round(time.Millisecond), dp99.Round(time.Millisecond))

	// Cleanup any leftover inserts from the run.
	createdMu.Lock()
	leftovers := createdIDs
	createdMu.Unlock()
	if len(leftovers) > 0 {
		t.Logf("cleaning up %d leftover inserts from mixed run", len(leftovers))
		for _, id := range leftovers {
			resp, _ := env.Metadata.DeleteVideo(id)
			if resp != nil {
				_ = resp.Body.Close()
			}
		}
	}
}
