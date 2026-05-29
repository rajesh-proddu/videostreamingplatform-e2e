package dataservice_io

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

// TestMixed_UploadDownload_4x16 runs 4 concurrent uploaders + 16 concurrent
// downloaders for SCALE_DURATION (default 2m) and reports aggregate MiB/s for
// each side plus error rates.
func TestMixed_UploadDownload_4x16(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mixed I/O in -short mode")
	}
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	duration := envDuration("SCALE_DURATION", 2*time.Minute)

	// Pre-stage a corpus of 3 small videos for downloaders to read from.
	corpus := buildCorpus(t, env, []int{5, 5, 5})

	uploadWorkers := envInt("SCALE_MIXED_UPLOADERS", 4)
	downloadWorkers := envInt("SCALE_MIXED_DOWNLOADERS", 16)
	const uploadSize = int64(10 * 1024 * 1024) // 10 MiB
	const uploadChunk = int64(5 * 1024 * 1024)

	dc := dataClientWithTimeout(env.Cfg, 10*time.Minute)
	hc := &http.Client{Timeout: 5 * time.Minute}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var (
		ulOk, ulErr, ulBytes int64
		dlOk, dlErr, dlBytes int64
	)
	var wg sync.WaitGroup

	// Uploaders: keep creating fresh videos + uploading in a loop.
	uploaderFn := func(workerID int) {
		defer wg.Done()
		for {
			if ctx.Err() != nil {
				return
			}
			// Create fresh video under test cleanup. CreateTestVideo registers
			// a t.Cleanup per call. Acceptable for the test duration.
			v := env.CreateTestVideo(t, testutil.UniqueTitle(fmt.Sprintf("mixed-ul-%d", workerID)), uploadSize)
			res := uploadSizedVideo(dc, v.ID, fmt.Sprintf("ul-%d", workerID), uploadSize, uploadChunk)
			if res.Err != nil {
				atomic.AddInt64(&ulErr, 1)
				continue
			}
			atomic.AddInt64(&ulOk, 1)
			atomic.AddInt64(&ulBytes, res.Bytes)
		}
	}

	// Downloaders: pick a random corpus video and stream it end-to-end.
	downloaderFn := func(workerID int) {
		defer wg.Done()
		i := 0
		for {
			if ctx.Err() != nil {
				return
			}
			cv := corpus[i%len(corpus)]
			i++
			r := streamDownload(env.Cfg.DataServiceURL, cv.VideoID, fmt.Sprintf("dl-%d", workerID), "", hc)
			if r.Err != nil || r.Status != http.StatusOK {
				atomic.AddInt64(&dlErr, 1)
				continue
			}
			atomic.AddInt64(&dlOk, 1)
			atomic.AddInt64(&dlBytes, r.Bytes)
		}
	}

	start := time.Now()
	for i := 0; i < uploadWorkers; i++ {
		wg.Add(1)
		go uploaderFn(i)
	}
	for i := 0; i < downloadWorkers; i++ {
		wg.Add(1)
		go downloaderFn(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalUl := ulOk + ulErr
	totalDl := dlOk + dlErr
	t.Logf("RESULT mixed elapsed=%v duration=%v", elapsed, duration)
	t.Logf("RESULT mixed UL: %d ok / %d err (%.1f%% err), %.2f MiB/s aggregate, %d MiB total",
		ulOk, ulErr, pct(ulErr, totalUl), mbps(ulBytes, elapsed), ulBytes/(1024*1024))
	t.Logf("RESULT mixed DL: %d ok / %d err (%.1f%% err), %.2f MiB/s aggregate, %d MiB total",
		dlOk, dlErr, pct(dlErr, totalDl), mbps(dlBytes, elapsed), dlBytes/(1024*1024))

	if totalUl > 0 && pct(ulErr, totalUl) > 50 {
		t.Errorf("upload error rate too high: %.1f%%", pct(ulErr, totalUl))
	}
	if totalDl > 0 && pct(dlErr, totalDl) > 50 {
		t.Errorf("download error rate too high: %.1f%%", pct(dlErr, totalDl))
	}
}

func pct(part, whole int64) float64 {
	if whole == 0 {
		return 0
	}
	return float64(part) * 100.0 / float64(whole)
}
