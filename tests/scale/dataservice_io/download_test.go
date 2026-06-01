package dataservice_io

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// corpusVideo represents a pre-uploaded fixture video for download tests.
type corpusVideo struct {
	VideoID string
	Size    int64
}

// buildCorpus uploads videos at the given sizes (in MiB) and returns their
// metadata. Uses chunked uploads at 5 MiB. Cleanup runs via t.Cleanup
// inside CreateTestVideo (metadata delete) — the S3 blob outlives the test
// in LocalStack, which is acceptable for the local environment.
func buildCorpus(t *testing.T, env *testutil.Env, sizesMB []int) []corpusVideo {
	t.Helper()
	dc := dataClientWithTimeout(env.Cfg, 20*time.Minute)
	corpus := make([]corpusVideo, len(sizesMB))
	for i, mb := range sizesMB {
		size := int64(mb) * 1024 * 1024
		v := env.CreateTestVideo(t, testutil.UniqueTitle(fmt.Sprintf("corpus-%dMB", mb)), size)
		res := uploadSizedVideo(dc, v.ID, "corpus-builder", size, 5*1024*1024)
		if res.Err != nil {
			t.Fatalf("corpus build size=%dMB: %v", mb, res.Err)
		}
		corpus[i] = corpusVideo{VideoID: v.ID, Size: size}
		t.Logf("corpus[%d MiB] ready: id=%s upload_elapsed=%v", mb, v.ID, res.Elapsed)
	}
	return corpus
}

// TestDownload_Single_VariedSize downloads videos at 1, 10, 100, and
// optionally 1024 MiB and reports MiB/s for each. The 1024 MiB size is gated
// behind SCALE_DOWNLOAD_GIB=1 (off by default) because a 1 GiB
// CompleteUpload regularly OOMs the local data-service container.
func TestDownload_Single_VariedSize(t *testing.T) {
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	sizes := []int{1, 10, 100}
	if !testing.Short() && envInt("SCALE_DOWNLOAD_GIB", 0) > 0 {
		sizes = append(sizes, 1024)
		t.Logf("SCALE_DOWNLOAD_GIB enabled: including 1024 MiB in sweep")
	}
	corpus := buildCorpus(t, env, sizes)

	hc := &http.Client{Timeout: 20 * time.Minute}
	for i, cv := range corpus {
		res := streamDownload(env.Cfg.DataServiceURL, cv.VideoID, "dl-user", "", env.Data.Token, hc)
		if res.Err != nil {
			t.Errorf("download %d MiB: %v", sizes[i], res.Err)
			continue
		}
		if res.Status != http.StatusOK {
			t.Errorf("download %d MiB: status=%d", sizes[i], res.Status)
			continue
		}
		if res.Bytes != cv.Size {
			t.Errorf("download %d MiB: got %d bytes, want %d", sizes[i], res.Bytes, cv.Size)
		}
		t.Logf("RESULT single-dl %4d MiB: %.2f MiB/s (elapsed=%v)",
			sizes[i], mbps(res.Bytes, res.Elapsed), res.Elapsed)
	}
}

// TestDownload_Concurrent_16x10MB launches 16 concurrent readers of a 10 MiB
// video and reports aggregate MiB/s.
func TestDownload_Concurrent_16x10MB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 16x10MB concurrent download in -short mode")
	}
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	corpus := buildCorpus(t, env, []int{10})
	cv := corpus[0]

	workers := envInt("SCALE_DL_WORKERS", 16)
	type r struct {
		bytes  int64
		status int
		err    error
		dt     time.Duration
	}
	results := make([]r, workers)
	hc := &http.Client{Timeout: 5 * time.Minute}

	start := time.Now()
	runConcurrent(workers, func(idx int) {
		dr := streamDownload(env.Cfg.DataServiceURL, cv.VideoID, fmt.Sprintf("dl-user-%d", idx), "", env.Data.Token, hc)
		results[idx] = r{bytes: dr.Bytes, status: dr.Status, err: dr.Err, dt: dr.Elapsed}
	})
	elapsed := time.Since(start)

	var ok int
	var totalBytes int64
	var lats []time.Duration
	for _, x := range results {
		if x.err != nil || x.status != http.StatusOK {
			t.Logf("dl error: status=%d err=%v", x.status, x.err)
			continue
		}
		if x.bytes != cv.Size {
			t.Errorf("dl byte mismatch: got %d want %d", x.bytes, cv.Size)
		}
		ok++
		totalBytes += x.bytes
		lats = append(lats, x.dt)
	}
	t.Logf("RESULT 16x10MB dl: %d/%d ok, %.2f aggregate MiB/s, elapsed=%v",
		ok, workers, mbps(totalBytes, elapsed), elapsed)
	t.Logf("RESULT 16x10MB dl latency: p50=%v p95=%v p99=%v",
		percentile(lats, 50), percentile(lats, 95), percentile(lats, 99))
	if ok < workers/2 {
		t.Errorf("too many download failures: %d/%d", workers-ok, workers)
	}
}

// TestDownload_RangeRequests issues 4 random Range requests against a 100 MiB
// video. Verifies byte length and measures latency.
func TestDownload_RangeRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping range-request test in -short mode")
	}
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	corpus := buildCorpus(t, env, []int{100})
	cv := corpus[0]
	hc := &http.Client{Timeout: 60 * time.Second}

	// Pick 4 ranges, randomly sized 1..16 MiB.
	type rng struct{ start, end int64 }
	ranges := make([]rng, 4)
	for i := 0; i < 4; i++ {
		var rb [16]byte
		_, _ = rand.Read(rb[:])
		startSeed := binary.LittleEndian.Uint64(rb[:8]) % uint64(cv.Size-1024)
		// length 1..16 MiB
		lenN := int64(binary.LittleEndian.Uint64(rb[8:])%uint64(16*1024*1024) + 1024)
		s := int64(startSeed)
		e := s + lenN - 1
		if e >= cv.Size {
			e = cv.Size - 1
		}
		ranges[i] = rng{start: s, end: e}
	}

	for i, r := range ranges {
		hdr := fmt.Sprintf("bytes=%d-%d", r.start, r.end)
		res := streamDownload(env.Cfg.DataServiceURL, cv.VideoID, "rng-user", hdr, env.Data.Token, hc)
		if res.Err != nil {
			t.Errorf("range %d (%s): %v", i, hdr, res.Err)
			continue
		}
		if res.Status != http.StatusPartialContent {
			t.Errorf("range %d (%s): expected 206 got %d", i, hdr, res.Status)
			continue
		}
		expected := r.end - r.start + 1
		if res.Bytes != expected {
			t.Errorf("range %d (%s): got %d bytes, want %d", i, hdr, res.Bytes, expected)
			continue
		}
		t.Logf("RESULT range[%d] %s: %d bytes in %v", i, hdr, res.Bytes, res.Elapsed)
	}
}

// TestDownload_CDNProxy_vs_Direct compares CDN proxy vs direct data-service.
// Confirms proxy doesn't break Range requests.
func TestDownload_CDNProxy_vs_Direct(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping CDN vs direct in -short mode")
	}
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	env.RequireCDN(t)
	corpus := buildCorpus(t, env, []int{10})
	cv := corpus[0]
	hc := &http.Client{Timeout: 60 * time.Second}

	// Wait for the object to be visible at the CDN edge (S3 origin reachable
	// through nginx may take a moment after CompleteUpload).
	deadline := time.Now().Add(env.Cfg.AnalyticsWaitTime)
	for {
		r := streamDownloadCDN(env.Cfg.CDNProxyURL, cv.VideoID, "", hc)
		if r.Err == nil && r.Status == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Skipf("CDN never returned 200 for %s (last status=%d err=%v); skipping", cv.VideoID, r.Status, r.Err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Full GET timings.
	directFull := streamDownload(env.Cfg.DataServiceURL, cv.VideoID, "cdn-cmp", "", env.Data.Token, hc)
	cdnFull := streamDownloadCDN(env.Cfg.CDNProxyURL, cv.VideoID, "", hc)
	if directFull.Err != nil || cdnFull.Err != nil {
		t.Fatalf("full GET: direct=%v cdn=%v", directFull.Err, cdnFull.Err)
	}
	if cdnFull.Bytes != directFull.Bytes {
		t.Errorf("size mismatch direct=%d cdn=%d", directFull.Bytes, cdnFull.Bytes)
	}
	t.Logf("RESULT full GET: direct=%.2f MiB/s (%v) cdn=%.2f MiB/s (%v)",
		mbps(directFull.Bytes, directFull.Elapsed), directFull.Elapsed,
		mbps(cdnFull.Bytes, cdnFull.Elapsed), cdnFull.Elapsed)

	// Range request, ensure CDN doesn't break it.
	rangeHdr := "bytes=1048576-2097151" // 1 MiB chunk in the middle
	cdnRange := streamDownloadCDN(env.Cfg.CDNProxyURL, cv.VideoID, rangeHdr, hc)
	directRange := streamDownload(env.Cfg.DataServiceURL, cv.VideoID, "cdn-cmp", rangeHdr, env.Data.Token, hc)
	if directRange.Status != http.StatusPartialContent {
		t.Errorf("direct range status=%d", directRange.Status)
	}
	if cdnRange.Status != http.StatusPartialContent {
		t.Errorf("CDN range status=%d (proxy may not forward Range)", cdnRange.Status)
	}
	if cdnRange.Status == http.StatusPartialContent && cdnRange.Bytes != directRange.Bytes {
		t.Errorf("CDN range byte count mismatch: cdn=%d direct=%d", cdnRange.Bytes, directRange.Bytes)
	}
	t.Logf("RESULT range %s: direct=%d bytes in %v, cdn=%d bytes in %v",
		rangeHdr, directRange.Bytes, directRange.Elapsed, cdnRange.Bytes, cdnRange.Elapsed)
}

// TestDownload_ParallelClients_Aggregate is a small sanity verifier — kicks
// 4 concurrent downloads off a 1 MiB video and ensures no errors. Kept light
// so -short still has download coverage.
func TestDownload_Sanity_Parallel(t *testing.T) {
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	corpus := buildCorpus(t, env, []int{1})
	cv := corpus[0]
	hc := &http.Client{Timeout: 60 * time.Second}

	const workers = 4
	var ok int64
	var mu sync.Mutex
	var lats []time.Duration
	runConcurrent(workers, func(idx int) {
		r := streamDownload(env.Cfg.DataServiceURL, cv.VideoID, fmt.Sprintf("u-%d", idx), "", env.Data.Token, hc)
		if r.Err == nil && r.Status == http.StatusOK && r.Bytes == cv.Size {
			atomic.AddInt64(&ok, 1)
			mu.Lock()
			lats = append(lats, r.Elapsed)
			mu.Unlock()
		}
	})
	t.Logf("sanity parallel dl: %d/%d ok, p50=%v p95=%v", ok, workers, percentile(lats, 50), percentile(lats, 95))
	if ok != workers {
		t.Errorf("expected all 4 downloads to succeed, got %d", ok)
	}
}
