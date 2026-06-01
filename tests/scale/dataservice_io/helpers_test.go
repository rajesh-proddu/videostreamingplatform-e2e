// Package dataservice_io contains throughput-oriented scale tests for the
// data-service upload/download paths. See README.md in this directory.
package dataservice_io

import (
	"crypto/rand"
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/config"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// dataClientWithTimeout builds a fresh DataClient with a longer timeout for
// the throughput tests. The default UploadTimeout (120s) is too tight for
// 1 GiB completes against LocalStack.
func dataClientWithTimeout(cfg *config.Config, timeout time.Duration) *client.DataClient {
	return client.NewDataClient(cfg.DataServiceURL, timeout)
}

// envInt returns the env var as int, or fallback.
func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// envDuration returns the env var as a duration, or fallback.
func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

// randomBuffer produces a single in-memory buffer of n bytes from crypto/rand.
// Reuse across chunks so we measure the network/server, not crypto/rand.
func randomBuffer(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand should never fail on linux; fall back to deterministic fill.
		for i := range b {
			b[i] = byte(i)
		}
	}
	return b
}

// uploadResult captures per-chunk PUT latencies and any error from a single
// upload run.
type uploadResult struct {
	UploadID    string
	VideoID     string
	Bytes       int64
	Chunks      int
	ChunkLats   []time.Duration
	InitLat     time.Duration
	CompleteLat time.Duration
	Elapsed     time.Duration
	Err         error
}

// uploadSizedVideo runs a full init -> chunk loop -> complete using the given
// chunk size and total size. It generates content from a single random buffer
// reused across chunks. The metadata video must have already been created with
// SizeBytes == totalSize.
func uploadSizedVideo(dc *client.DataClient, videoID, userID string, totalSize int64, chunkSize int64) uploadResult {
	res := uploadResult{VideoID: videoID, Bytes: totalSize}

	overallStart := time.Now()
	initStart := time.Now()
	initResp, _, err := dc.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   videoID,
		UserID:    userID,
		TotalSize: totalSize,
	})
	res.InitLat = time.Since(initStart)
	if err != nil {
		res.Err = fmt.Errorf("initiate: %w", err)
		return res
	}
	res.UploadID = initResp.UploadID

	// Use caller-requested chunk size if positive, else server-suggested.
	effChunk := chunkSize
	if effChunk <= 0 {
		effChunk = initResp.ChunkSize
		if effChunk <= 0 {
			effChunk = 5 * 1024 * 1024
		}
	}

	// One reused random buffer at chunk size.
	buf := randomBuffer(int(effChunk))

	chunkIdx := 0
	for offset := int64(0); offset < totalSize; offset += effChunk {
		end := offset + effChunk
		if end > totalSize {
			end = totalSize
		}
		slice := buf[: end-offset]

		start := time.Now()
		resp, err := dc.UploadChunk(initResp.UploadID, chunkIdx, slice)
		res.ChunkLats = append(res.ChunkLats, time.Since(start))
		if err != nil {
			res.Err = fmt.Errorf("chunk %d: %w", chunkIdx, err)
			return res
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			res.Err = fmt.Errorf("chunk %d status %d", chunkIdx, resp.StatusCode)
			return res
		}
		chunkIdx++
	}
	res.Chunks = chunkIdx

	cStart := time.Now()
	_, _, err = dc.CompleteUpload(initResp.UploadID)
	res.CompleteLat = time.Since(cStart)
	if err != nil {
		res.Err = fmt.Errorf("complete: %w", err)
		return res
	}

	res.Elapsed = time.Since(overallStart)
	return res
}

// createVideoAndUpload combines CreateTestVideo + uploadSizedVideo. It uses the
// shared env's metadata client (auto cleanup via t.Cleanup).
func createVideoAndUpload(t *testing.T, env *testutil.Env, dc *client.DataClient, title string, totalSize int64, chunkSize int64) (videoID string, res uploadResult) {
	t.Helper()
	v := env.CreateTestVideo(t, title, totalSize)
	res = uploadSizedVideo(dc, v.ID, "e2e-user", totalSize, chunkSize)
	return v.ID, res
}

// percentile returns the pth percentile (0..100) of the durations in d.
// Allocates a copy and sorts it.
func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	cp := make([]time.Duration, len(d))
	copy(cp, d)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(math.Ceil((p/100.0)*float64(len(cp)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

// mbps converts bytes/elapsed into MiB/s.
func mbps(bytesN int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(bytesN) / elapsed.Seconds() / (1024.0 * 1024.0)
}

// downloadResult is used by download tests.
type downloadResult struct {
	Bytes   int64
	Elapsed time.Duration
	Status  int
	Err     error
}

// streamDownload issues a GET (with optional Range header) and discards bytes,
// counting them. Returns elapsed/total bytes/status.
func streamDownload(baseURL, videoID, userID, rangeHeader, token string, hc *http.Client) downloadResult {
	url := fmt.Sprintf("%s/videos/%s/download", baseURL, videoID)
	if userID != "" {
		url += "?user_id=" + userID
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return downloadResult{Err: err}
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	start := time.Now()
	resp, err := hc.Do(req)
	if err != nil {
		return downloadResult{Err: err, Elapsed: time.Since(start)}
	}
	defer resp.Body.Close()

	// Drain body to count bytes; use a 1 MiB scratch.
	scratch := make([]byte, 1*1024*1024)
	var total int64
	for {
		n, rerr := resp.Body.Read(scratch)
		total += int64(n)
		if rerr != nil {
			break
		}
	}
	return downloadResult{
		Bytes:   total,
		Elapsed: time.Since(start),
		Status:  resp.StatusCode,
	}
}

// streamDownloadCDN issues a GET against /videos/{id} on the CDN proxy
// (no /download suffix, no user_id query).
func streamDownloadCDN(cdnURL, videoID, rangeHeader string, hc *http.Client) downloadResult {
	url := fmt.Sprintf("%s/videos/%s", cdnURL, videoID)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return downloadResult{Err: err}
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	start := time.Now()
	resp, err := hc.Do(req)
	if err != nil {
		return downloadResult{Err: err, Elapsed: time.Since(start)}
	}
	defer resp.Body.Close()
	scratch := make([]byte, 1*1024*1024)
	var total int64
	for {
		n, rerr := resp.Body.Read(scratch)
		total += int64(n)
		if rerr != nil {
			break
		}
	}
	return downloadResult{
		Bytes:   total,
		Elapsed: time.Since(start),
		Status:  resp.StatusCode,
	}
}

// requireDataServiceUp short-circuits a test if the data-service health
// endpoint isn't responding. Useful when running the full suite — a prior
// test can crash the local container (e.g. OOM on a large CompleteUpload)
// and the rest of the suite would otherwise log "connection refused" for
// every assertion. We pause to allow Docker auto-restart, then skip if
// still unreachable.
func requireDataServiceUp(t *testing.T, env *testutil.Env) {
	t.Helper()
	// Acquire an entitled token so gated download calls succeed (no-op if the
	// user service is unreachable / paywall is off). Sets env.Data.Token, which
	// streamDownload forwards as a bearer token.
	env.EnsureEntitled(t)
	deadline := time.Now().Add(15 * time.Second)
	for {
		code, err := env.Data.Health()
		if err == nil && code == http.StatusOK {
			return
		}
		if time.Now().After(deadline) {
			t.Skipf("data-service unreachable at %s (last code=%d err=%v); skipping",
				env.Cfg.DataServiceURL, code, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// runConcurrent fans out n goroutines and waits for them all. fn is called
// with the goroutine index.
func runConcurrent(n int, fn func(idx int)) {
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			fn(idx)
		}(i)
	}
	wg.Wait()
}
