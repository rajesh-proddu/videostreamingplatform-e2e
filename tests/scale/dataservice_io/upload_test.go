package dataservice_io

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestUpload_SingleStream_Throughput_1GB measures single-stream upload of
// a 1 GiB payload (or SCALE_UPLOAD_SIZE_MB MiB if set), using chunked PUTs at
// the default 5 MiB chunk size. Reports MB/s, chunks/sec, and p95 chunk PUT
// latency. Skipped in -short mode.
func TestUpload_SingleStream_Throughput_1GB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1GB upload in -short mode")
	}
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	dc := dataClientWithTimeout(env.Cfg, 30*time.Minute)

	// Default 256 MiB locally — LocalStack/data-service on a dev machine
	// regularly OOMs on a full 1 GiB CompleteUpload (200-part MPU finalize).
	// AWS / larger envs should set SCALE_UPLOAD_SIZE_MB=1024 (or higher).
	sizeMB := envInt("SCALE_UPLOAD_SIZE_MB", 256)
	totalSize := int64(sizeMB) * 1024 * 1024
	chunkSize := int64(5 * 1024 * 1024)

	t.Logf("uploading %d MiB (chunk %d MiB)", sizeMB, chunkSize/1024/1024)
	videoID, res := createVideoAndUpload(t, env, dc, testutil.UniqueTitle("scale-1gb"), totalSize, chunkSize)
	if res.Err != nil {
		t.Fatalf("upload failed (videoID=%s, upload=%s): %v", videoID, res.UploadID, res.Err)
	}

	mb := mbps(res.Bytes, res.Elapsed)
	chunksPerSec := float64(res.Chunks) / res.Elapsed.Seconds()
	p50 := percentile(res.ChunkLats, 50)
	p95 := percentile(res.ChunkLats, 95)
	p99 := percentile(res.ChunkLats, 99)
	t.Logf("RESULT 1GB upload: %.2f MiB/s, %.2f chunks/s, %d chunks in %v",
		mb, chunksPerSec, res.Chunks, res.Elapsed)
	t.Logf("RESULT 1GB chunk PUT: p50=%v p95=%v p99=%v init=%v complete=%v",
		p50, p95, p99, res.InitLat, res.CompleteLat)
}

// TestUpload_Concurrent_8x100MB runs 8 parallel uploaders, each 100 MiB,
// and reports aggregate MiB/s.
func TestUpload_Concurrent_8x100MB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent 8x100MB upload in -short mode")
	}
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	dc := dataClientWithTimeout(env.Cfg, 15*time.Minute)

	workers := envInt("SCALE_UPLOAD_WORKERS", 8)
	perSizeMB := envInt("SCALE_UPLOAD_SIZE_MB_EACH", 100)
	totalEach := int64(perSizeMB) * 1024 * 1024
	chunkSize := int64(5 * 1024 * 1024)

	// Pre-create videos so metadata-service is not on the critical path.
	videoIDs := make([]string, workers)
	for i := 0; i < workers; i++ {
		v := env.CreateTestVideo(t, testutil.UniqueTitle(fmt.Sprintf("scale-conc-%d", i)), totalEach)
		videoIDs[i] = v.ID
	}

	results := make([]uploadResult, workers)
	start := time.Now()
	runConcurrent(workers, func(idx int) {
		results[idx] = uploadSizedVideo(dc, videoIDs[idx], fmt.Sprintf("user-%d", idx), totalEach, chunkSize)
	})
	elapsed := time.Since(start)

	var ok int
	var totalBytes int64
	var allChunkLats []time.Duration
	for _, r := range results {
		if r.Err == nil {
			ok++
			totalBytes += r.Bytes
		} else {
			t.Logf("worker error: %v", r.Err)
		}
		allChunkLats = append(allChunkLats, r.ChunkLats...)
	}
	mb := mbps(totalBytes, elapsed)
	t.Logf("RESULT 8x100MB: %d/%d ok, %.2f aggregate MiB/s, %d MiB total in %v",
		ok, workers, mb, totalBytes/(1024*1024), elapsed)
	t.Logf("RESULT 8x100MB chunk PUT: p50=%v p95=%v p99=%v",
		percentile(allChunkLats, 50), percentile(allChunkLats, 95), percentile(allChunkLats, 99))

	if ok < workers/2 {
		t.Errorf("too many upload failures: %d/%d", workers-ok, workers)
	}
}

// TestUpload_VariedChunkSizes uploads the same 100 MiB payload with chunk sizes
// 1, 4, 8, 16 MiB and prints the per-chunk-size throughput. Used to find the
// sweet spot for the local LocalStack backend.
func TestUpload_VariedChunkSizes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping varied chunk sweep in -short mode")
	}
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	dc := dataClientWithTimeout(env.Cfg, 10*time.Minute)

	totalSize := int64(100 * 1024 * 1024)
	sizes := []int64{1, 4, 8, 16}
	type row struct {
		Chunk    int64
		Mbps     float64
		ChunkP95 time.Duration
	}
	var rows []row
	for _, s := range sizes {
		cs := s * 1024 * 1024
		title := testutil.UniqueTitle(fmt.Sprintf("scale-sweep-%dM", s))
		_, res := createVideoAndUpload(t, env, dc, title, totalSize, cs)
		if res.Err != nil {
			t.Errorf("sweep size %d MiB: %v", s, res.Err)
			continue
		}
		mb := mbps(res.Bytes, res.Elapsed)
		p95 := percentile(res.ChunkLats, 95)
		t.Logf("RESULT sweep chunk=%2d MiB: %.2f MiB/s, %d chunks, elapsed=%v, p95=%v",
			s, mb, res.Chunks, res.Elapsed, p95)
		rows = append(rows, row{Chunk: s, Mbps: mb, ChunkP95: p95})
	}
	// Identify the best.
	var best row
	for _, r := range rows {
		if r.Mbps > best.Mbps {
			best = r
		}
	}
	if best.Chunk > 0 {
		t.Logf("RESULT sweep best: chunk=%d MiB at %.2f MiB/s (p95=%v)", best.Chunk, best.Mbps, best.ChunkP95)
	}
}

// TestUpload_ManySmallVideos uploads 200 1-MiB videos using 16 workers and
// reports uploads/sec and aggregate MiB/s.
func TestUpload_ManySmallVideos(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping many-small-videos in -short mode")
	}
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	dc := dataClientWithTimeout(env.Cfg, 10*time.Minute)

	total := envInt("SCALE_SMALL_COUNT", 200)
	workers := envInt("SCALE_SMALL_WORKERS", 16)
	const eachSize = int64(1 * 1024 * 1024)

	// Pre-create video metadata sequentially. CreateTestVideo registers a t.Cleanup
	// per video; we need the IDs.
	videoIDs := make([]string, total)
	for i := 0; i < total; i++ {
		v := env.CreateTestVideo(t, testutil.UniqueTitle(fmt.Sprintf("scale-small-%d", i)), eachSize)
		videoIDs[i] = v.ID
	}

	jobs := make(chan int, total)
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)

	var ok int64
	var fail int64
	var bytesUploaded int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for idx := range jobs {
				res := uploadSizedVideo(dc, videoIDs[idx], fmt.Sprintf("user-%d", workerID), eachSize, eachSize)
				if res.Err != nil {
					atomic.AddInt64(&fail, 1)
					continue
				}
				atomic.AddInt64(&ok, 1)
				atomic.AddInt64(&bytesUploaded, res.Bytes)
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	uploadsPerSec := float64(ok) / elapsed.Seconds()
	mb := mbps(bytesUploaded, elapsed)
	t.Logf("RESULT many-small: %d ok / %d fail, %.2f uploads/s, %.2f MiB/s, total=%v",
		ok, fail, uploadsPerSec, mb, elapsed)
	if fail > int64(total/10) {
		t.Errorf("too many failures: %d/%d", fail, total)
	}
}

// Sanity test: tiny upload, verifies the helpers are wired right and exists so
// `go test -short` still has something to run in this package.
func TestUpload_Sanity(t *testing.T) {
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	dc := dataClientWithTimeout(env.Cfg, 60*time.Second)
	const total = int64(64 * 1024) // 64 KiB
	_, res := createVideoAndUpload(t, env, dc, testutil.UniqueTitle("scale-sanity"), total, total)
	if res.Err != nil {
		t.Fatalf("sanity upload: %v", res.Err)
	}
	if res.Bytes != total {
		t.Errorf("uploaded %d bytes, want %d", res.Bytes, total)
	}
	t.Logf("sanity upload ok: %d bytes in %v", res.Bytes, res.Elapsed)
	_ = http.StatusOK // silence import on minimal builds
	_ = client.UploadInitiateRequest{}
}
