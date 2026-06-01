package scale

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestConcurrentUploads(t *testing.T) {
	env := testutil.NewEnv(t)
	concurrency := env.Cfg.ConcurrentUsers
	if concurrency > 5 {
		concurrency = 5 // cap for upload tests
	}

	// Create videos first
	type uploadJob struct {
		videoID  string
		uploadID string
	}
	var jobs []uploadJob

	for i := 0; i < concurrency; i++ {
		v := env.CreateTestVideo(t, fmt.Sprintf("conc-upload-%d-%s", i, testutil.UniqueTitle("u")), 4*1024)
		initResp, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
			VideoID:   v.ID,
			UserID:    fmt.Sprintf("user-%d", i),
			TotalSize: 4 * 1024,
		})
		if err != nil {
			t.Fatalf("InitiateUpload %d failed: %v", i, err)
		}
		jobs = append(jobs, uploadJob{videoID: v.ID, uploadID: initResp.UploadID})
	}

	// Upload all in parallel
	var (
		wg     sync.WaitGroup
		errors int
		mu     sync.Mutex
	)

	start := time.Now()
	wg.Add(len(jobs))
	for _, job := range jobs {
		go func(j uploadJob) {
			defer wg.Done()
			chunk := testutil.RandomBytes(4 * 1024)
			resp, err := env.Data.UploadChunk(j.uploadID, 0, chunk)
			if err != nil {
				mu.Lock()
				errors++
				mu.Unlock()
				return
			}
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
				mu.Lock()
				errors++
				mu.Unlock()
				return
			}

			_, _, err = env.Data.CompleteUpload(j.uploadID)
			if err != nil {
				mu.Lock()
				errors++
				mu.Unlock()
			}
		}(job)
	}
	wg.Wait()

	elapsed := time.Since(start)
	t.Logf("concurrent uploads: %d success, %d errors in %v", len(jobs)-errors, errors, elapsed)

	if errors > len(jobs)/2 {
		t.Errorf("too many upload errors: %d/%d", errors, len(jobs))
	}
}

func TestConcurrentDownloads(t *testing.T) {
	env := testutil.NewEnv(t)
	env.EnsureEntitled(t)

	// Create and upload one video
	video := env.CreateTestVideo(t, testutil.UniqueTitle("conc-dl"), 4*1024)
	payload := testutil.RandomBytes(4 * 1024)

	initResp, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   video.ID,
		UserID:    "e2e-user",
		TotalSize: int64(len(payload)),
	})
	if err != nil {
		t.Fatalf("InitiateUpload failed: %v", err)
	}

	resp, err := env.Data.UploadChunk(initResp.UploadID, 0, payload)
	if err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload chunk status = %d", resp.StatusCode)
	}

	_, _, err = env.Data.CompleteUpload(initResp.UploadID)
	if err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}

	// Download concurrently
	concurrency := env.Cfg.ConcurrentUsers
	var (
		wg       sync.WaitGroup
		dlErrors int
		mu       sync.Mutex
	)

	start := time.Now()
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(n int) {
			defer wg.Done()
			data, dlResp, err := env.Data.DownloadVideo(video.ID, fmt.Sprintf("user-%d", n))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				dlErrors++
				return
			}
			if dlResp.StatusCode != http.StatusOK {
				dlErrors++
				return
			}
			if len(data) != len(payload) {
				t.Errorf("download %d: got %d bytes, want %d", n, len(data), len(payload))
			}
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)
	t.Logf("concurrent downloads: %d success, %d errors in %v",
		concurrency-dlErrors, dlErrors, elapsed)

	if dlErrors > concurrency/2 {
		t.Errorf("too many download errors: %d/%d", dlErrors, concurrency)
	}
}
