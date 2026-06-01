package scale

import (
	"crypto/sha256"
	"net/http"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestLargeFileUpload_1MB(t *testing.T) {
	env := testutil.NewEnv(t)
	env.EnsureEntitled(t)
	const fileSize = 1 * 1024 * 1024 // 1 MB

	video := env.CreateTestVideo(t, testutil.UniqueTitle("large-1mb"), fileSize)
	payload := testutil.RandomBytes(fileSize)
	uploadHash := sha256.Sum256(payload)

	initResp, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   video.ID,
		UserID:    "e2e-user",
		TotalSize: fileSize,
	})
	if err != nil {
		t.Fatalf("InitiateUpload failed: %v", err)
	}

	// Upload in chunks
	chunkSize := initResp.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 256 * 1024 // 256 KB chunks
	}

	start := time.Now()
	chunkIdx := 0
	for offset := int64(0); offset < fileSize; offset += chunkSize {
		end := offset + chunkSize
		if end > fileSize {
			end = fileSize
		}
		resp, err := env.Data.UploadChunk(initResp.UploadID, chunkIdx, payload[offset:end])
		if err != nil {
			t.Fatalf("UploadChunk(%d) failed: %v", chunkIdx, err)
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			t.Fatalf("chunk %d status = %d", chunkIdx, resp.StatusCode)
		}
		chunkIdx++
	}

	_, _, err = env.Data.CompleteUpload(initResp.UploadID)
	if err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}

	uploadElapsed := time.Since(start)
	t.Logf("uploaded 1MB in %v (%d chunks)", uploadElapsed, chunkIdx)

	// Download and verify integrity
	dlStart := time.Now()
	downloaded, dlResp, err := env.Data.DownloadVideo(video.ID, "e2e-user")
	if err != nil {
		t.Fatalf("DownloadVideo failed: %v", err)
	}
	if dlResp.StatusCode != http.StatusOK {
		t.Fatalf("download status = %d", dlResp.StatusCode)
	}

	dlElapsed := time.Since(dlStart)
	t.Logf("downloaded 1MB in %v", dlElapsed)

	downloadHash := sha256.Sum256(downloaded)
	if uploadHash != downloadHash {
		t.Error("integrity mismatch: upload and download hashes differ")
	}
}

func TestLargeFileUpload_10MB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10MB upload in short mode")
	}

	env := testutil.NewEnv(t)
	const fileSize = 10 * 1024 * 1024 // 10 MB

	video := env.CreateTestVideo(t, testutil.UniqueTitle("large-10mb"), fileSize)
	payload := testutil.RandomBytes(fileSize)

	initResp, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   video.ID,
		UserID:    "e2e-user",
		TotalSize: fileSize,
	})
	if err != nil {
		t.Fatalf("InitiateUpload failed: %v", err)
	}

	chunkSize := initResp.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 5 * 1024 * 1024 // 5 MB default
	}

	start := time.Now()
	chunkIdx := 0
	for offset := int64(0); offset < fileSize; offset += chunkSize {
		end := offset + chunkSize
		if end > fileSize {
			end = fileSize
		}
		resp, err := env.Data.UploadChunk(initResp.UploadID, chunkIdx, payload[offset:end])
		if err != nil {
			t.Fatalf("UploadChunk(%d) failed: %v", chunkIdx, err)
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			t.Fatalf("chunk %d status = %d", chunkIdx, resp.StatusCode)
		}
		chunkIdx++
	}

	_, _, err = env.Data.CompleteUpload(initResp.UploadID)
	if err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}

	elapsed := time.Since(start)
	mbps := float64(fileSize) / elapsed.Seconds() / 1024 / 1024
	t.Logf("uploaded 10MB in %v (%.1f MB/s, %d chunks)", elapsed, mbps, chunkIdx)
}

func TestPagination_UnderLoad(t *testing.T) {
	env := testutil.NewEnv(t)
	const count = 30

	// Create many videos
	var ids []string
	for i := 0; i < count; i++ {
		v := env.CreateTestVideo(t, testutil.UniqueTitle("page-load"), 128)
		ids = append(ids, v.ID)
	}

	// Paginate through all
	seen := make(map[string]bool)
	pageSize := 5
	for offset := 0; ; offset += pageSize {
		list, _, err := env.Metadata.ListVideos(pageSize, offset)
		if err != nil {
			t.Fatalf("ListVideos(offset=%d) failed: %v", offset, err)
		}
		if len(list.Videos) == 0 {
			break
		}
		for _, v := range list.Videos {
			if seen[v.ID] {
				t.Errorf("duplicate video %s at offset %d", v.ID, offset)
			}
			seen[v.ID] = true
		}
		if len(list.Videos) < pageSize {
			break
		}
	}

	t.Logf("paginated through %d unique videos", len(seen))
	if len(seen) < count {
		t.Errorf("saw %d videos, created %d", len(seen), count)
	}
}
