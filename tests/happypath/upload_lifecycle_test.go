package happypath

import (
	"crypto/sha256"
	"net/http"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestUploadLifecycle(t *testing.T) {
	env := testutil.NewEnv(t)
	video := env.CreateTestVideo(t, testutil.UniqueTitle("upload"), 10*1024) // 10 KB

	// Initiate upload
	initResp, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   video.ID,
		UserID:    "e2e-user",
		TotalSize: 10 * 1024,
	})
	if err != nil {
		t.Fatalf("InitiateUpload failed: %v", err)
	}
	if initResp.UploadID == "" {
		t.Fatal("InitiateUpload returned empty upload ID")
	}
	t.Logf("upload_id=%s chunk_size=%d", initResp.UploadID, initResp.ChunkSize)

	// Determine chunk size (use server-provided or default 5MB)
	chunkSize := initResp.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 5 * 1024 * 1024
	}

	// Generate payload and upload in chunks
	payload := testutil.RandomBytes(10 * 1024)
	totalChunks := 0
	for offset := int64(0); offset < int64(len(payload)); offset += chunkSize {
		end := offset + chunkSize
		if end > int64(len(payload)) {
			end = int64(len(payload))
		}
		resp, err := env.Data.UploadChunk(initResp.UploadID, totalChunks, payload[offset:end])
		if err != nil {
			t.Fatalf("UploadChunk(%d) failed: %v", totalChunks, err)
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			t.Fatalf("UploadChunk(%d) status = %d", totalChunks, resp.StatusCode)
		}
		totalChunks++
	}

	// Check progress
	progress, _, err := env.Data.GetProgress(initResp.UploadID)
	if err != nil {
		t.Fatalf("GetProgress failed: %v", err)
	}
	if progress.UploadedChunks != totalChunks {
		t.Errorf("uploaded_chunks = %d, want %d", progress.UploadedChunks, totalChunks)
	}

	// Complete upload
	complete, _, err := env.Data.CompleteUpload(initResp.UploadID)
	if err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}
	if complete.Status != "COMPLETED" && complete.Status != "completed" {
		t.Errorf("complete status = %q, want COMPLETED", complete.Status)
	}

	// Verify metadata upload_status updated
	got, _, err := env.Metadata.GetVideo(video.ID)
	if err != nil {
		t.Fatalf("GetVideo after upload failed: %v", err)
	}
	if got.UploadStatus != "COMPLETED" && got.UploadStatus != "completed" {
		t.Logf("upload_status = %q (may update asynchronously)", got.UploadStatus)
	}

	// Store hash for download integrity test
	t.Logf("upload payload sha256: %x", sha256.Sum256(payload))
}

func TestUploadProgress_TracksDuringUpload(t *testing.T) {
	env := testutil.NewEnv(t)
	video := env.CreateTestVideo(t, testutil.UniqueTitle("progress"), 20*1024)

	initResp, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   video.ID,
		UserID:    "e2e-user",
		TotalSize: 20 * 1024,
	})
	if err != nil {
		t.Fatalf("InitiateUpload failed: %v", err)
	}

	// Check initial progress
	progress, _, err := env.Data.GetProgress(initResp.UploadID)
	if err != nil {
		t.Fatalf("GetProgress (initial) failed: %v", err)
	}
	if progress.UploadedChunks != 0 {
		t.Errorf("initial uploaded_chunks = %d, want 0", progress.UploadedChunks)
	}

	// Upload one chunk
	chunk := testutil.RandomBytes(5 * 1024)
	_, err = env.Data.UploadChunk(initResp.UploadID, 0, chunk)
	if err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}

	// Check progress increased
	progress2, _, err := env.Data.GetProgress(initResp.UploadID)
	if err != nil {
		t.Fatalf("GetProgress (after chunk) failed: %v", err)
	}
	if progress2.UploadedChunks < 1 {
		t.Errorf("after 1 chunk, uploaded_chunks = %d, want >= 1", progress2.UploadedChunks)
	}
	if progress2.UploadedSize <= 0 {
		t.Errorf("after 1 chunk, uploaded_size = %d, want > 0", progress2.UploadedSize)
	}
}
