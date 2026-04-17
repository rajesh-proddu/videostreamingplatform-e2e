package resiliency

import (
	"net/http"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestUpload_NonExistentUploadID_Fails(t *testing.T) {
	env := testutil.NewEnv(t)

	chunk := testutil.RandomBytes(1024)
	resp, err := env.Data.UploadChunk("nonexistent-upload-id", 0, chunk)
	if err != nil {
		t.Skipf("UploadChunk to invalid ID returned error: %v", err)
		return
	}
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		t.Errorf("uploading to nonexistent upload should fail, got status %d", resp.StatusCode)
	}
}

func TestUpload_CompleteWithoutChunks_Fails(t *testing.T) {
	env := testutil.NewEnv(t)
	video := env.CreateTestVideo(t, testutil.UniqueTitle("no-chunks"), 5*1024)

	initResp, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   video.ID,
		UserID:    "e2e-user",
		TotalSize: 5 * 1024,
	})
	if err != nil {
		t.Fatalf("InitiateUpload failed: %v", err)
	}

	// Try to complete without uploading any chunks
	_, _, err = env.Data.CompleteUpload(initResp.UploadID)
	if err == nil {
		t.Error("CompleteUpload without chunks should fail")
	}
}

func TestUpload_DuplicateChunkIndex(t *testing.T) {
	env := testutil.NewEnv(t)
	video := env.CreateTestVideo(t, testutil.UniqueTitle("dup-chunk"), 5*1024)

	initResp, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   video.ID,
		UserID:    "e2e-user",
		TotalSize: 5 * 1024,
	})
	if err != nil {
		t.Fatalf("InitiateUpload failed: %v", err)
	}

	chunk := testutil.RandomBytes(1024)

	// Upload chunk 0
	resp1, err := env.Data.UploadChunk(initResp.UploadID, 0, chunk)
	if err != nil {
		t.Fatalf("first UploadChunk failed: %v", err)
	}
	if resp1.StatusCode != http.StatusOK && resp1.StatusCode != http.StatusCreated {
		t.Fatalf("first chunk status = %d", resp1.StatusCode)
	}

	// Upload chunk 0 again — should either succeed (idempotent) or return an error
	resp2, err := env.Data.UploadChunk(initResp.UploadID, 0, chunk)
	if err != nil {
		t.Logf("duplicate chunk returned error (expected): %v", err)
		return
	}
	t.Logf("duplicate chunk status = %d (server may accept idempotent uploads)", resp2.StatusCode)
}

func TestUpload_ProgressForNonExistentUpload(t *testing.T) {
	env := testutil.NewEnv(t)

	_, resp, err := env.Data.GetProgress("nonexistent-upload-id")
	if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
		t.Error("GetProgress for non-existent upload should fail")
	}
}

func TestUpload_ZeroSizeFile(t *testing.T) {
	env := testutil.NewEnv(t)
	video := env.CreateTestVideo(t, testutil.UniqueTitle("zero-size"), 0)

	_, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   video.ID,
		UserID:    "e2e-user",
		TotalSize: 0,
	})
	// Zero-size upload may be rejected or accepted — both are valid
	if err != nil {
		t.Logf("zero-size upload rejected: %v", err)
	} else {
		t.Log("zero-size upload accepted")
	}
}
