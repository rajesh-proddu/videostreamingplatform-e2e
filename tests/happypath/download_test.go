package happypath

import (
	"crypto/sha256"
	"net/http"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestDownload_IntegrityCheck(t *testing.T) {
	env := testutil.NewEnv(t)
	video := env.CreateTestVideo(t, testutil.UniqueTitle("download"), 8*1024)

	// Upload a known payload
	payload := testutil.RandomBytes(8 * 1024)
	uploadHash := sha256.Sum256(payload)

	initResp, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   video.ID,
		UserID:    "e2e-user",
		TotalSize: int64(len(payload)),
	})
	if err != nil {
		t.Fatalf("InitiateUpload failed: %v", err)
	}

	// Upload single chunk (small file)
	resp, err := env.Data.UploadChunk(initResp.UploadID, 0, payload)
	if err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("UploadChunk status = %d", resp.StatusCode)
	}

	_, _, err = env.Data.CompleteUpload(initResp.UploadID)
	if err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}

	// Download and verify integrity
	downloaded, dlResp, err := env.Data.DownloadVideo(video.ID, "e2e-user")
	if err != nil {
		t.Fatalf("DownloadVideo failed: %v", err)
	}
	if dlResp.StatusCode != http.StatusOK {
		t.Fatalf("download status = %d, want 200", dlResp.StatusCode)
	}

	downloadHash := sha256.Sum256(downloaded)
	if uploadHash != downloadHash {
		t.Errorf("integrity mismatch: upload sha256=%x, download sha256=%x", uploadHash, downloadHash)
	}
	if len(downloaded) != len(payload) {
		t.Errorf("downloaded size = %d, uploaded size = %d", len(downloaded), len(payload))
	}
}

func TestDownload_NonExistentVideo_Returns404(t *testing.T) {
	env := testutil.NewEnv(t)

	_, resp, err := env.Data.DownloadVideo("nonexistent-video-id", "e2e-user")
	if err == nil && resp != nil && resp.StatusCode != http.StatusNotFound {
		t.Errorf("download non-existent video: status = %d, want 404", resp.StatusCode)
	}
}
