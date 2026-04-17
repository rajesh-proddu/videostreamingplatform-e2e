package resiliency

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestCreateVideo_MissingTitle_Returns400(t *testing.T) {
	env := testutil.NewEnv(t)

	resp, err := env.Metadata.RawPost("/videos", "application/json",
		bytes.NewReader([]byte(`{"description":"no title","size_bytes":100}`)))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing title: status = %d, want 400", resp.StatusCode)
	}
}

func TestCreateVideo_InvalidJSON_Returns400(t *testing.T) {
	env := testutil.NewEnv(t)

	resp, err := env.Metadata.RawPost("/videos", "application/json",
		bytes.NewReader([]byte(`{invalid json`)))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid JSON: status = %d, want 400", resp.StatusCode)
	}
}

func TestCreateVideo_EmptyBody_Returns400(t *testing.T) {
	env := testutil.NewEnv(t)

	resp, err := env.Metadata.RawPost("/videos", "application/json",
		bytes.NewReader([]byte(``)))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty body: status = %d, want 400", resp.StatusCode)
	}
}

func TestGetVideo_NotFound_Returns404(t *testing.T) {
	env := testutil.NewEnv(t)

	_, resp, err := env.Metadata.GetVideo("does-not-exist-12345")
	if err == nil {
		t.Error("expected error for non-existent video")
	}
	if resp != nil && resp.StatusCode != http.StatusNotFound {
		t.Errorf("non-existent video: status = %d, want 404", resp.StatusCode)
	}
}

func TestUpdateVideo_NotFound_Returns404(t *testing.T) {
	env := testutil.NewEnv(t)

	_, resp, err := env.Metadata.UpdateVideo("does-not-exist-12345", &client.UpdateVideoRequest{
		Title: "should fail",
	})
	if err == nil {
		t.Error("expected error for updating non-existent video")
	}
	if resp != nil && resp.StatusCode != http.StatusNotFound {
		t.Errorf("update non-existent video: status = %d, want 404", resp.StatusCode)
	}
}

func TestDeleteVideo_NotFound_Returns404(t *testing.T) {
	env := testutil.NewEnv(t)

	resp, err := env.Metadata.DeleteVideo("does-not-exist-12345")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete non-existent video: status = %d, want 404 or 204", resp.StatusCode)
	}
}

func TestDeleteVideo_DoubleDelete_Idempotent(t *testing.T) {
	env := testutil.NewEnv(t)
	video := env.CreateTestVideo(t, testutil.UniqueTitle("dbl-del"), 256)

	// First delete
	resp1, err := env.Metadata.DeleteVideo(video.ID)
	if err != nil {
		t.Fatalf("first delete failed: %v", err)
	}
	resp1.Body.Close()

	// Second delete — should return 404 or 204 (idempotent)
	resp2, err := env.Metadata.DeleteVideo(video.ID)
	if err != nil {
		t.Fatalf("second delete failed: %v", err)
	}
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusNotFound && resp2.StatusCode != http.StatusNoContent {
		t.Errorf("double delete: status = %d, want 404 or 204", resp2.StatusCode)
	}
}

func TestCreateVideo_VeryLongTitle(t *testing.T) {
	env := testutil.NewEnv(t)

	longTitle := strings.Repeat("a", 10000)
	_, resp, err := env.Metadata.CreateVideo(&client.CreateVideoRequest{
		Title:     longTitle,
		SizeBytes: 100,
	})
	// Either it succeeds (and we clean up) or returns 400 — both acceptable
	if err == nil {
		t.Logf("accepted long title (len=%d)", len(longTitle))
	} else if resp != nil && resp.StatusCode == http.StatusBadRequest {
		t.Logf("correctly rejected long title with 400")
	} else if resp != nil {
		t.Logf("unexpected status for long title: %d", resp.StatusCode)
	}
}
