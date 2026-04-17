package happypath

import (
	"net/http"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestVideoCRUDLifecycle(t *testing.T) {
	env := testutil.NewEnv(t)
	title := testutil.UniqueTitle("crud")

	// Create
	video, _, err := env.Metadata.CreateVideo(&client.CreateVideoRequest{
		Title:       title,
		Description: "Full CRUD lifecycle test",
		SizeBytes:   1024,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if video.ID == "" {
		t.Fatal("Create returned empty ID")
	}
	if video.Title != title {
		t.Errorf("Create title = %q, want %q", video.Title, title)
	}
	if video.UploadStatus != "PENDING" {
		t.Errorf("Create upload_status = %q, want PENDING", video.UploadStatus)
	}
	t.Cleanup(func() {
		resp, _ := env.Metadata.DeleteVideo(video.ID)
		if resp != nil {
			resp.Body.Close()
		}
	})

	// Read
	got, _, err := env.Metadata.GetVideo(video.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.ID != video.ID {
		t.Errorf("Get ID = %q, want %q", got.ID, video.ID)
	}
	if got.Title != title {
		t.Errorf("Get title = %q, want %q", got.Title, title)
	}

	// Update
	newTitle := testutil.UniqueTitle("updated")
	updated, _, err := env.Metadata.UpdateVideo(video.ID, &client.UpdateVideoRequest{
		Title:       newTitle,
		Description: "Updated description",
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updated.Title != newTitle {
		t.Errorf("Update title = %q, want %q", updated.Title, newTitle)
	}
	if updated.Description != "Updated description" {
		t.Errorf("Update description = %q, want %q", updated.Description, "Updated description")
	}

	// Verify update persisted
	got2, _, err := env.Metadata.GetVideo(video.ID)
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if got2.Title != newTitle {
		t.Errorf("Get after update title = %q, want %q", got2.Title, newTitle)
	}

	// Delete
	resp, err := env.Metadata.DeleteVideo(video.ID)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("Delete status = %d, want 204", resp.StatusCode)
	}

	// Verify deletion
	_, getResp, err := env.Metadata.GetVideo(video.ID)
	if err == nil {
		t.Error("Get after delete should fail")
	}
	if getResp != nil && getResp.StatusCode != http.StatusNotFound {
		t.Errorf("Get after delete status = %d, want 404", getResp.StatusCode)
	}
}

func TestCreateVideo_ReturnsUniqueIDs(t *testing.T) {
	env := testutil.NewEnv(t)

	v1 := env.CreateTestVideo(t, testutil.UniqueTitle("id1"), 512)
	v2 := env.CreateTestVideo(t, testutil.UniqueTitle("id2"), 512)

	if v1.ID == v2.ID {
		t.Errorf("two videos got same ID: %s", v1.ID)
	}
}
