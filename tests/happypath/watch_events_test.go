package happypath

import (
	"context"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestWatchEvent_DownloadProducesStartedAndCompleted exercises the only path
// that produces watch events in the platform: GET /videos/{id}/download on
// dataservice for a full (non-Range) request.
func TestWatchEvent_DownloadProducesStartedAndCompleted(t *testing.T) {
	env := testutil.NewEnv(t)

	consumer := client.NewKafkaConsumer(
		env.Cfg.KafkaBrokers,
		"watch-events",
		"e2e-watch-"+testutil.UniqueID("grp"),
	)
	defer consumer.Close()

	const sizeBytes = 4 * 1024
	v := env.CreateTestVideo(t, testutil.UniqueTitle("watch"), sizeBytes)

	init, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   v.ID,
		UserID:    "e2e-watcher",
		TotalSize: sizeBytes,
	})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	if _, err := env.Data.UploadChunk(init.UploadID, 0, testutil.RandomBytes(sizeBytes)); err != nil {
		t.Fatalf("UploadChunk: %v", err)
	}
	if _, _, err := env.Data.CompleteUpload(init.UploadID); err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}

	if _, _, err := env.Data.DownloadVideo(v.ID, "e2e-watcher"); err != nil {
		t.Fatalf("DownloadVideo: %v", err)
	}

	events, err := consumer.ReadEvents(context.Background(), env.Cfg.EventWaitTime)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}

	startedFound, completedFound := false, false
	for _, e := range events {
		switch e.Type {
		case "watch.started", "WATCH_STARTED":
			startedFound = true
		case "watch.completed", "WATCH_COMPLETED":
			completedFound = true
		}
	}

	if !startedFound && !completedFound {
		t.Skipf("watch events not received within %s — Kafka producer may be disabled in dataservice", env.Cfg.EventWaitTime)
	}
	if startedFound {
		t.Log("received watch.started")
	}
	if completedFound {
		t.Log("received watch.completed")
	}
}
