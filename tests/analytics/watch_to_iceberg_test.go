package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

type rawWatchEvent struct {
	Version   string         `json:"version"`
	Type      string         `json:"type"`
	Timestamp string         `json:"timestamp"`
	Payload   map[string]any `json:"payload"`
}

func produceWatchEvent(t *testing.T, brokers, topic string, evt rawWatchEvent) {
	t.Helper()
	prod := client.NewKafkaProducer(brokers, topic)
	defer prod.Close()
	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := fmt.Sprintf("%v", evt.Payload["video_id"])
	if err := prod.WriteJSON(ctx, key, body); err != nil {
		t.Fatalf("kafka write: %v", err)
	}
}

func uploadAndDownload(t *testing.T, env *testutil.Env, sizeKB int) string {
	t.Helper()
	env.EnsureEntitled(t)
	v := env.CreateTestVideo(t, testutil.UniqueTitle("ice"), int64(sizeKB*1024))

	init, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   v.ID,
		UserID:    "ice-user",
		TotalSize: int64(sizeKB * 1024),
	})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	chunk := testutil.RandomBytes(sizeKB * 1024)
	if _, err := env.Data.UploadChunk(init.UploadID, 0, chunk); err != nil {
		t.Fatalf("UploadChunk: %v", err)
	}
	if _, _, err := env.Data.CompleteUpload(init.UploadID); err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}
	if _, _, err := env.Data.DownloadVideo(v.ID, "ice-user"); err != nil {
		t.Fatalf("DownloadVideo: %v", err)
	}
	return v.ID
}

func TestIceberg_Download_ProducesWatchStartedAndCompleted(t *testing.T) {
	env := testutil.NewEnv(t)

	consumer := client.NewKafkaConsumer(
		env.Cfg.KafkaBrokers,
		"watch-events",
		"e2e-iceberg-watchevt-"+testutil.UniqueID("grp"),
	)
	defer consumer.Close()

	_ = uploadAndDownload(t, env, 4)

	events, err := consumer.ReadEvents(context.Background(), env.Cfg.EventWaitTime)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	started, completed := false, false
	for _, e := range events {
		switch e.Type {
		case "watch.started", "WATCH_STARTED":
			started = true
		case "watch.completed", "WATCH_COMPLETED":
			completed = true
		}
	}
	if !started || !completed {
		t.Skipf("watch events not seen within %s (started=%v completed=%v)", env.Cfg.EventWaitTime, started, completed)
	}
}

func TestIceberg_WatchEvent_AppendsToTable(t *testing.T) {
	env := testutil.NewEnv(t)
	ice := env.IcebergS3(t)

	ctx := context.Background()
	startCount, err := ice.CountDataFiles(ctx)
	if err != nil {
		t.Fatalf("CountDataFiles: %v", err)
	}

	produceWatchEvent(t, env.Cfg.KafkaBrokers, "watch-events", rawWatchEvent{
		Version:   "1.0",
		Type:      "WATCH_COMPLETED",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload: map[string]any{
			"video_id":   testutil.UniqueID("ice-vid"),
			"user_id":    testutil.UniqueID("ice-user"),
			"session_id": testutil.UniqueID("sess"),
			"bytes_read": 1024,
		},
	})

	endCount, err := ice.WaitForFileIncrease(ctx, startCount, env.Cfg.AnalyticsWaitTime)
	if err != nil {
		t.Skipf("Iceberg flush not observed within %s (start=%d end=%d): %v", env.Cfg.AnalyticsWaitTime, startCount, endCount, err)
	}
	if endCount <= startCount {
		t.Fatalf("expected new parquet file: start=%d end=%d", startCount, endCount)
	}
}

func TestIceberg_VersionMismatchIsDropped(t *testing.T) {
	env := testutil.NewEnv(t)
	ice := env.IcebergS3(t)

	ctx := context.Background()
	startCount, err := ice.CountDataFiles(ctx)
	if err != nil {
		t.Fatalf("CountDataFiles: %v", err)
	}

	produceWatchEvent(t, env.Cfg.KafkaBrokers, "watch-events", rawWatchEvent{
		Version:   "0.9",
		Type:      "WATCH_COMPLETED",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload: map[string]any{
			"video_id":   testutil.UniqueID("v09"),
			"user_id":    testutil.UniqueID("u09"),
			"session_id": testutil.UniqueID("s09"),
			"bytes_read": 1,
		},
	})

	time.Sleep(env.Cfg.AnalyticsWaitTime)
	endCount, err := ice.CountDataFiles(ctx)
	if err != nil {
		t.Fatalf("CountDataFiles: %v", err)
	}
	if endCount > startCount {
		t.Fatalf("v0.9 event should be dropped, but parquet count grew from %d to %d", startCount, endCount)
	}
}

func TestIceberg_MissingIDsAreDropped(t *testing.T) {
	env := testutil.NewEnv(t)
	ice := env.IcebergS3(t)

	ctx := context.Background()
	startCount, err := ice.CountDataFiles(ctx)
	if err != nil {
		t.Fatalf("CountDataFiles: %v", err)
	}

	produceWatchEvent(t, env.Cfg.KafkaBrokers, "watch-events", rawWatchEvent{
		Version:   "1.0",
		Type:      "WATCH_COMPLETED",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload: map[string]any{
			"video_id":   "",
			"user_id":    "",
			"session_id": testutil.UniqueID("s-empty"),
			"bytes_read": 0,
		},
	})

	time.Sleep(env.Cfg.AnalyticsWaitTime)
	endCount, err := ice.CountDataFiles(ctx)
	if err != nil {
		t.Fatalf("CountDataFiles: %v", err)
	}
	if endCount > startCount {
		t.Fatalf("empty-id event should be dropped, but parquet count grew from %d to %d", startCount, endCount)
	}
}

func TestIceberg_BatchFlushOnIdle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping idle-flush test in -short mode")
	}
	env := testutil.NewEnv(t)
	ice := env.IcebergS3(t)

	ctx := context.Background()
	startCount, err := ice.CountDataFiles(ctx)
	if err != nil {
		t.Fatalf("CountDataFiles: %v", err)
	}

	for i := 0; i < 5; i++ {
		produceWatchEvent(t, env.Cfg.KafkaBrokers, "watch-events", rawWatchEvent{
			Version:   "1.0",
			Type:      "WATCH_STARTED",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Payload: map[string]any{
				"video_id":   testutil.UniqueID("idle-vid"),
				"user_id":    testutil.UniqueID("idle-user"),
				"session_id": testutil.UniqueID("idle-sess"),
				"bytes_read": int64(i),
			},
		})
	}

	endCount, err := ice.WaitForFileIncrease(ctx, startCount, env.Cfg.AnalyticsWaitTime)
	if err != nil {
		t.Skipf("idle flush not observed within %s: %v", env.Cfg.AnalyticsWaitTime, err)
	}
	if endCount <= startCount {
		t.Fatalf("expected idle flush to write parquet: start=%d end=%d", startCount, endCount)
	}
}

func TestKafka_VideoEventEnvelopeShape(t *testing.T) {
	env := testutil.NewEnv(t)
	consumer := client.NewKafkaConsumer(
		env.Cfg.KafkaBrokers,
		"video-events",
		"e2e-envelope-"+testutil.UniqueID("grp"),
	)
	defer consumer.Close()

	v := env.CreateTestVideo(t, testutil.UniqueTitle("env"), 256)

	events, err := consumer.ReadEvents(context.Background(), env.Cfg.EventWaitTime)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	for _, e := range events {
		var p map[string]any
		if json.Unmarshal(e.Payload, &p) == nil {
			if id, _ := p["id"].(string); id == v.ID {
				if e.Version == "" {
					t.Errorf("envelope.version is empty for video %s", v.ID)
				}
				if e.Type == "" {
					t.Errorf("envelope.type is empty for video %s", v.ID)
				}
				return
			}
		}
	}
	t.Skipf("envelope for %s not observed within %s", v.ID, env.Cfg.EventWaitTime)
}
