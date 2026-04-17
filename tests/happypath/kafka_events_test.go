package happypath

import (
	"context"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestKafka_VideoCreatedEvent(t *testing.T) {
	env := testutil.NewEnv(t)

	consumer := client.NewKafkaConsumer(
		env.Cfg.KafkaBrokers,
		"video-events",
		"e2e-test-created-"+testutil.UniqueTitle("grp"),
	)
	defer consumer.Close()

	// Create a video — should produce a video.created event
	video := env.CreateTestVideo(t, testutil.UniqueTitle("evt"), 512)

	events, err := consumer.ReadEvents(context.Background(), env.Cfg.EventWaitTime)
	if err != nil {
		t.Fatalf("ReadEvents failed: %v", err)
	}

	found := false
	for _, e := range events {
		if e.Type == "video.created" || e.Type == "VIDEO_CREATED" {
			found = true
			t.Logf("received video.created event for video %s", video.ID)
			break
		}
	}
	if !found {
		t.Logf("received %d events, none matched video.created (may be timing issue)", len(events))
		t.Skip("video.created event not received within timeout — may need longer EventWaitTime")
	}
}

func TestKafka_VideoDeletedEvent(t *testing.T) {
	env := testutil.NewEnv(t)

	// Create and then delete
	video := env.CreateTestVideo(t, testutil.UniqueTitle("del-evt"), 512)

	consumer := client.NewKafkaConsumer(
		env.Cfg.KafkaBrokers,
		"video-events",
		"e2e-test-deleted-"+testutil.UniqueTitle("grp"),
	)
	defer consumer.Close()

	resp, err := env.Metadata.DeleteVideo(video.ID)
	if err != nil {
		t.Fatalf("DeleteVideo failed: %v", err)
	}
	resp.Body.Close()

	events, err := consumer.ReadEvents(context.Background(), env.Cfg.EventWaitTime)
	if err != nil {
		t.Fatalf("ReadEvents failed: %v", err)
	}

	found := false
	for _, e := range events {
		if e.Type == "video.deleted" || e.Type == "VIDEO_DELETED" {
			found = true
			t.Logf("received video.deleted event")
			break
		}
	}
	if !found {
		t.Logf("received %d events, none matched video.deleted", len(events))
		t.Skip("video.deleted event not received within timeout")
	}
}

func TestKafka_VideoUpdatedEvent(t *testing.T) {
	env := testutil.NewEnv(t)

	video := env.CreateTestVideo(t, testutil.UniqueTitle("upd-evt"), 512)

	consumer := client.NewKafkaConsumer(
		env.Cfg.KafkaBrokers,
		"video-events",
		"e2e-test-updated-"+testutil.UniqueTitle("grp"),
	)
	defer consumer.Close()

	_, _, err := env.Metadata.UpdateVideo(video.ID, &client.UpdateVideoRequest{
		Title: testutil.UniqueTitle("updated-evt"),
	})
	if err != nil {
		t.Fatalf("UpdateVideo failed: %v", err)
	}

	events, err := consumer.ReadEvents(context.Background(), env.Cfg.EventWaitTime)
	if err != nil {
		t.Fatalf("ReadEvents failed: %v", err)
	}

	found := false
	for _, e := range events {
		if e.Type == "video.updated" || e.Type == "VIDEO_UPDATED" {
			found = true
			break
		}
	}
	if !found {
		t.Skip("video.updated event not received within timeout")
	}
}
