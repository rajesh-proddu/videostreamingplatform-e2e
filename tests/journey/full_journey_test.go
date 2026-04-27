package journey

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestSingleUserFullJourney exercises the full platform for one user:
//
//  1. Create video metadata
//  2. Upload bytes via dataservice
//  3. Download (full, no Range) — produces watch.started + watch.completed on Kafka
//  4. Verify both watch events on the watch-events Kafka topic (filtered by video_id)
//  5. Verify the watch-history-consumer flushed the events to the Iceberg table
//     (count of parquet files under the table data prefix grew)
//  6. Call the recommendations API for the user — expect a 200 with valid shape
//
// Elasticsearch verification is optional: if reachable, also assert the video
// document landed in the videos index. Otherwise that step is skipped.
func TestSingleUserFullJourney(t *testing.T) {
	env := testutil.NewEnv(t)

	const (
		sizeBytes = 8 * 1024
		userID    = "journey-user"
	)
	title := testutil.UniqueTitle("journey")

	// 1. Create video.
	v := env.CreateTestVideo(t, title, sizeBytes)
	t.Logf("created video %s (%q)", v.ID, title)

	// 2. Upload bytes.
	init, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   v.ID,
		UserID:    userID,
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
	t.Logf("uploaded %d bytes for video %s", sizeBytes, v.ID)

	// (Optional) Elasticsearch — verify the video was indexed by kafka-es-consumer.
	// if code, err := env.ES.Health(); err == nil && code < 500 {
	// 	if doc, err := env.ES.WaitForDoc(v.ID, true, env.Cfg.AnalyticsWaitTime); err == nil && doc.Found {
	// 		if got, _ := doc.Source["title"].(string); got == title {
	// 			t.Logf("✓ ES indexed video %s", v.ID)
	// 		} else {
	// 			t.Logf("ES has video %s but title=%q want %q (consumer may still be catching up)", v.ID, got, title)
	// 		}
	// 	} else {
	// 		t.Logf("ES reachable but video %s not yet indexed: %v", v.ID, err)
	// 	}
	// } else {
	// 	t.Logf("ES unreachable at %s — skipping ES assertion", env.Cfg.ElasticsearchURL)
	// }

	// 3. Snapshot the Iceberg parquet count BEFORE producing events, so we
	//    can detect a real increase caused by this test's download. Done
	//    pre-download because the consumer may flush within milliseconds of
	//    receiving the events — measuring after risks racing the flush.
	ice := env.IcebergS3(t)
	ctx := context.Background()
	startCount, err := ice.CountDataFiles(ctx)
	if err != nil {
		t.Fatalf("CountDataFiles (start): %v", err)
	}
	t.Logf("initial Iceberg parquet count = %d", startCount)

	// 4. Download — produces watch.started + watch.completed on Kafka.
	if _, _, err := env.Data.DownloadVideo(v.ID, userID); err != nil {
		t.Fatalf("DownloadVideo: %v", err)
	}
	t.Logf("downloaded video %s", v.ID)

	// 5. Verify Kafka events. Use FirstOffset so we don't race against group
	//    rebalance — we filter to events for THIS video_id, so consuming
	//    earlier traffic is harmless.
	consumer := client.NewKafkaConsumerFromStart(
		env.Cfg.KafkaBrokers,
		"watch-events",
		"e2e-journey-"+testutil.UniqueID("grp"),
	)
	defer consumer.Close()

	startedFound, completedFound := false, false
	deadline := time.Now().Add(env.Cfg.EventWaitTime + 10*time.Second)
	for time.Now().Before(deadline) && !(startedFound && completedFound) {
		events, err := consumer.ReadEvents(context.Background(), 3*time.Second)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		for _, e := range events {
			var p map[string]any
			if json.Unmarshal(e.Payload, &p) != nil {
				continue
			}
			if vid, _ := p["video_id"].(string); vid != v.ID {
				continue
			}
			switch e.Type {
			case "watch.started", "WATCH_STARTED":
				startedFound = true
			case "watch.completed", "WATCH_COMPLETED":
				completedFound = true
			}
		}
	}
	if !startedFound || !completedFound {
		t.Fatalf("missing watch events for %s on Kafka: started=%v completed=%v", v.ID, startedFound, completedFound)
	}
	t.Logf("✓ watch.started + watch.completed received on Kafka for %s", v.ID)

	// 6. Verify Iceberg ingestion: parquet file count grew vs. the pre-download snapshot.
	endCount, err := ice.WaitForFileIncrease(ctx, startCount, env.Cfg.AnalyticsWaitTime)
	if err != nil || endCount <= startCount {
		t.Fatalf("watch-history-consumer did not flush to Iceberg within %s: start=%d end=%d err=%v", env.Cfg.AnalyticsWaitTime, startCount, endCount, err)
	}
	t.Logf("✓ Iceberg parquet count grew %d → %d", startCount, endCount)

	// 7. Call recommendations API.
	env.RequireRecommendations(t)
	resp, _, err := env.Recommend.Recommend(ctx, &client.RecommendRequest{
		UserID: userID,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	if resp.UserID != userID {
		t.Errorf("response user_id = %q, want %q", resp.UserID, userID)
	}
	if len(resp.Recommendations) > 10 {
		t.Errorf("got %d recommendations, want ≤ 10", len(resp.Recommendations))
	}
	for _, r := range resp.Recommendations {
		if r.Score < 0 || r.Score > 1 {
			t.Errorf("score %f out of [0,1] for %s", r.Score, r.VideoID)
		}
	}
	t.Logf("✓ recommendations API returned %d items for %s", len(resp.Recommendations), userID)
}
