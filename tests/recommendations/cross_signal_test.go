package recommendations

import (
	"context"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestReco_SeesVideoCreatedViaESPipeline(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireES(t)
	env.RequireRecommendations(t)

	uniqueWord := testutil.UniqueID("xpipe")
	v := env.CreateTestVideo(t, uniqueWord+" cross-signal flow", 512)

	if _, err := env.ES.WaitForDoc(v.ID, true, env.Cfg.AnalyticsWaitTime); err != nil {
		t.Fatalf("video did not land in ES: %v", err)
	}

	resp, _, err := env.Recommend.Recommend(context.Background(), &client.RecommendRequest{
		UserID: testutil.UniqueID("xpipe-user"),
		Query:  uniqueWord,
		Limit:  20,
	})
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	for _, r := range resp.Recommendations {
		if r.VideoID == v.ID {
			return
		}
	}
	t.Fatalf("video %s created via metadataservice did not surface as a recommendation via the ES path", v.ID)
}

func TestReco_VideoDeletedDisappearsFromRecs(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireES(t)
	env.RequireRecommendations(t)

	uniqueWord := testutil.UniqueID("delxpipe")
	v := env.CreateTestVideo(t, uniqueWord+" to be deleted", 512)
	if _, err := env.ES.WaitForDoc(v.ID, true, env.Cfg.AnalyticsWaitTime); err != nil {
		t.Fatalf("video did not land in ES: %v", err)
	}

	resp, err := env.Metadata.DeleteVideo(v.ID)
	if err != nil {
		t.Fatalf("DeleteVideo: %v", err)
	}
	resp.Body.Close()

	if _, err := env.ES.WaitForDoc(v.ID, false, env.Cfg.AnalyticsWaitTime); err != nil {
		t.Fatalf("doc still in ES after delete: %v", err)
	}

	rec, _, err := env.Recommend.Recommend(context.Background(), &client.RecommendRequest{
		UserID: testutil.UniqueID("delxpipe-user"),
		Query:  uniqueWord,
		Limit:  20,
	})
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	for _, r := range rec.Recommendations {
		if r.VideoID == v.ID {
			t.Fatalf("deleted video %s still surfaced in recommendations", v.ID)
		}
	}
}
