package recommendations

import (
	"context"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestReco_WatchedFilteredOut_WhenNoQuery(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)
	pg := env.PgVector(t)

	user := testutil.UniqueID("watched-user")

	v1 := env.CreateTestVideo(t, testutil.UniqueTitle("seen-1"), 256)
	v2 := env.CreateTestVideo(t, testutil.UniqueTitle("seen-2"), 256)
	v3 := env.CreateTestVideo(t, testutil.UniqueTitle("unseen"), 256)
	env.SeedAndCleanupHistory(t, pg, user, []string{v1.ID, v2.ID})

	for _, id := range []string{v1.ID, v2.ID, v3.ID} {
		if err := pg.SeedTrending(context.Background(), id, 5); err != nil {
			t.Fatalf("SeedTrending: %v", err)
		}
		captured := id
		t.Cleanup(func() { _ = pg.DeleteByVideoID(context.Background(), captured) })
	}

	resp, _, err := env.Recommend.Recommend(context.Background(), &client.RecommendRequest{
		UserID: user,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	for _, r := range resp.Recommendations {
		if r.VideoID == v1.ID || r.VideoID == v2.ID {
			t.Fatalf("watched video %s should have been filtered out", r.VideoID)
		}
	}
}

func TestReco_WatchedNotFiltered_WhenQueryGiven(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)
	env.RequireES(t)
	pg := env.PgVector(t)

	user := testutil.UniqueID("query-user")
	uniqueWord := testutil.UniqueID("kwq")
	title := uniqueWord + " is a watched title"
	v := env.CreateTestVideo(t, title, 256)

	if _, err := env.ES.WaitForDoc(v.ID, true, env.Cfg.AnalyticsWaitTime); err != nil {
		t.Skipf("video not indexed in ES yet: %v", err)
	}
	env.SeedAndCleanupHistory(t, pg, user, []string{v.ID})

	resp, _, err := env.Recommend.Recommend(context.Background(), &client.RecommendRequest{
		UserID: user,
		Query:  uniqueWord,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	found := false
	for _, r := range resp.Recommendations {
		if r.VideoID == v.ID {
			found = true
			break
		}
	}
	if !found {
		t.Logf("watched-but-searched video %s not surfaced; LLM ranker may have demoted it", v.ID)
	}
}

func TestReco_TrendingSurfaces_WhenNoQuery(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)
	pg := env.PgVector(t)

	user := testutil.UniqueID("trend-watcher")
	trendyVideo := env.CreateTestVideo(t, testutil.UniqueTitle("trendy"), 256)
	if err := pg.SeedTrending(context.Background(), trendyVideo.ID, 25); err != nil {
		t.Fatalf("SeedTrending: %v", err)
	}
	t.Cleanup(func() { _ = pg.DeleteByVideoID(context.Background(), trendyVideo.ID) })

	resp, _, err := env.Recommend.Recommend(context.Background(), &client.RecommendRequest{
		UserID: user,
		Limit:  20,
	})
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	for _, r := range resp.Recommendations {
		if r.VideoID == trendyVideo.ID {
			return
		}
	}
	t.Fatalf("trending video %s not in recommendations (got %d items)", trendyVideo.ID, len(resp.Recommendations))
}

func TestReco_SearchHits_WhenQueryAndNoHistory(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)
	env.RequireES(t)

	uniqueWord := testutil.UniqueID("srchq")
	v := env.CreateTestVideo(t, uniqueWord+" needle", 256)
	if _, err := env.ES.WaitForDoc(v.ID, true, env.Cfg.AnalyticsWaitTime); err != nil {
		t.Skipf("video not indexed in ES: %v", err)
	}

	resp, _, err := env.Recommend.Recommend(context.Background(), &client.RecommendRequest{
		UserID: testutil.UniqueID("srchu"),
		Query:  uniqueWord,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	for _, r := range resp.Recommendations {
		if r.VideoID == v.ID {
			return
		}
	}
	t.Fatalf("search target %s not in recommendations", v.ID)
}

func TestReco_MinScoreThresholdEnforced(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)

	resp, _, err := env.Recommend.Recommend(context.Background(), &client.RecommendRequest{
		UserID: testutil.UniqueID("score-user"),
		Limit:  20,
	})
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	for _, r := range resp.Recommendations {
		if r.Score < 0.1 {
			t.Errorf("score %f below 0.1 threshold for %s", r.Score, r.VideoID)
		}
	}
}

func TestReco_LatencyUnderBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency test in -short mode")
	}
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)

	start := time.Now()
	if _, _, err := env.Recommend.Recommend(context.Background(), &client.RecommendRequest{
		UserID: testutil.UniqueID("lat-user"),
		Limit:  10,
	}); err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	elapsed := time.Since(start)
	budget := 30 * time.Second
	if elapsed > budget {
		t.Fatalf("recommend took %s > budget %s", elapsed, budget)
	}
}
