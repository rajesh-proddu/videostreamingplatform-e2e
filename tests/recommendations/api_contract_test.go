package recommendations

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestReco_NativeAPI_EmptyState_ReturnsOK(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)

	resp, _, err := env.Recommend.Recommend(context.Background(), &client.RecommendRequest{
		UserID: testutil.UniqueID("nostate-user"),
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	if len(resp.Recommendations) > 10 {
		t.Errorf("returned %d > limit=10 recommendations", len(resp.Recommendations))
	}
	for _, r := range resp.Recommendations {
		if r.Score < 0 || r.Score > 1 {
			t.Errorf("score %f out of [0,1] for %s", r.Score, r.VideoID)
		}
	}
}

func TestReco_NativeAPI_LimitTooLow_422(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)

	resp, err := env.Recommend.RawRecommend(context.Background(), []byte(`{"user_id":"x","limit":0}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("limit=0 status = %d, want 422", resp.StatusCode)
	}
}

func TestReco_NativeAPI_LimitTooHigh_422(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)

	resp, err := env.Recommend.RawRecommend(context.Background(), []byte(`{"user_id":"x","limit":51}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("limit=51 status = %d, want 422", resp.StatusCode)
	}
}

func TestReco_NativeAPI_LimitMaxAllowed(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)

	resp, _, err := env.Recommend.Recommend(context.Background(), &client.RecommendRequest{
		UserID: testutil.UniqueID("max-user"),
		Limit:  50,
	})
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	if len(resp.Recommendations) > 50 {
		t.Fatalf("returned %d > 50", len(resp.Recommendations))
	}
}

func TestReco_NativeAPI_MissingUserID_422(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireRecommendations(t)

	resp, err := env.Recommend.RawRecommend(context.Background(), []byte(`{"limit":10}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("missing user_id status = %d, want 422", resp.StatusCode)
	}
}

func TestReco_Proxy_MissingUserID_400(t *testing.T) {
	env := testutil.NewEnv(t)
	httpClient := &http.Client{Timeout: env.Cfg.HTTPTimeout}

	resp, err := httpClient.Get(env.Cfg.MetadataServiceURL + "/recommendations")
	if err != nil {
		t.Fatalf("GET /recommendations: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable {
		t.Skip("recommendation proxy disabled at metadataservice (RECOMMENDATION_SERVICE_URL unset)")
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestReco_Proxy_HappyPath_ReturnsResponse(t *testing.T) {
	env := testutil.NewEnv(t)
	httpClient := &http.Client{Timeout: env.Cfg.HTTPTimeout}

	r, resp, err := client.RecommendViaProxy(httpClient, env.Cfg.MetadataServiceURL, testutil.UniqueID("proxy-user"), "")
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusServiceUnavailable {
			t.Skip("recommendation proxy disabled at metadataservice")
		}
		t.Fatalf("RecommendViaProxy: %v", err)
	}
	if r.UserID == "" {
		t.Errorf("user_id empty in proxy response")
	}
}

func TestReco_Proxy_RespectsLimitQueryParam(t *testing.T) {
	env := testutil.NewEnv(t)
	httpClient := &http.Client{Timeout: env.Cfg.HTTPTimeout}

	url := env.Cfg.MetadataServiceURL + "/recommendations?user_id=" + testutil.UniqueID("limit-user") + "&limit=3"
	resp, err := httpClient.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable {
		t.Skip("recommendation proxy disabled at metadataservice")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var body client.RecommendationResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Recommendations) > 3 {
		t.Fatalf("got %d recommendations, want ≤ 3 (query limit)", len(body.Recommendations))
	}
}

func TestReco_Health(t *testing.T) {
	env := testutil.NewEnv(t)

	code, err := env.Recommend.Health()
	if err != nil {
		t.Skipf("recommend service unreachable: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("/health status = %d, want 200", code)
	}
}
