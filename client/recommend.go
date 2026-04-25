package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Recommendation struct {
	VideoID string  `json:"video_id"`
	Title   string  `json:"title"`
	Score   float64 `json:"score"`
	Reason  string  `json:"reason"`
}

type RecommendationResponse struct {
	UserID          string           `json:"user_id"`
	Recommendations []Recommendation `json:"recommendations"`
	Query           string           `json:"query,omitempty"`
}

type RecommendRequest struct {
	UserID string `json:"user_id"`
	Query  string `json:"query,omitempty"`
	Limit  int    `json:"limit"`
}

type RecommendClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewRecommendClient(baseURL string, timeout time.Duration) *RecommendClient {
	return &RecommendClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: timeout},
	}
}

func (c *RecommendClient) Recommend(ctx context.Context, req *RecommendRequest) (*RecommendationResponse, *http.Response, error) {
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/v1/recommend", bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, resp, fmt.Errorf("recommend: status %d, body: %s", resp.StatusCode, respBody)
	}
	var r RecommendationResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, resp, err
	}
	return &r, resp, nil
}

func (c *RecommendClient) RawRecommend(ctx context.Context, raw []byte) (*http.Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/v1/recommend", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	return c.HTTPClient.Do(httpReq)
}

func (c *RecommendClient) Health() (int, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/health")
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

func RecommendViaProxy(httpClient *http.Client, metadataBase, userID, query string) (*RecommendationResponse, *http.Response, error) {
	q := url.Values{}
	q.Set("user_id", userID)
	if query != "" {
		q.Set("query", query)
	}
	resp, err := httpClient.Get(metadataBase + "/recommendations?" + q.Encode())
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, resp, fmt.Errorf("proxy recommend: status %d, body: %s", resp.StatusCode, body)
	}
	var r RecommendationResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, resp, err
	}
	return &r, resp, nil
}
