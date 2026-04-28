package client

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

type CDNResponse struct {
	StatusCode  int
	CacheStatus string // X-Cache-Status header (MISS / HIT / EXPIRED / STALE / BYPASS)
	ServedBy    string // X-Served-By header
	Body        []byte
}

type CDNClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewCDNClient(baseURL string, timeout time.Duration) *CDNClient {
	return &CDNClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: timeout},
	}
}

// GetVideo fetches /videos/{id} through the CDN proxy and returns body + cache headers.
func (c *CDNClient) GetVideo(videoID string) (*CDNResponse, error) {
	resp, err := c.HTTPClient.Get(fmt.Sprintf("%s/videos/%s", c.BaseURL, videoID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &CDNResponse{
		StatusCode:  resp.StatusCode,
		CacheStatus: resp.Header.Get("X-Cache-Status"),
		ServedBy:    resp.Header.Get("X-Served-By"),
		Body:        body,
	}, nil
}

func (c *CDNClient) Health() (int, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/health")
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}
