// Package client provides HTTP clients for the video streaming platform services.
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Video represents a video resource from the metadata service.
type Video struct {
	ID             string `json:"id"`
	Title          string `json:"title"`
	Description    string `json:"description"`
	Duration       int    `json:"duration"`
	SizeBytes      int64  `json:"size_bytes"`
	Format         string `json:"format"`
	UploadProgress int    `json:"upload_progress"`
	UploadStatus   string `json:"upload_status"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

// CreateVideoRequest is the request body for creating a video.
type CreateVideoRequest struct {
	Title           string `json:"title"`
	Description     string `json:"description,omitempty"`
	DurationSeconds int    `json:"duration_seconds,omitempty"`
	SizeBytes       int64  `json:"size_bytes"`
	Format          string `json:"format,omitempty"`
}

// UpdateVideoRequest is the request body for updating a video.
type UpdateVideoRequest struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
}

// VideoList is the response from listing videos.
type VideoList struct {
	Videos []Video `json:"videos"`
	Count  int     `json:"count"`
}

// MetadataClient wraps HTTP calls to the metadata service.
type MetadataClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewMetadataClient(baseURL string, timeout time.Duration) *MetadataClient {
	return &MetadataClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: timeout},
	}
}

func (c *MetadataClient) CreateVideo(req *CreateVideoRequest) (*Video, *http.Response, error) {
	body, _ := json.Marshal(req)
	resp, err := c.HTTPClient.Post(c.BaseURL+"/videos", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, resp, fmt.Errorf("create video: status %d, body: %s", resp.StatusCode, respBody)
	}
	var v Video
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return nil, resp, err
	}
	return &v, resp, nil
}

func (c *MetadataClient) GetVideo(id string) (*Video, *http.Response, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/videos/" + id)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, resp, fmt.Errorf("get video: status %d, body: %s", resp.StatusCode, respBody)
	}
	var v Video
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return nil, resp, err
	}
	return &v, resp, nil
}

func (c *MetadataClient) UpdateVideo(id string, req *UpdateVideoRequest) (*Video, *http.Response, error) {
	body, _ := json.Marshal(req)
	httpReq, _ := http.NewRequest(http.MethodPut, c.BaseURL+"/videos/"+id, bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, resp, fmt.Errorf("update video: status %d, body: %s", resp.StatusCode, respBody)
	}
	var v Video
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return nil, resp, err
	}
	return &v, resp, nil
}

func (c *MetadataClient) DeleteVideo(id string) (*http.Response, error) {
	httpReq, _ := http.NewRequest(http.MethodDelete, c.BaseURL+"/videos/"+id, nil)
	return c.HTTPClient.Do(httpReq)
}

func (c *MetadataClient) ListVideos(limit, offset int) (*VideoList, *http.Response, error) {
	url := fmt.Sprintf("%s/videos?limit=%d&offset=%d", c.BaseURL, limit, offset)
	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, resp, fmt.Errorf("list videos: status %d, body: %s", resp.StatusCode, respBody)
	}
	var list VideoList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, resp, err
	}
	return &list, resp, nil
}

func (c *MetadataClient) Health() (int, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/health")
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// RawGet performs a raw GET and returns the response (caller must close body).
func (c *MetadataClient) RawGet(path string) (*http.Response, error) {
	return c.HTTPClient.Get(c.BaseURL + path)
}

// RawPost performs a raw POST and returns the response (caller must close body).
func (c *MetadataClient) RawPost(path string, contentType string, body io.Reader) (*http.Response, error) {
	return c.HTTPClient.Post(c.BaseURL+path, contentType, body)
}
