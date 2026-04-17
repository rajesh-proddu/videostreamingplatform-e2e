package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// UploadInitiateRequest is the request to start an upload.
type UploadInitiateRequest struct {
	VideoID   string `json:"video_id"`
	UserID    string `json:"user_id"`
	TotalSize int64  `json:"total_size"`
}

// UploadInitiateResponse is returned when an upload is initiated.
type UploadInitiateResponse struct {
	UploadID  string `json:"upload_id"`
	ChunkSize int64  `json:"chunk_size"`
	Message   string `json:"message"`
}

// UploadProgress is the response from the progress endpoint.
type UploadProgress struct {
	ID               string  `json:"id"`
	VideoID          string  `json:"video_id"`
	UserID           string  `json:"user_id"`
	TotalSize        int64   `json:"total_size"`
	UploadedSize     int64   `json:"uploaded_size"`
	UploadedChunks   int     `json:"uploaded_chunks"`
	TotalChunks      int     `json:"total_chunks"`
	Status           string  `json:"status"`
	Percentage       float64 `json:"percentage"`
	SpeedMbps        float64 `json:"speed_mbps"`
	EstimatedSeconds float64 `json:"estimated_seconds"`
}

// CompleteUploadResponse is returned when an upload is completed.
type CompleteUploadResponse struct {
	UploadID string `json:"upload_id"`
	Status   string `json:"status"`
	Message  string `json:"message"`
}

// DataClient wraps HTTP calls to the data service.
type DataClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewDataClient(baseURL string, timeout time.Duration) *DataClient {
	return &DataClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: timeout},
	}
}

func (c *DataClient) InitiateUpload(req *UploadInitiateRequest) (*UploadInitiateResponse, *http.Response, error) {
	body, _ := json.Marshal(req)
	resp, err := c.HTTPClient.Post(c.BaseURL+"/uploads/initiate", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, resp, fmt.Errorf("initiate upload: status %d, body: %s", resp.StatusCode, respBody)
	}
	var r UploadInitiateResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, resp, err
	}
	return &r, resp, nil
}

func (c *DataClient) UploadChunk(uploadID string, chunkIndex int, data []byte) (*http.Response, error) {
	url := fmt.Sprintf("%s/uploads/%s/chunks?chunkIndex=%d", c.BaseURL, uploadID, chunkIndex)
	resp, err := c.HTTPClient.Post(url, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return resp, nil
}

func (c *DataClient) GetProgress(uploadID string) (*UploadProgress, *http.Response, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/uploads/" + uploadID + "/progress")
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, resp, fmt.Errorf("get progress: status %d, body: %s", resp.StatusCode, respBody)
	}
	var p UploadProgress
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return nil, resp, err
	}
	return &p, resp, nil
}

func (c *DataClient) CompleteUpload(uploadID string) (*CompleteUploadResponse, *http.Response, error) {
	resp, err := c.HTTPClient.Post(c.BaseURL+"/uploads/"+uploadID+"/complete", "application/json", nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, resp, fmt.Errorf("complete upload: status %d, body: %s", resp.StatusCode, respBody)
	}
	var r CompleteUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, resp, err
	}
	return &r, resp, nil
}

func (c *DataClient) DownloadVideo(videoID, userID string) ([]byte, *http.Response, error) {
	url := fmt.Sprintf("%s/videos/%s/download", c.BaseURL, videoID)
	if userID != "" {
		url += "?user_id=" + userID
	}
	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp, err
	}
	return data, resp, nil
}

func (c *DataClient) Health() (int, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/health")
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}
