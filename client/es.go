package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ESDoc struct {
	Found  bool                   `json:"found"`
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

type esSearchResponse struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			ID     string                 `json:"_id"`
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type ESClient struct {
	BaseURL    string
	Index      string
	HTTPClient *http.Client
}

func NewESClient(baseURL, index string, timeout time.Duration) *ESClient {
	return &ESClient{
		BaseURL:    baseURL,
		Index:      index,
		HTTPClient: &http.Client{Timeout: timeout},
	}
}

func (c *ESClient) GetDoc(id string) (*ESDoc, *http.Response, error) {
	url := fmt.Sprintf("%s/%s/_doc/%s", c.BaseURL, c.Index, id)
	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return &ESDoc{Found: false, ID: id}, resp, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, resp, fmt.Errorf("get doc: status %d, body: %s", resp.StatusCode, body)
	}
	var doc ESDoc
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, resp, err
	}
	return &doc, resp, nil
}

func (c *ESClient) WaitForDoc(id string, mustExist bool, timeout time.Duration) (*ESDoc, error) {
	deadline := time.Now().Add(timeout)
	for {
		doc, _, err := c.GetDoc(id)
		if err == nil && doc.Found == mustExist {
			return doc, nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return nil, fmt.Errorf("waiting for doc %s: %w", id, err)
			}
			return doc, fmt.Errorf("timeout: doc %s found=%v, want found=%v", id, doc.Found, mustExist)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *ESClient) SearchTitle(q string) (int, error) {
	url := fmt.Sprintf("%s/%s/_search?q=title:%s", c.BaseURL, c.Index, q)
	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("search: status %d, body: %s", resp.StatusCode, body)
	}
	var r esSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return 0, err
	}
	return r.Hits.Total.Value, nil
}

func (c *ESClient) RefreshIndex() error {
	url := fmt.Sprintf("%s/%s/_refresh", c.BaseURL, c.Index)
	resp, err := c.HTTPClient.Post(url, "application/json", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *ESClient) Health() (int, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/_cluster/health")
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}
