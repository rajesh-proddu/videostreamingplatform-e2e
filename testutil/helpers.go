// Package testutil provides shared helpers for e2e tests.
package testutil

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/config"
)

// Env holds initialized clients for all services. Created once per test suite.
type Env struct {
	Cfg      *config.Config
	Metadata *client.MetadataClient
	Data     *client.DataClient
}

// NewEnv creates a test environment with all clients.
func NewEnv(t *testing.T) *Env {
	t.Helper()
	cfg := config.Load()
	return &Env{
		Cfg:      cfg,
		Metadata: client.NewMetadataClient(cfg.MetadataServiceURL, cfg.HTTPTimeout),
		Data:     client.NewDataClient(cfg.DataServiceURL, cfg.UploadTimeout),
	}
}

// CreateTestVideo creates a video and registers cleanup.
func (e *Env) CreateTestVideo(t *testing.T, title string, sizeBytes int64) *client.Video {
	t.Helper()
	v, _, err := e.Metadata.CreateVideo(&client.CreateVideoRequest{
		Title:       title,
		Description: "e2e test video",
		SizeBytes:   sizeBytes,
	})
	if err != nil {
		t.Fatalf("CreateTestVideo(%q): %v", title, err)
	}
	t.Cleanup(func() {
		resp, _ := e.Metadata.DeleteVideo(v.ID)
		if resp != nil {
			resp.Body.Close()
		}
	})
	return v
}

// RandomBytes generates n random bytes for upload payloads.
func RandomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// UniqueTitle generates a unique video title for test isolation.
func UniqueTitle(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%x", prefix, b)
}
