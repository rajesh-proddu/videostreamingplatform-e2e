// Package testutil provides shared helpers for e2e tests.
package testutil

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/config"
)

type Env struct {
	Cfg       *config.Config
	Metadata  *client.MetadataClient
	Data      *client.DataClient
	ES        *client.ESClient
	Recommend *client.RecommendClient
}

func NewEnv(t *testing.T) *Env {
	t.Helper()
	cfg := config.Load()
	return &Env{
		Cfg:       cfg,
		Metadata:  client.NewMetadataClient(cfg.MetadataServiceURL, cfg.HTTPTimeout),
		Data:      client.NewDataClient(cfg.DataServiceURL, cfg.UploadTimeout),
		ES:        client.NewESClient(cfg.ElasticsearchURL, cfg.ESVideoIndex, cfg.HTTPTimeout),
		Recommend: client.NewRecommendClient(cfg.RecommendationServiceURL, cfg.HTTPTimeout),
	}
}

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

func (e *Env) PgVector(t *testing.T) *client.PgVectorClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pg, err := client.NewPgVectorClient(ctx, e.Cfg.PgVectorDSN)
	if err != nil {
		t.Skipf("pgvector unreachable at %s: %v", e.Cfg.PgVectorDSN, err)
	}
	if err := pg.EnsureWatchHistoryTable(ctx); err != nil {
		pg.Close()
		t.Skipf("watch_history schema not available: %v", err)
	}
	t.Cleanup(pg.Close)
	return pg
}

func (e *Env) SeedAndCleanupHistory(t *testing.T, pg *client.PgVectorClient, userID string, videoIDs []string) {
	t.Helper()
	ctx := context.Background()
	if err := pg.SeedHistory(ctx, userID, videoIDs); err != nil {
		t.Fatalf("SeedHistory: %v", err)
	}
	t.Cleanup(func() {
		_ = pg.DeleteUserHistory(context.Background(), userID)
	})
}

func (e *Env) IcebergS3(t *testing.T) *client.IcebergS3Client {
	t.Helper()
	ctx := context.Background()
	c, err := client.NewIcebergS3Client(
		ctx,
		e.Cfg.S3Endpoint,
		e.Cfg.S3Region,
		e.Cfg.S3AccessKey,
		e.Cfg.S3SecretKey,
		e.Cfg.IcebergWarehouseBucket,
		e.Cfg.IcebergTablePrefix,
	)
	if err != nil {
		t.Skipf("S3 client init failed: %v", err)
	}
	if _, err := c.CountDataFiles(ctx); err != nil {
		t.Skipf("Iceberg warehouse unreachable at %s/%s: %v", e.Cfg.S3Endpoint, e.Cfg.IcebergWarehouseBucket, err)
	}
	return c
}

func (e *Env) RequireES(t *testing.T) {
	t.Helper()
	code, err := e.ES.Health()
	if err != nil || code >= 500 {
		t.Skipf("Elasticsearch unreachable at %s: code=%d err=%v", e.Cfg.ElasticsearchURL, code, err)
	}
}

func (e *Env) RequireRecommendations(t *testing.T) {
	t.Helper()
	code, err := e.Recommend.Health()
	if err != nil || code >= 500 {
		t.Skipf("Recommendation service unreachable at %s: code=%d err=%v", e.Cfg.RecommendationServiceURL, code, err)
	}
}

func RandomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func UniqueTitle(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%x", prefix, b)
}

func UniqueID(prefix string) string {
	return UniqueTitle(prefix)
}
