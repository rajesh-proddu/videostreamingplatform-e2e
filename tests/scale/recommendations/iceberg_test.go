package recommendations_scale

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/parquet-go/parquet-go"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// produceWatchEvents writes `n` synthetic WATCH_COMPLETED events to the
// watch-events topic. Returns the number actually produced.
func produceWatchEvents(t *testing.T, brokers string, n int) int {
	t.Helper()
	prod := client.NewKafkaProducer(brokers, "watch-events")
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	count := 0
	now := time.Now().UTC()
	for i := 0; i < n; i++ {
		evt := map[string]any{
			"version":   "1.0",
			"type":      "WATCH_COMPLETED",
			"timestamp": now.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339),
			"payload": map[string]any{
				"video_id":   fmt.Sprintf("scale-reco-ice-vid-%d", i),
				"user_id":    fmt.Sprintf("scale-reco-ice-user-%d", i%50),
				"session_id": fmt.Sprintf("scale-reco-ice-sess-%d", i),
				"bytes_read": int64(1024 + i),
			},
		}
		body, _ := json.Marshal(evt)
		if err := prod.WriteJSON(ctx, fmt.Sprintf("scale-reco-ice-%d", i), body); err != nil {
			t.Logf("kafka write %d: %v", i, err)
			break
		}
		count++
	}
	return count
}

// listParquetKeys returns all parquet keys under the watch_history data prefix.
func listParquetKeys(ctx context.Context, ice *client.IcebergS3Client) ([]string, time.Duration, error) {
	prefix := strings.TrimSuffix(ice.DataPath, "/") + "/"
	var keys []string
	var continuation *string
	start := time.Now()
	for {
		out, err := ice.S3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &ice.Bucket,
			Prefix:            &prefix,
			ContinuationToken: continuation,
		})
		if err != nil {
			return nil, time.Since(start), err
		}
		for _, obj := range out.Contents {
			if obj.Key != nil && strings.HasSuffix(*obj.Key, ".parquet") {
				keys = append(keys, *obj.Key)
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		continuation = out.NextContinuationToken
	}
	return keys, time.Since(start), nil
}

func getObject(ctx context.Context, ice *client.IcebergS3Client, key string) ([]byte, time.Duration, error) {
	start := time.Now()
	out, err := ice.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &ice.Bucket,
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, time.Since(start), err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	return b, time.Since(start), err
}

func countParquetRows(data []byte) (int64, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return 0, err
	}
	return f.NumRows(), nil
}

// ensureIcebergData makes sure there are parquet files in the table. If zero,
// it produces events and waits for the consumer to flush. Returns the final
// parquet count.
func ensureIcebergData(t *testing.T, env *testutil.Env, ice *client.IcebergS3Client) int {
	t.Helper()
	ctx := context.Background()
	startCount, err := ice.CountDataFiles(ctx)
	if err != nil {
		t.Skipf("CountDataFiles: %v", err)
	}
	target := envIntOr("SCALE_ICEBERG_EVENTS", 1000)
	if startCount > 0 {
		t.Logf("[iceberg_seed] %d parquet files already present; not seeding more", startCount)
		return startCount
	}
	t.Logf("[iceberg_seed] producing %d watch events", target)
	produced := produceWatchEvents(t, env.Cfg.KafkaBrokers, target)
	t.Logf("[iceberg_seed] produced %d events; waiting up to %s for consumer flush",
		produced, env.Cfg.AnalyticsWaitTime*4)
	// The watch-history-consumer batches/idle-flushes; give it a generous
	// window.
	end, err := ice.WaitForFileIncrease(ctx, startCount, env.Cfg.AnalyticsWaitTime*4)
	if err != nil {
		t.Skipf("Iceberg flush not observed: %v (start=%d end=%d)", err, startCount, end)
	}
	t.Logf("[iceberg_seed] parquet count: %d -> %d", startCount, end)
	return end
}

func TestIceberg_ListPartitions(t *testing.T) {
	env := testutil.NewEnv(t)
	ice := env.IcebergS3(t)
	_ = ensureIcebergData(t, env, ice)

	ctx := context.Background()
	// Three runs to get a noisy p50.
	var keys []string
	var lats []time.Duration
	for i := 0; i < 3; i++ {
		k, d, err := listParquetKeys(ctx, ice)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		keys = k
		lats = append(lats, d)
	}
	s := summarize(lats)
	t.Logf("[iceberg_list] files=%d runs=%d p50=%s max=%s",
		len(keys), s.N, s.P50, s.Max)
}

func TestIceberg_ParquetRead_SingleFile(t *testing.T) {
	env := testutil.NewEnv(t)
	ice := env.IcebergS3(t)
	_ = ensureIcebergData(t, env, ice)

	ctx := context.Background()
	keys, _, err := listParquetKeys(ctx, ice)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(keys) == 0 {
		t.Skip("no parquet files to read")
	}
	key := keys[0]
	data, fetchDur, err := getObject(ctx, ice, key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	parseStart := time.Now()
	rows, err := countParquetRows(data)
	parseDur := time.Since(parseStart)
	if err != nil {
		t.Fatalf("parse %s: %v", key, err)
	}
	totalSec := (fetchDur + parseDur).Seconds()
	mbps := float64(len(data)) / 1e6 / totalSec
	rowsPerSec := float64(rows) / totalSec
	t.Logf("[iceberg_read_one] key=%s size=%d_bytes rows=%d fetch=%s parse=%s rows/s=%.0f MB/s=%.2f",
		key, len(data), rows, fetchDur, parseDur, rowsPerSec, mbps)
}

func TestIceberg_FullScan_AllFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping full-scan in -short mode")
	}
	env := testutil.NewEnv(t)
	ice := env.IcebergS3(t)
	_ = ensureIcebergData(t, env, ice)

	ctx := context.Background()
	keys, listDur, err := listParquetKeys(ctx, ice)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(keys) == 0 {
		t.Skip("no parquet files to read")
	}

	totalBytes := int64(0)
	totalRows := int64(0)
	start := time.Now()
	for _, key := range keys {
		data, _, err := getObject(ctx, ice, key)
		if err != nil {
			t.Logf("get %s: %v", key, err)
			continue
		}
		totalBytes += int64(len(data))
		rows, err := countParquetRows(data)
		if err != nil {
			t.Logf("parse %s: %v", key, err)
			continue
		}
		totalRows += rows
	}
	elapsed := time.Since(start)
	mbps := float64(totalBytes) / 1e6 / elapsed.Seconds()
	rowsPerSec := float64(totalRows) / elapsed.Seconds()
	t.Logf("[iceberg_full_scan] files=%d list=%s read+parse=%s bytes=%d rows=%d MB/s=%.2f rows/s=%.0f",
		len(keys), listDur, elapsed, totalBytes, totalRows, mbps, rowsPerSec)
}
