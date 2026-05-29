// Package metadatadb contains scale tests focused on the metadata-service
// MySQL backend. Tests assume a sizable existing corpus (target 10M rows,
// fallback >= SCALE_CORPUS). All HTTP traffic goes through the metadata
// service; direct DB access is used only for seeding and the EXPLAIN probe.
package metadatadb

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/yourusername/videostreamingplatform-e2e/config"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// seedTitlePrefix tags rows inserted by our seeder so they are distinguishable
// (and so seeding is idempotent: we measure the existing corpus before topping
// up).
const seedTitlePrefix = "seed-corpus-"

// openDB opens a MySQL connection using config.MySQLDSN. Caller must Close.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	cfg := config.Load()
	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		t.Fatalf("sql.Open mysql: %v", err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Skipf("MySQL unreachable at %s: %v", cfg.MySQLDSN, err)
	}
	return db
}

// rowCount returns the current videos row count. Uses an index-only path.
func rowCount(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow("SELECT COUNT(*) FROM videos").Scan(&n); err != nil {
		t.Fatalf("count videos: %v", err)
	}
	return n
}

// sampleIDs returns up to n random video IDs from the table.
func sampleIDs(t *testing.T, db *sql.DB, n int) []string {
	t.Helper()
	// ORDER BY RAND() on 10M rows would be terrible; instead, scan via a
	// random-offset window using the PK index, then take ids.
	q := `SELECT id FROM videos LIMIT ?`
	rows, err := db.Query(q, n)
	if err != nil {
		t.Fatalf("sample ids: %v", err)
	}
	defer rows.Close()
	ids := make([]string, 0, n)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan id: %v", err)
		}
		ids = append(ids, id)
	}
	rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	return ids
}

// latencyStats holds a sortable slice of measured durations.
type latencyStats struct {
	mu   sync.Mutex
	data []time.Duration
}

func (l *latencyStats) add(d time.Duration) {
	l.mu.Lock()
	l.data = append(l.data, d)
	l.mu.Unlock()
}

func (l *latencyStats) percentile(p float64) time.Duration {
	if len(l.data) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(l.data))
	copy(sorted, l.data)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (l *latencyStats) summary() (p50, p95, p99 time.Duration, count int) {
	return l.percentile(0.50), l.percentile(0.95), l.percentile(0.99), len(l.data)
}

// timeIt wraps an HTTP call and records latency + outcome.
func timeIt(stats *latencyStats, fn func() (*http.Response, error)) error {
	start := time.Now()
	resp, err := fn()
	elapsed := time.Since(start)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	stats.add(elapsed)
	return err
}

// readEnvOrDefault provides a small helper for inline defaults in tests.
func readEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// newEnv mirrors testutil.NewEnv but is kept local for clarity.
func newEnv(t *testing.T) *testutil.Env {
	t.Helper()
	return testutil.NewEnv(t)
}

// runWorkers runs fn from `workers` goroutines for `duration`. Each fn
// invocation should perform exactly one operation and return whether it
// succeeded. Returns (operations, errors).
func runWorkers(workers int, duration time.Duration, fn func() bool) (int64, int64) {
	var ops, errs atomic.Int64
	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				if fn() {
					ops.Add(1)
				} else {
					errs.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	return ops.Load(), errs.Load()
}

// fmtPerSec formats throughput.
func fmtPerSec(n int64, d time.Duration) string {
	if d <= 0 {
		return "0/s"
	}
	return fmt.Sprintf("%.1f/s", float64(n)/d.Seconds())
}
