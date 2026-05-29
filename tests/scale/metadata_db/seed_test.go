package metadatadb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	mrand "math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/config"
)

// finalCorpusCount is captured at the end of seed so TestSeedCorpus_Verify can
// report it without re-querying.
var finalCorpusCount int64

// TestMain seeds the videos corpus before any tests run. This guarantees the
// corpus is present regardless of test-name ordering (Go orders by file +
// source position, so a plain `TestSeed...` could otherwise run after the
// read tests). If seed fails the suite still proceeds — individual tests
// gate themselves on the actual row count and skip when too small.
func TestMain(m *testing.M) {
	if err := seedCorpus(); err != nil {
		log.Printf("WARN: seedCorpus failed: %v (continuing — tests will gate on actual row count)", err)
	}
	os.Exit(m.Run())
}

// TestSeedCorpus_Verify is a no-op reporter that surfaces the final row count
// in the test log. The actual seeding happens in TestMain so it runs before
// every other test in this package.
func TestSeedCorpus_Verify(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	have := rowCount(t, db)
	target := int64(config.Load().ScaleCorpus)
	t.Logf("post-seed corpus: %d rows (target %d)", have, target)
	if have < target {
		t.Logf("WARNING: corpus below target — deep-offset tests may have shorter offset lists")
	}
}

// seedCorpus does the actual top-up. Returns an error only on a fatal setup
// failure; per-batch errors are logged but do not abort the seed.
func seedCorpus() error {
	cfg := config.Load()
	target := int64(cfg.ScaleCorpus)
	if target <= 0 {
		log.Printf("seedCorpus: SCALE_CORPUS<=0, skipping")
		return nil
	}

	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		return fmt.Errorf("sql.Open: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(5 * time.Minute)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("ping mysql: %w", err)
	}

	have, err := dbRowCount(db)
	if err != nil {
		return fmt.Errorf("count videos: %w", err)
	}
	log.Printf("seedCorpus: current=%d target=%d", have, target)
	if have >= target {
		log.Printf("seedCorpus: corpus already >= target, no seeding required")
		atomic.StoreInt64(&finalCorpusCount, have)
		return nil
	}

	gap := target - have
	log.Printf("seedCorpus: seeding %d rows (%d existing, %d target)", gap, have, target)

	// Tunables — keep batch reasonable for MySQL's max_allowed_packet (default 64MB).
	const (
		batchRows = 5000
		workers   = 8
	)
	totalBatches := (gap + batchRows - 1) / batchRows

	// Spread created_at across the last year for realistic time-based queries.
	yearStart := time.Now().Add(-365 * 24 * time.Hour).Unix()
	yearEnd := time.Now().Unix()

	var batchIdx atomic.Int64
	var inserted atomic.Int64
	var failures atomic.Int64

	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				ins := inserted.Load()
				elapsed := time.Since(start).Seconds()
				rate := float64(ins) / elapsed
				if rate > 0 {
					etaSec := float64(gap-ins) / rate
					log.Printf("seed progress: %d/%d rows (%.0f rows/s, ETA %s)", ins, gap, rate, time.Duration(etaSec*float64(time.Second)).Round(time.Second))
				} else {
					log.Printf("seed progress: %d/%d rows", ins, gap)
				}
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(workers)
	startSeed := time.Now()
	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			rng := mrand.New(mrand.NewSource(time.Now().UnixNano() + int64(workerID)))
			for {
				b := batchIdx.Add(1) - 1
				if b >= totalBatches {
					return
				}
				rows := batchRows
				remaining := gap - b*batchRows
				if remaining < int64(rows) {
					rows = int(remaining)
				}
				if rows <= 0 {
					return
				}
				if err := insertBatch(ctx, db, rows, rng, yearStart, yearEnd); err != nil {
					failures.Add(1)
					log.Printf("seed worker %d batch %d failed: %v", workerID, b, err)
					continue
				}
				inserted.Add(int64(rows))
			}
		}(w)
	}
	wg.Wait()
	close(progressDone)

	elapsed := time.Since(startSeed)
	final, _ := dbRowCount(db)
	atomic.StoreInt64(&finalCorpusCount, final)
	log.Printf("seed complete: inserted=%d failures=%d duration=%s final_count=%d (%.0f rows/s)",
		inserted.Load(), failures.Load(), elapsed.Round(time.Second), final,
		float64(inserted.Load())/elapsed.Seconds())
	return nil
}

// dbRowCount is the *sql.DB version of rowCount (no *testing.T available in TestMain).
func dbRowCount(db *sql.DB) (int64, error) {
	var n int64
	err := db.QueryRow("SELECT COUNT(*) FROM videos").Scan(&n)
	return n, err
}

// insertBatch builds and executes one multi-row INSERT.
func insertBatch(ctx context.Context, db *sql.DB, rows int, rng *mrand.Rand, tsLo, tsHi int64) error {
	if rows <= 0 {
		return nil
	}
	var sb strings.Builder
	sb.Grow(rows * 80)
	sb.WriteString("INSERT INTO videos (id, title, description, duration, size_bytes, upload_status, created_at) VALUES ")
	args := make([]any, 0, rows*6)
	for i := 0; i < rows; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?,?,?)")
		id := newUUID(rng)
		title := fmt.Sprintf("%s%s-%d", seedTitlePrefix, randomToken(rng, 6), rng.Intn(1<<30))
		desc := "scale-seed"
		duration := rng.Intn(7200) + 30
		sizeBytes := int64(rng.Intn(1<<30)) + 1024
		status := "COMPLETED"
		// Spread created_at uniformly over the last year.
		ts := tsLo + rng.Int63n(tsHi-tsLo)
		createdAt := time.Unix(ts, 0).UTC().Format("2006-01-02 15:04:05")
		args = append(args, id, title, desc, duration, sizeBytes, status, createdAt)
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, sb.String(), args...)
	return err
}

// newUUID generates a UUID v4-ish string using the test's RNG (good enough
// for the corpus — we don't need crypto strength here).
func newUUID(rng *mrand.Rand) string {
	var b [16]byte
	for i := range b {
		b[i] = byte(rng.Intn(256))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// randomToken returns a short hex token. Uses crypto/rand for short uniqueness needs.
func randomToken(_ *mrand.Rand, n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
