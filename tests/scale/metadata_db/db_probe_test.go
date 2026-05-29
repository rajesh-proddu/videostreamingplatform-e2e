package metadatadb

import (
	"database/sql"
	"testing"
)

// TestDB_Probe_PaginationPlan asserts the deep-OFFSET pagination query uses
// an index (not a full table scan). If MySQL falls back to ALL with
// filesort, fails with a clear remediation hint to add a compound
// (created_at DESC, id DESC) index.
//
// Caveat: on a very small table the optimizer often picks ALL anyway
// because a table scan is cheaper than index traversal. The assertion is
// only meaningful once the corpus is large enough (>100k rows); below
// that we just report the plan and skip the assertion.
func TestDB_Probe_PaginationPlan(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	have := rowCount(t, db)
	t.Logf("corpus size for EXPLAIN: %d rows", have)

	const offset = 9_999_000
	// Mirror the production ListVideos query (deferred join): a covering-index
	// subquery picks the page's PKs in created_at/id order, then we join back
	// for full rows. This keeps deep-OFFSET pagination on idx_created_at_id
	// instead of degrading to a full-table scan + filesort, which the optimizer
	// picks when all columns (incl. TEXT) are selected directly at depth.
	query := `EXPLAIN SELECT v.id, v.title, v.description, v.duration, v.size_bytes, v.upload_status, v.created_at, v.updated_at
              FROM videos v
              JOIN (SELECT id FROM videos ORDER BY created_at DESC, id DESC LIMIT 20 OFFSET ?) k ON v.id = k.id
              ORDER BY v.created_at DESC, v.id DESC`

	rs, err := db.Query(query, offset)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rs.Close()

	var usesIndex, bigScan bool
	for rs.Next() {
		var (
			id, selectType                 sql.NullString
			table, partitions              sql.NullString
			typ, possibleKeys, key, keyLen sql.NullString
			ref                            sql.NullString
			nrows                          sql.NullInt64
			filtered                       sql.NullFloat64
			extra                          sql.NullString
		)
		if err := rs.Scan(&id, &selectType, &table, &partitions, &typ, &possibleKeys, &key, &keyLen, &ref, &nrows, &filtered, &extra); err != nil {
			t.Fatalf("EXPLAIN scan: %v", err)
		}
		t.Logf("EXPLAIN: table=%s type=%s key=%s rows=%d extra=%q",
			table.String, typ.String, key.String, nrows.Int64, extra.String)
		if key.String == "idx_created_at_id" {
			usesIndex = true
		}
		// A full-table scan over the corpus is the pathology. Ignore derived /
		// materialized rows (table name like "<derived2>"): their row estimate is
		// inherited from the inner scan, but execution only materializes the
		// LIMITed page. Only a real base-table type=ALL over the corpus is bad.
		if len(table.String) > 0 && table.String[0] != '<' && typ.String == "ALL" && nrows.Int64 > 100_000 {
			bigScan = true
		}
	}
	if err := rs.Err(); err != nil {
		t.Fatalf("EXPLAIN rows: %v", err)
	}

	// Below 100k rows the optimizer's choice of ALL is not pathological.
	if have < 100_000 {
		t.Logf("corpus < 100k rows — skipping plan assertion (small-table optimization)")
		return
	}
	if bigScan {
		t.Errorf("deep-OFFSET pagination still does a full-table scan (type=ALL over >100k rows) — deferred join + `idx_created_at_id` not effective")
	}
	if !usesIndex {
		t.Errorf("deep-OFFSET pagination did not use idx_created_at_id — compound index missing or not chosen")
	}
}

// TestDB_Probe_TableSize reports the videos table size from
// information_schema, useful for capacity planning and as a sanity check
// that the seed actually grew the table.
func TestDB_Probe_TableSize(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	const q = `SELECT table_rows,
                      data_length, index_length,
                      data_length + index_length AS total_bytes
               FROM information_schema.TABLES
               WHERE table_schema = DATABASE() AND table_name = 'videos'`
	var (
		tableRows, dataLen, idxLen, totalBytes sql.NullInt64
	)
	if err := db.QueryRow(q).Scan(&tableRows, &dataLen, &idxLen, &totalBytes); err != nil {
		t.Fatalf("information_schema.TABLES scan: %v", err)
	}
	mib := func(b int64) float64 { return float64(b) / 1024.0 / 1024.0 }
	t.Logf("videos table: approx_rows=%d data=%.1f MiB index=%.1f MiB total=%.1f MiB",
		tableRows.Int64, mib(dataLen.Int64), mib(idxLen.Int64), mib(totalBytes.Int64))
}
