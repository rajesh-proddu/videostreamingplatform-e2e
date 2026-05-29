# Metadata DB Scale Tests

End-to-end scale tests focused on the **metadata-service MySQL backend**.
All HTTP traffic goes through `metadataservice` so we exercise the full
stack (HTTP -> service -> Redis cache -> MySQL). The direct DB connection
is used only for (a) seeding the corpus, (b) sampling IDs cheaply, and
(c) the `EXPLAIN` plan probe.

## Tests

| Test | What it measures |
|------|------------------|
| `TestSeedCorpus` | Ensures `videos` has at least `SCALE_CORPUS` rows (default 1M; set to `10000000` to target the original 10M goal). Idempotent — re-running does not double the table. Inserts via parallel multi-row `INSERT` (5k rows per stmt, 8 workers). |
| `TestList_DeepPagination_Latency` | `GET /videos?limit=20&offset=N` at N = 0, 100, 1k, 10k, 100k, 1M, 5M, 9.9M. Reports p50/p95/p99. Exposes the OFFSET-scan antipattern. |
| `TestPointGet_ByID_Throughput` | Random `GET /videos/{id}` at 16/32/64 workers. Reports QPS and p95. |
| `TestList_LimitVariation` | Fixed offset=0, limit varies 1/10/100/1000. Linear scaling expected. |
| `TestList_OrderByConsistency` | At offset 1M (or `have/2`), fetches two adjacent 100-row pages and asserts no duplicate IDs. Verifies the `ORDER BY created_at DESC, id DESC` tiebreaker. |
| `TestInsert_Throughput_HTTP` | N workers (8/16/32) `POST /videos` for `SCALE_DURATION`. Cleanup at end. |
| `TestUpdate_Throughput` | Pre-sampled 10k IDs, N workers `PUT /videos/{id}` for `SCALE_DURATION`. |
| `TestDelete_Throughput` | Creates 10k throwaway videos, then deletes concurrently at 16/32 workers. |
| `TestMixedWorkload` | 70/20/10 read/write/delete, 32 workers, 2 minutes (capped by `SCALE_DURATION`). |
| `TestDB_Probe_PaginationPlan` | `EXPLAIN` the deep-offset pagination query; asserts a key is used and `Extra` is not `Using filesort` (only enforced for corpora >= 100k rows — small-table optimization picks `ALL` regardless). Fails with a clear hint that a compound `(created_at DESC, id DESC)` index is needed. |
| `TestDB_Probe_TableSize` | Reads `information_schema.TABLES` and reports approx_rows, data, index, and total bytes. |

## Environment

| Variable | Default | Purpose |
|---|---|---|
| `METADATA_SERVICE_URL` | `http://127.0.0.1:8080` | HTTP target for all read/write tests. |
| `MYSQL_DSN` | `videouser:videopass@tcp(127.0.0.1:3306)/videoplatform?parseTime=true` | Direct DB connection for seeding + EXPLAIN. |
| `SCALE_CORPUS` | `1000000` | Target row count for `TestSeedCorpus`. Set to `10000000` for the original 10M goal. |
| `SCALE_DURATION` | `60s` | Duration of each insert/update workload phase. |
| `SCALE_WORKERS` | `16` | Default worker count for the mixed workload (the read/insert/update/delete tests sweep through their own fixed worker lists for comparability). |
| `HTTP_TIMEOUT` | `30s` | Per-request HTTP timeout. Override if running against a slow remote (`HTTP_TIMEOUT=60s`). |

## Running

### Local

```bash
cd videostreamingplatform-e2e
# 1) Seed + run everything (90m budget covers ~1M rows on local docker MySQL)
go test ./tests/scale/metadata_db/... -v -timeout 90m

# 2) Just the deep-pagination test (assumes corpus already seeded)
go test ./tests/scale/metadata_db/... -v -run TestList_DeepPagination_Latency -timeout 30m

# 3) Skip heavy tests
go test ./tests/scale/metadata_db/... -v -short
```

### AWS (EKS)

```bash
METADATA_SERVICE_URL=https://metadata.<env>.aws.example.com \
MYSQL_DSN='videouser:<pw>@tcp(<rds-host>:3306)/videoplatform?parseTime=true&tls=true' \
SCALE_CORPUS=10000000 \
SCALE_DURATION=120s \
SCALE_WORKERS=64 \
HTTP_TIMEOUT=60s \
go test ./tests/scale/metadata_db/... -v -timeout 120m
```

## Caveats

- **Redis cache on read tests.** `metadataservice` caches `videos:list:{limit}:{offset}` and `video:{id}` in Redis. The list-latency tests scatter offsets by +/- a small jitter (each request a distinct cache key) so we measure DB work, not cache hits. Point-get uses 10k random IDs to keep the cache cold-ish across a 15s run.
- **Corpus floor.** The user-stated target is 10M rows. To keep this suite inside the 90 minute `-timeout` budget on a local docker MySQL (which seeds ~25k-50k rows/s for this schema), the default `SCALE_CORPUS=1_000_000`. The deep-pagination test only includes offsets the corpus actually supports — at 1M the bucket list collapses to `[0, 100, 1k, 10k, 100k, 1M]`. Set `SCALE_CORPUS=10000000` and `-timeout 180m` to target 10M.
- **EXPLAIN assertion is corpus-gated.** Below 100k rows MySQL legitimately prefers a full scan; the probe logs the plan but does not fail.
- **Sibling agents.** Three other agents may be running concurrent scale tests against the same local stack (Kafka, ES, recommendations, data-service). They do not write to `videoplatform.videos` directly, but heavy traffic at the metadata-service tier could cause connection-pool / rate-limit interference. If results look noisy, run the metadata_db suite in isolation.
- **Cleanup.** Insert/delete tests clean up the rows they create. The seed-corpus rows (titles starting with `seed-corpus-`) are **left in place** — they are the corpus. To wipe them: `DELETE FROM videos WHERE title LIKE 'seed-corpus-%' LIMIT 10000;` (loop until 0 rows affected).
