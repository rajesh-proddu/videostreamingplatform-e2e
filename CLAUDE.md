# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

End-to-end test suite for the video streaming platform. Tests run against live services (metadata-service, data-service, Kafka) — no mocks. The repo contains HTTP/Kafka clients and three test suites: happy path, resiliency, and scale.

## Commands

```bash
# Run all tests (requires services running)
go test ./tests/... -v

# Run a specific suite
go test ./tests/happypath/... -v
go test ./tests/resiliency/... -v
go test ./tests/scale/... -v
go test ./tests/analytics/... -v          # ES + Iceberg consumers
go test ./tests/recommendations/... -v    # FastAPI + LangGraph agent

# Run a single test
go test ./tests/happypath/... -v -run TestVideoCRUDLifecycle

# Skip long-running tests
go test ./tests/... -v -short

# Build/vet check
go build ./...
go vet ./...
```

## Configuration

All config is via environment variables with sensible defaults for local Kind cluster with NodePort services:

| Variable | Default | Description |
|---|---|---|
| `METADATA_SERVICE_URL` | `http://localhost:8080` | Metadata service base URL |
| `DATA_SERVICE_URL` | `http://localhost:8081` | Data service base URL |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker addresses |
| `HTTP_TIMEOUT` | `30s` | General HTTP timeout |
| `UPLOAD_TIMEOUT` | `120s` | Upload operation timeout |
| `EVENT_WAIT_TIME` | `5s` | How long to wait for Kafka events |
| `BULK_COUNT` | `50` | Number of items for bulk scale tests |
| `CONCURRENT_USERS` | `10` | Concurrency level for scale tests |
| `ELASTICSEARCH_URL` | `http://localhost:9200` | ES base URL (analytics suite) |
| `ES_VIDEO_INDEX` | `videos` | ES index for the video catalog |
| `RECOMMENDATION_SERVICE_URL` | `http://localhost:8000` | Recommendations FastAPI base URL |
| `PGVECTOR_DSN` | `postgres://recouser:recopass@localhost:5432/recommendations?sslmode=disable` | pgvector DSN — used to seed `watch_history` |
| `S3_ENDPOINT` | `http://localhost:4566` | S3 endpoint (LocalStack for Iceberg checks) |
| `ICEBERG_WAREHOUSE_BUCKET` | `iceberg-warehouse` | S3 bucket holding Iceberg data files |
| `ICEBERG_TABLE_PREFIX` | `analytics.db/watch_history/data` | Object key prefix for parquet data files |
| `ANALYTICS_WAIT_TIME` | `30s` | Polling timeout for ES indexing + Iceberg flushes |

## Architecture

- `config/` — env-based config loaded once per test
- `client/` — typed clients: `MetadataClient`, `DataClient`, `KafkaConsumer`, `KafkaProducer` (raw event injection), `ESClient` (ES doc lookup + polling), `RecommendClient` + `RecommendViaProxy`, `PgVectorClient` (seed `watch_history` for agent tests), `IcebergS3Client` (count parquet files under the table data prefix)
- `testutil/` — `Env` bundles all clients; `RequireES` / `RequireRecommendations` skip when targets unreachable; `PgVector(t)` and `IcebergS3(t)` lazy-init + cleanup; `SeedAndCleanupHistory` auto-deletes seeded rows; `CreateTestVideo` auto-cleans
- `tests/happypath/` — golden path: health, CRUD, pagination, upload lifecycle, download integrity, video kafka events, watch events (now driven by `GET /videos/{id}/download` — the only path that produces them)
- `tests/resiliency/` — error paths: invalid requests, 404s, rate limiting, upload edge cases, concurrent operations
- `tests/scale/` — load: bulk create/delete, concurrent uploads/downloads, large file integrity, pagination under load
- `tests/analytics/` — `kafka-es-consumer` + `watch-history-consumer` end-to-end: video CRUD → ES doc presence; raw Kafka watch event → Iceberg parquet append; version/ID drop rules; idle-flush; envelope shape
- `tests/recommendations/` — FastAPI contract (limit bounds, missing user_id), proxy-via-metadataservice (400 on missing user, current `limit` hardcoding), agent behaviour (watch filter on/off based on `query`, trending surfacing, search hit, score threshold), latency budget, and a cross-pipeline test that requires the full `metadataservice → Kafka → ES → recs.retrieve` chain

## Key patterns

- Every test creates its own data and cleans up via `t.Cleanup` — tests are independent and parallelizable
- Kafka event tests use `t.Skip` when events aren't received within timeout (avoids flaky failures)
- Scale test parameters are configurable via env vars so CI can dial them up/down
- `testing.Short()` skips the 10MB upload test, the ES create-burst test, the Iceberg idle-flush test, and the recommendations latency budget test
- The analytics + recommendations suites are dependency-heavy by design: ES, pgvector, LocalStack S3, Kafka, FastAPI must all be reachable. Each suite uses `RequireX(t)` skips so partial environments still run a useful subset
- LLM ranking is non-deterministic. Recommendation tests assert presence/exclusion in the result set, score bounds, and limit bounds — never order or `reason` text
- Watch events on Kafka are produced **only** by `dataservice` when `GET /videos/{id}/download` is called without a `Range` header. There is no metadataservice `/watch-events` endpoint — tests that need watch events upload a video and then download it
