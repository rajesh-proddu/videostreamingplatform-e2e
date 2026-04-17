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

## Architecture

- `config/` — env-based config loaded once per test
- `client/` — typed HTTP clients: `MetadataClient` (CRUD + list + health), `DataClient` (upload/download), `KafkaConsumer` (event reading)
- `testutil/` — `Env` struct bundles all clients; `CreateTestVideo` auto-registers cleanup; `RandomBytes`/`UniqueTitle` for test isolation
- `tests/happypath/` — golden path: health, CRUD, pagination, upload lifecycle, download integrity, Kafka events, watch events
- `tests/resiliency/` — error paths: invalid requests, 404s, rate limiting, upload edge cases, concurrent operations
- `tests/scale/` — load: bulk create/delete, concurrent uploads/downloads, large file integrity, pagination under load

## Key patterns

- Every test creates its own data and cleans up via `t.Cleanup` — tests are independent and parallelizable
- Kafka event tests use `t.Skip` when events aren't received within timeout (avoids flaky failures)
- Scale test parameters are configurable via env vars so CI can dial them up/down
- `testing.Short()` skips the 10MB upload test
