# Platform event-pipeline scale tests

Scale tests for the **platform's own** Kafka producer/consumer code — not the
broker.

The sibling suite [`../kafka_throughput`](../kafka_throughput) builds raw
`kafka-go` writers/readers and benchmarks the broker ("can Kafka sustain
100k?"). This suite answers a different question: **"does my produce→consume
code stay correct under sustained load?"** It drives the real HTTP endpoints
that make the platform publish events and asserts the real Python consumers
drain them.

| Pipeline | Producer under test | Trigger | Consumer under test | Sink |
|---|---|---|---|---|
| video→ES | `metadataservice` `kafka.Producer` | `POST /videos` | `kafka-es-consumer` | Elasticsearch |
| watch→Iceberg | `dataservice` `kafka.Producer` | `GET /videos/{id}/download` | `watch-history-consumer` | Iceberg (parquet on S3) |

## What is asserted

The headline is **correctness, not a rate**:

- **video→ES** — every created video must be indexed (`indexed == produced`),
  counted exactly by document id via the ES `_count` API (analyzer-independent).
  A shortfall after the drain grace is **real loss → `Fatalf`**.
- **watch→Iceberg** — parquet must grow under load and then **stabilize** within
  the grace window (consumer caught up). Weaker by nature: watch-events are
  append-only and the consumer flushes in idle batches, so exact no-loss can't
  be checked from S3 alone (would need an Athena row count). We assert the
  consumer is alive and drains, and report the rate.

Both tests run a **canary** first (one create / one download) that `Fatalf`s
fast if the consumer isn't live — so a dead consumer fails clearly instead of
silently going green like a `Skip`.

The achieved rate is **reported**, gated only when `SCALE_PIPELINE_MIN_RPS` is
set. The local ceiling is metadataservice+MySQL (produce) and the single-replica
Python consumer (consume) — **not** Kafka — so this won't approach 100k locally,
and that's expected. Use `kafka_throughput` for the broker ceiling; use this for
code correctness under load.

## Environment knobs

| Variable | Default | Purpose |
|---|---|---|
| `SCALE_PIPELINE_VIDEOS` | `2000` | video→ES: number of videos to create |
| `SCALE_PIPELINE_WATCHES` | `300` | watch→Iceberg: number of downloads to drive |
| `SCALE_PIPELINE_VIDEO_POOL` | `20` | watch→Iceberg: uploaded videos to download against |
| `SCALE_PIPELINE_WORKERS` | `32` (es) / `8` (watch) | Concurrent driver goroutines |
| `SCALE_PIPELINE_DRAIN_GRACE` | `60s` / `90s` | How long to wait for the consumer to catch up |
| `SCALE_PIPELINE_MIN_RPS` | `0` | Hard floor on produce + e2e consume rate. `0` = report-only |

Standard harness env (`METADATA_SERVICE_URL`, `DATA_SERVICE_URL`, `KAFKA_BROKERS`,
`ELASTICSEARCH_URL`, `S3_ENDPOINT`, …) is inherited from `config/`.
`-short` shrinks volumes for fast iteration.

## Running

```bash
cd /home/rajesh/go_workspace/videostreamingplatform-e2e

# video→ES only (the runnable-everywhere pipeline)
go test ./tests/scale/event_pipeline/... -v -run TestPipeline_VideoToES_Sustained -timeout 20m

# both, fast smoke
go test ./tests/scale/event_pipeline/... -v -short

# enforce a floor (real multi-broker / multi-replica hardware)
SCALE_PIPELINE_MIN_RPS=5000 SCALE_PIPELINE_VIDEOS=50000 \
  go test ./tests/scale/event_pipeline/... -v -run TestPipeline_VideoToES_Sustained -timeout 30m
```

## Stack requirements / caveats

- **video→ES** needs: infra (Kafka) + metadataservice + Elasticsearch +
  `kafka-es-consumer` running. Runnable on the local Kind/compose stack.
- **watch→Iceberg** additionally needs the `watch-history-consumer` committing to
  an Iceberg table via the **Glue catalog**. Locally that means LocalStack-Glue,
  which is a **LocalStack Pro feature** — without it the consumer can't commit
  parquet and the canary will `Fatalf` ("pipeline not live"). On AWS (real Glue)
  it runs. This is intentional: the test refuses to pass against a pipeline that
  isn't actually flushing.
- The shared `MetadataClient`/`DataClient` use the default HTTP transport
  (`MaxIdleConnsPerHost=2`), so produce-side concurrency is connection-churn
  bound — fine for correctness, but don't read the local produce rate as a
  service ceiling.
