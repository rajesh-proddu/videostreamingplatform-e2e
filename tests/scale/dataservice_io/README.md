# `tests/scale/dataservice_io`

Throughput-oriented scale tests for the data-service upload + download paths.
These tests run against live services (no mocks) and are intended for ad-hoc
performance measurement, not regression gates.

## Test inventory

| File | Test | What it measures |
|---|---|---|
| `upload_test.go` | `TestUpload_Sanity` | 64 KiB upload — smoke check, runs in `-short` |
| | `TestUpload_SingleStream_Throughput_1GB` | Single-stream 1 GiB upload (chunked, 5 MiB chunks). Logs MiB/s, chunks/s, p50/p95/p99 chunk PUT latency, init + complete latency |
| | `TestUpload_Concurrent_8x100MB` | 8 parallel uploaders × 100 MiB. Aggregate MiB/s + chunk latency percentiles |
| | `TestUpload_VariedChunkSizes` | Same 100 MiB payload at chunk sizes 1/4/8/16 MiB. Logs best chunk size |
| | `TestUpload_ManySmallVideos` | 200 × 1 MiB videos from 16 workers. uploads/sec + MiB/s |
| `download_test.go` | `TestDownload_Sanity_Parallel` | 4 concurrent downloads of 1 MiB — smoke check |
| | `TestDownload_Single_VariedSize` | Single full GETs at 1/10/100/1024 MiB (1024 skipped in `-short`) |
| | `TestDownload_Concurrent_16x10MB` | 16 parallel readers of the same 10 MiB video. Aggregate MiB/s + latency percentiles |
| | `TestDownload_RangeRequests` | 4 random `Range:` requests on a 100 MiB video. Asserts 206 + correct byte count |
| | `TestDownload_CDNProxy_vs_Direct` | Same content via `CDN_PROXY_URL` vs direct data-service. Compares full GET throughput; asserts CDN forwards `Range` (206) |
| `mixed_test.go` | `TestMixed_UploadDownload_4x16` | 4 uploaders + 16 downloaders running for `SCALE_DURATION` (default 2m). Aggregate MiB/s each way + error rate |
| `grpc_test.go` | `TestGRPC_StreamingPaths` | Skipped — see "gRPC" section below |
| `resilience_test.go` | `TestUpload_AbortedSessions_DoNotLeak` | 50 uploads (25 abandoned mid-flight) followed by 50 more; verifies orphans don't break later uploads |
| | `TestUpload_OversizedChunk_Rejected` | Sends a chunk > declared ChunkSize; logs observed behavior (400/413/422 expected, but tolerates a permissive server) |

## How to run

### Local (default — LocalStack S3, single-node Docker stack)

```bash
cd /home/rajesh/go_workspace/videostreamingplatform-e2e

# -short skips the 1 GiB upload + concurrent + sweep + mixed I/O tests
go test ./tests/scale/dataservice_io/... -v -short

# Full run (long — generates ~3 GiB through LocalStack)
go test ./tests/scale/dataservice_io/... -v -timeout 60m
```

### AWS (EKS + real S3)

Same command, but configured via env vars to point at the cluster. Once the
endpoints are exposed via NodePort / LoadBalancer:

```bash
export DATA_SERVICE_URL=http://<lb>:8081
export METADATA_SERVICE_URL=http://<lb>:8080
export CDN_PROXY_URL=http://<lb>:8090

# Dial up the workload for the larger cluster
export SCALE_UPLOAD_SIZE_MB=1024     # full 1 GiB single-stream
export SCALE_UPLOAD_WORKERS=16
export SCALE_DOWNLOAD_GIB=1          # adds 1 GiB to the download corpus
export SCALE_DURATION=10m

go test ./tests/scale/dataservice_io/... -v -timeout 2h
```

## Env-var knobs

| Variable | Default | Used by |
|---|---|---|
| `SCALE_UPLOAD_SIZE_MB` | `256` (set `1024` on AWS) | `TestUpload_SingleStream_Throughput_1GB` |
| `SCALE_DOWNLOAD_GIB` | `0` (set `1` on AWS) | adds 1024 MiB to `TestDownload_Single_VariedSize` corpus |
| `SCALE_UPLOAD_WORKERS` | `8` | `TestUpload_Concurrent_8x100MB` |
| `SCALE_UPLOAD_SIZE_MB_EACH` | `100` | `TestUpload_Concurrent_8x100MB` |
| `SCALE_SMALL_COUNT` | `200` | `TestUpload_ManySmallVideos` |
| `SCALE_SMALL_WORKERS` | `16` | `TestUpload_ManySmallVideos` |
| `SCALE_DL_WORKERS` | `16` | `TestDownload_Concurrent_16x10MB` |
| `SCALE_MIXED_UPLOADERS` | `4` | `TestMixed_UploadDownload_4x16` |
| `SCALE_MIXED_DOWNLOADERS` | `16` | `TestMixed_UploadDownload_4x16` |
| `SCALE_DURATION` | `2m` | `TestMixed_UploadDownload_4x16` |

Inherited from `config/config.go`: `DATA_SERVICE_URL`, `METADATA_SERVICE_URL`,
`CDN_PROXY_URL`, `UPLOAD_TIMEOUT`, etc. The throughput tests build their own
`DataClient` instances with longer per-request timeouts (10–30 min depending
on test) rather than relying on `UPLOAD_TIMEOUT`.

## Memory discipline

The 1 GiB upload does **not** allocate the payload up front. We allocate a
single chunk-sized buffer (default 5 MiB) from `crypto/rand` and reuse it for
every chunk PUT, so the resident set stays small regardless of `TotalSize`.

Likewise, downloads read into a 1 MiB scratch buffer in a loop and discard
the bytes — no `io.ReadAll`, no full-file buffers in the test process.

## gRPC

`TestGRPC_StreamingPaths` is intentionally skipped. The data-service ships a
gRPC `DataService` (port `50051`), but:

1. Its generated Go code lives in
   `github.com/yourusername/videostreamingplatform/dataservice/pb`, a
   different Go module. Importing it would require a `replace` directive in
   this module's `go.mod` plus adding `google.golang.org/grpc` +
   `google.golang.org/protobuf` deps.
2. The `go_package` option in `dataservice/proto/dataservice.proto` is
   `videostreamingplatform/dataservice/pb`, which isn't a valid import path
   (no domain prefix), so a vanilla `go get` won't work either.
3. The proto defines only **unary** RPCs (`InitiateUpload`, `UploadChunk`,
   `GetUploadProgress`, `CompleteUpload`, `ListUploads`) — there is no
   server-streaming download to compare against an HTTP byte stream.

If a streaming download RPC is added later, wire it here.

## Memory caution — local 1 GiB uploads

On a typical dev machine, the data-service container hits an OOM (exit 137)
when finalizing a 1 GiB chunked upload — `CompleteUpload` issues an S3
`CompleteMultipartUpload` against LocalStack, which buffers the ~200 part
manifest plus assembles the final object in memory. The local container
has no explicit memory limit, but the Go process + LocalStack together
exhaust available RAM.

Defaults are conservative on local:

- `TestUpload_SingleStream_Throughput_1GB` defaults to **256 MiB**
  (`SCALE_UPLOAD_SIZE_MB`). Set `SCALE_UPLOAD_SIZE_MB=1024` for a true 1 GiB
  run, but expect the local container to die if it doesn't have enough
  headroom.
- `TestDownload_Single_VariedSize` sweeps **1 / 10 / 100 MiB** by default
  (no 1024 MiB). Set `SCALE_DOWNLOAD_GIB=1` to add the 1024 MiB case.

Each test runs `requireDataServiceUp(t, env)` at start — if the container
crashed in a prior test, the rest of the suite skips cleanly instead of
flooding the log with `connection refused`.

## Concurrency caution — shared infrastructure

These tests **share LocalStack S3** (`video-platform-storage` bucket at
`http://127.0.0.1:4566`) with:

- The recommendations agent's Iceberg writes (separate bucket
  `iceberg-warehouse`, but same LocalStack process — large concurrent uploads
  contend for the same Python S3-mock event loop).
- Any other scale tests running at the same time (Kafka / MySQL / ES tests
  in sibling agents).

If you're running all four scale suites concurrently, expect:
- **MiB/s numbers will be lower** than running this suite in isolation.
- Occasional 5xx from LocalStack under sustained parallel load — the tests
  tolerate up to 50% errors on the concurrent tests before failing.

For clean numbers, run this suite alone (`go test ./tests/scale/dataservice_io/...`).

## Output conventions

- Every result line is logged with `t.Logf` and prefixed `RESULT` so it's
  greppable from the test log:
  ```
  grep RESULT /tmp/scale_dataservice.log
  ```
- Latency percentiles always report `p50 / p95 / p99`.
- Throughput is `MiB/s` (1 MiB = 1024 × 1024 bytes), not decimal MB/s.

## Cleanup

- Metadata videos are auto-cleaned via `t.Cleanup` in `CreateTestVideo`.
- Upload payloads in S3 (LocalStack) are **not** cleaned up — by design,
  download tests reuse them within the same `*testing.T`. They are
  ephemeral with the LocalStack container.
