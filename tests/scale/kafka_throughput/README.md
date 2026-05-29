# Kafka throughput & lag scale tests

Black-box Kafka tests that measure producer rate, consumer drain throughput,
end-to-end latency, lag behaviour under load, and offset-commit recovery.
The suite runs against the same Kafka broker as the rest of the platform but
uses dedicated `scaletest-events-*` topics so it never reads or writes the
real `video-events` / `watch-events` topics.

## Topics

`TestMain` creates these topics if they don't exist (idempotent):

| Topic | Partitions | Purpose |
|---|---|---|
| `scaletest-events-low` | 3 | Mirrors `video-events`; single-thread & e2e latency tests |
| `scaletest-events-high` | 12 | Mirrors fan-out scenarios; partition parallelism tests |

## Test files

| File | Tests |
|---|---|
| `producer_test.go` | `TestProducer_SingleThread_Rate`, `TestProducer_Concurrent_8Threads`, `TestProducer_MessageSizeSweep`, `TestProducer_BatchedVsUnbatched` |
| `consumer_test.go` | `TestConsumer_SingleConsumer_Throughput`, `TestConsumer_GroupOfN_Parallelism`, `TestConsumer_RebalanceLatency` |
| `lag_test.go` | `TestLag_UnderBurst`, `TestLag_ConsumerCold_BootstrapTime`, `TestLag_EndToEnd_Latency` |
| `resilience_test.go` | `TestProducer_BrokerSlow`, `TestConsumer_OffsetCommit_RecoveryAfterCrash` |

All metrics are emitted via `t.Logf` in tabular form so they are easy to grep
out of `go test -v` output.

## Environment knobs

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BROKERS` | `127.0.0.1:9092` | Comma-separated brokers |
| `KAFKA_TLS` | `false` | Set to `true` for AWS MSK TLS |
| `KAFKA_SASL_USERNAME` | (unset) | MSK SASL_PLAIN username |
| `KAFKA_SASL_PASSWORD` | (unset) | MSK SASL_PLAIN password |
| `SCALE_KAFKA_MSGS` | `100000` | Default message count for sweeps |
| `SCALE_KAFKA_PRODUCERS` | `8` | Concurrent producer goroutines |
| `SCALE_KAFKA_CONSUMERS` | `12` | Max consumers in group-parallelism test |
| `SCALE_KAFKA_DURATION` | `60s` | Default duration for time-bounded tests |

SASL/TLS are wired into both `kafka-go.Writer.Transport` and
`kafka-go.ReaderConfig.Dialer` — even when unused locally — so the same tests
run unchanged against a SASL-secured MSK cluster.

## Running

```bash
cd /home/rajesh/go_workspace/videostreamingplatform-e2e
go test ./tests/scale/kafka_throughput/... -v -timeout 60m -run '.*' 2>&1 | tee /tmp/scale_kafka.log

# Faster iteration during development:
go test ./tests/scale/kafka_throughput/... -v -short -timeout 10m

# A single subtest:
go test ./tests/scale/kafka_throughput/... -v -run TestProducer_SingleThread_Rate
```

`TestConsumer_GroupOfN_Parallelism` (the 500k-msg test) is skipped under
`-short`. The lag test relies on shelling out to `docker exec kafka
/opt/kafka/bin/kafka-consumer-groups.sh --describe`, which requires the
`kafka` container to be running locally.

## Notes / caveats

* The existing `client.KafkaProducer` hardcodes `RequiredAcks=RequireAll` +
  `BatchTimeout=100ms`. The sweep tests construct `kafka-go.Writer` directly
  so they can vary `acks` and `linger` independently — using the wrapper
  would invalidate the sweep.
* `TestConsumer_RebalanceLatency` infers rebalance pauses from gaps in
  delivered message timestamps because kafka-go's `Reader` does not expose
  `PartitionsAssigned/Revoked` callbacks.
* `TestProducer_BrokerSlow` documents that `tc/netem` requires `NET_ADMIN`
  inside the kafka container (not granted in the default compose file) and
  falls back to extreme-rate produce as the task allows.
