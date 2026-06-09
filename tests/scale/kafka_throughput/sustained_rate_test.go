package kafka_throughput

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// TopicSustained is a dedicated topic for the steady-state target-rate test so the
// partition count can be dialed independently of TopicLow/TopicHigh.
const TopicSustained = "scaletest-events-sustained"

// newAsyncWriter builds an async, batched writer suitable for sustained high-rate
// produce. Unlike newWriter (which is used by the latency sweeps and does a
// synchronous round-trip per WriteMessages call — round-trip-bound, caps out well
// below 100k/s), this writer enqueues and returns immediately; delivery results
// arrive on the Completion callback. That's the only way a single Go process can
// approach a six-figure produce rate.
func newAsyncWriter(cfg scaleConfig, topic string, onComplete func(msgs []kafkago.Message, err error)) *kafkago.Writer {
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Topic:        topic,
		Balancer:     &kafkago.RoundRobin{}, // even partition spread regardless of key
		RequiredAcks: kafkago.RequireOne,
		Async:        true,
		BatchSize:    1000,
		BatchBytes:   16 * 1024 * 1024,
		BatchTimeout: 10 * time.Millisecond,
		MaxAttempts:  3,
		Completion:   onComplete,
	}
	if tr := cfg.transport(); tr != nil {
		w.Transport = tr
	}
	return w
}

// TestSustained_TargetRPS_ProduceConsume is the only test in the suite that validates
// the *architecture goal* (100k RPS) rather than measuring a one-off throughput number.
// It runs N producers and a consumer group of M concurrently for a fixed duration, then
// asserts the consumer KEPT UP: every produced message is drained within a bounded grace
// window and end-of-run lag returns to ~0.
//
// What's different from the existing tests:
//   - TestProducer_Concurrent_8Threads measures aggregate produce rate (no consumer, no keep-up check).
//   - TestLag_UnderBurst does burst-then-drain (not a sustained target rate).
//   - TestLag_EndToEnd_Latency runs steady-state but at only 100 msgs/s.
//
// Knobs (all optional; defaults aim at the 100k goal):
//   - SCALE_KAFKA_TARGET_RPS  (default 100000) — headline target, used for reporting + the keep-up window.
//   - SCALE_KAFKA_MIN_RPS     (default 0)      — hard floor for produce AND consume rate. 0 = report-only.
//   - SCALE_KAFKA_PARTITIONS  (default 12)     — partitions on the sustained topic. Consumers are capped to this.
//   - SCALE_KAFKA_PRODUCERS   (default 8)      — concurrent async producers.
//   - SCALE_KAFKA_CONSUMERS   (default 12)     — consumer-group members.
//   - SCALE_KAFKA_DURATION    (default 60s)    — produce window.
//   - SCALE_KAFKA_MSG_SIZE    (default 1024)   — payload bytes per message.
//
// ENVIRONMENT CEILING: a single-broker / 16 GB local box CANNOT reach 100k msgs/s with the
// load generator co-resident (the skill documents 78% producer error on a single broker).
// That is why SCALE_KAFKA_MIN_RPS defaults to 0 — this test reports the achieved rate and
// verifies correctness (no message loss, lag drains) locally, and only enforces the 100k
// floor when pointed at real multi-broker hardware with SCALE_KAFKA_MIN_RPS set.
func TestSustained_TargetRPS_ProduceConsume(t *testing.T) {
	cfg := loadScaleConfig()
	target := intEnv("SCALE_KAFKA_TARGET_RPS", 100000)
	minRPS := intEnv("SCALE_KAFKA_MIN_RPS", 0)
	partitions := intEnv("SCALE_KAFKA_PARTITIONS", 12)
	size := intEnv("SCALE_KAFKA_MSG_SIZE", 1024)
	producers := cfg.Producers
	if producers <= 0 {
		producers = 8
	}
	consumers := cfg.Consumers
	if consumers <= 0 {
		consumers = 12
	}
	if consumers > partitions {
		// Members beyond the partition count sit idle in a consumer group.
		consumers = partitions
	}
	duration := cfg.Duration
	if duration <= 0 {
		duration = 60 * time.Second
	}
	if testing.Short() {
		duration = 5 * time.Second
		minRPS = 0 // never enforce a floor in -short
	}

	if err := resetTopic(cfg.Brokers, TopicSustained, partitions); err != nil {
		t.Fatalf("resetTopic: %v", err)
	}

	t.Logf("=== Sustained target-rate produce+consume ===")
	t.Logf("topic=%s partitions=%d producers=%d consumers=%d duration=%s msg_size=%dB",
		TopicSustained, partitions, producers, consumers, duration, size)
	t.Logf("target=%d msgs/s  floor(SCALE_KAFKA_MIN_RPS)=%d (%s)",
		target, minRPS, floorMode(minRPS))

	// Precondition: the topic must actually have the partitions we asked for. A topic
	// silently stuck at 1 partition (broker auto-create default) caps all the parallelism
	// and makes the keep-up assertion meaningless — fail loudly instead.
	if got := topicPartitionCount(cfg.Brokers, TopicSustained); got != partitions {
		t.Fatalf("precondition failed: topic %s has %d partitions, want %d (topic setup is broken)",
			TopicSustained, got, partitions)
	}

	// Total volume is bounded to target×duration so the test runtime and the producer's
	// in-flight backlog stay bounded. Producers push this volume as fast as they can; the
	// achieved rate = delivered / wall-time then tells us whether the cluster sustained the
	// target (volume drains in ≤ duration ⇒ rate ≥ target).
	totalVolume := int64(target) * int64(duration/time.Second)
	if totalVolume <= 0 {
		totalVolume = int64(target)
	}
	t.Logf("volume=%d msgs (target × duration)", totalVolume)

	// ---- consumer group: start first so the bootstrap window doesn't eat the produce window ----
	var consumed int64
	groupID := uniqueGroupID("scale-sustained")
	consumeCtx, stopConsumers := context.WithCancel(context.Background())
	var consWG sync.WaitGroup
	perMember := make([]int64, consumers)
	for i := 0; i < consumers; i++ {
		consWG.Add(1)
		go func(idx int) {
			defer consWG.Done()
			r := newReader(cfg, TopicSustained, groupID)
			defer r.Close()
			for {
				select {
				case <-consumeCtx.Done():
					return
				default:
				}
				mctx, mcancel := context.WithTimeout(consumeCtx, 2*time.Second)
				_, err := r.ReadMessage(mctx)
				mcancel()
				if err != nil {
					if consumeCtx.Err() != nil {
						return
					}
					continue // idle read timeout — keep polling
				}
				atomic.AddInt64(&perMember[idx], 1)
				atomic.AddInt64(&consumed, 1)
			}
		}(i)
	}
	// Bootstrap tax for a fresh consumer group is ~3-7s; let members join + get assignments
	// before we start the clock, so the idle join window doesn't dilute the consume rate.
	time.Sleep(7 * time.Second)

	// ---- producers: async, batched, push max-rate for the duration ----
	var delivered, produceErrs int64
	onComplete := func(msgs []kafkago.Message, err error) {
		if err != nil {
			atomic.AddInt64(&produceErrs, int64(len(msgs)))
			return
		}
		atomic.AddInt64(&delivered, int64(len(msgs)))
	}
	body := payload(size)
	const batchPerCall = 500

	var enqueued int64 // shared budget cursor across producers
	var prodWG sync.WaitGroup
	startBarrier := make(chan struct{})
	produceStart := time.Now()
	for p := 0; p < producers; p++ {
		prodWG.Add(1)
		go func() {
			defer prodWG.Done()
			w := newAsyncWriter(cfg, TopicSustained, onComplete)
			defer w.Close() // Close flushes pending batches and waits for their Completion
			// Safety ceiling: even a badly degraded cluster can't run forever.
			ctx, cancel := context.WithTimeout(context.Background(), 10*duration+5*time.Minute)
			defer cancel()
			<-startBarrier
			batch := make([]kafkago.Message, 0, batchPerCall)
			for {
				// Claim the next slice of the shared volume budget.
				end := atomic.AddInt64(&enqueued, batchPerCall)
				start := end - batchPerCall
				if start >= totalVolume {
					return
				}
				n := batchPerCall
				if end > totalVolume {
					n = int(totalVolume - start)
				}
				batch = batch[:0]
				for k := 0; k < n; k++ {
					batch = append(batch, kafkago.Message{Value: body})
				}
				// Async writer: WriteMessages enqueues and returns (blocking only when the
				// internal queue is full — that's the backpressure that bounds memory);
				// delivery results land on onComplete.
				if err := w.WriteMessages(ctx, batch...); err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(5 * time.Millisecond) // transient enqueue error; brief backoff
				}
			}
		}()
	}
	close(startBarrier)
	prodWG.Wait() // includes the flush in each writer's Close()
	produceElapsed := time.Since(produceStart)
	produced := atomic.LoadInt64(&delivered)

	produceRate := float64(produced) / produceElapsed.Seconds()
	t.Logf("--- produce window done: delivered=%d errs=%d elapsed=%s rate=%.0f msgs/s (%.1f%% of target) ---",
		produced, atomic.LoadInt64(&produceErrs), produceElapsed.Round(time.Millisecond),
		produceRate, 100*produceRate/float64(target))

	// ---- keep-up: wait for the consumer group to drain the backlog within a bounded grace window ----
	// If consumers genuinely kept up, drain completes quickly. The grace is generous (the produce
	// window again, min 30s) so a healthy-but-slightly-behind consumer still passes.
	grace := duration
	if grace < 30*time.Second {
		grace = 30 * time.Second
	}
	drainDeadline := time.Now().Add(grace)
	for atomic.LoadInt64(&consumed) < produced && time.Now().Before(drainDeadline) {
		time.Sleep(250 * time.Millisecond)
	}
	// Measure consume throughput over the concurrent produce→drain window (consumers were
	// already joined and warm before produceStart), so it reflects steady-state keep-up
	// rate rather than being diluted by the join/bootstrap idle period.
	consumeElapsed := time.Since(produceStart)
	got := atomic.LoadInt64(&consumed)
	stopConsumers()
	consWG.Wait()

	consumeRate := float64(got) / consumeElapsed.Seconds()

	// Best-effort end-of-run lag (CLI shells into a container named "kafka"; -1 when unavailable).
	lagCtx, lagCancel := context.WithTimeout(context.Background(), 8*time.Second)
	endLag, lagErr := describeLagViaCLI(lagCtx, groupID, TopicSustained)
	lagCancel()

	// ---- report ----
	t.Logf("%-18s | %14s", "METRIC", "VALUE")
	t.Logf("%-18s | %14.0f", "produce msgs/s", produceRate)
	t.Logf("%-18s | %14.0f", "consume msgs/s", consumeRate)
	t.Logf("%-18s | %14d", "produced", produced)
	t.Logf("%-18s | %14d", "consumed", got)
	t.Logf("%-18s | %13.1f%%", "keep-up", 100*float64(got)/float64(max64(produced, 1)))
	if lagErr == nil && endLag >= 0 {
		t.Logf("%-18s | %14d", "end lag", endLag)
	} else {
		t.Logf("%-18s | %14s", "end lag", "n/a (CLI unavailable)")
	}
	activeMembers := 0
	for i, v := range perMember {
		t.Logf("  member[%d] consumed=%d", i, v)
		if v > 0 {
			activeMembers++
		}
	}

	// ---- assertions ----
	if produced == 0 {
		t.Fatalf("producers made no progress; delivered=0 (is Kafka reachable at %s?)", cfg.BrokersCSV)
	}
	// Parallelism actually happened: with >1 partition and >1 consumer, work must spread
	// across members. (A single member draining everything is the single-partition-topic
	// symptom that previously hid behind a passing keep-up check.)
	if partitions > 1 && consumers > 1 && activeMembers < 2 {
		t.Fatalf("no consumer parallelism: only %d of %d members consumed (partitions=%d) — "+
			"messages likely landed on one partition", activeMembers, consumers, partitions)
	}
	// Correctness regardless of environment: the consumer must drain everything it was sent.
	// Allow tiny slack for at-least-once tail timing.
	if got < produced-int64(float64(produced)*0.001)-1 {
		t.Fatalf("consumer did NOT keep up: produced=%d consumed=%d (backlog %d not drained within %s grace)",
			produced, got, produced-got, grace)
	}
	// Throughput floor only enforced when explicitly opted in (real multi-broker hardware).
	if minRPS > 0 {
		if produceRate < float64(minRPS) {
			t.Fatalf("produce rate %.0f msgs/s below floor SCALE_KAFKA_MIN_RPS=%d", produceRate, minRPS)
		}
		if consumeRate < float64(minRPS) {
			t.Fatalf("consume rate %.0f msgs/s below floor SCALE_KAFKA_MIN_RPS=%d", consumeRate, minRPS)
		}
	} else {
		t.Logf("NOTE: SCALE_KAFKA_MIN_RPS=0 — rate floor not enforced (report-only). "+
			"Set SCALE_KAFKA_MIN_RPS=%d against multi-broker hardware to gate on the architecture target.", target)
	}
}

// floorMode describes whether the rate floor is enforced, for the header log line.
func floorMode(minRPS int) string {
	if minRPS > 0 {
		return "ENFORCED"
	}
	return "report-only"
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// topicPartitionCount returns the current partition count for a topic, or -1 if it
// can't be read. The topic must already exist (this reads it by name).
func topicPartitionCount(brokers []string, topic string) int {
	if len(brokers) == 0 {
		return -1
	}
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		return -1
	}
	defer conn.Close()
	parts, err := conn.ReadPartitions(topic)
	if err != nil {
		return -1
	}
	return len(parts)
}
