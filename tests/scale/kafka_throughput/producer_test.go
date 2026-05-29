package kafka_throughput

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// newWriter builds a Writer with the given knobs, plus SASL/TLS plumbing for AWS MSK
// compatibility. Tests pass acks/batch knobs directly so the existing client wrapper
// (which hardcodes acks=all, linger=100ms) doesn't bias the sweep.
func newWriter(cfg scaleConfig, topic string, acks kafkago.RequiredAcks, lingerMs int, batchSize int) *kafkago.Writer {
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Topic:        topic,
		Balancer:     &kafkago.RoundRobin{},
		RequiredAcks: acks,
		BatchTimeout: time.Duration(lingerMs) * time.Millisecond,
		BatchSize:    batchSize,
		// Generous batch byte cap so the BatchSize sweep isn't byte-capped first.
		BatchBytes: 16 * 1024 * 1024,
		// MaxAttempts is low so a misbehaving broker fails fast instead of silently retrying.
		MaxAttempts: 3,
	}
	if t := cfg.transport(); t != nil {
		w.Transport = t
	}
	return w
}

func TestProducer_SingleThread_Rate(t *testing.T) {
	cfg := loadScaleConfig()
	const sizeBytes = 1024
	msgs := cfg.Msgs
	if testing.Short() {
		msgs = 5000
	}
	body := payload(sizeBytes)

	type variant struct {
		name     string
		acks     kafkago.RequiredAcks
		lingerMs int
	}
	variants := []variant{
		{"acks=0 linger=0", kafkago.RequireNone, 0},
		{"acks=1 linger=0", kafkago.RequireOne, 0},
		{"acks=all linger=0", kafkago.RequireAll, 0},
		{"acks=1 linger=10", kafkago.RequireOne, 10},
		{"acks=1 linger=50", kafkago.RequireOne, 50},
		{"acks=all linger=10", kafkago.RequireAll, 10},
	}

	t.Logf("=== Single-thread producer | topic=%s | msgs=%d | size=%d bytes ===", TopicLow, msgs, sizeBytes)
	t.Logf("%-22s | %10s | %10s | %10s | %10s | %10s", "VARIANT", "MSGS/S", "MB/S", "ACK p50", "ACK p95", "ACK p99")

	for _, v := range variants {
		v := v
		t.Run(strings.ReplaceAll(v.name, " ", "_"), func(t *testing.T) {
			// BatchSize=1 isolates per-call ACK latency so the sweep means what it says.
			w := newWriter(cfg, TopicLow, v.acks, v.lingerMs, 1)
			defer w.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			samples := make([]time.Duration, 0, msgs)
			start := time.Now()
			for i := 0; i < msgs; i++ {
				t0 := time.Now()
				err := w.WriteMessages(ctx, kafkago.Message{
					Key:   []byte(fmt.Sprintf("k-%d", i)),
					Value: body,
				})
				if err != nil {
					t.Fatalf("write %d: %v", i, err)
				}
				samples = append(samples, time.Since(t0))
			}
			elapsed := time.Since(start)
			rate := float64(msgs) / elapsed.Seconds()
			mbps := (float64(msgs) * float64(sizeBytes)) / elapsed.Seconds() / (1024 * 1024)
			p := percentiles(samples)
			t.Logf("%-22s | %10.1f | %10.2f | %10s | %10s | %10s",
				v.name, rate, mbps, p.P50, p.P95, p.P99)
		})
	}
}

func TestProducer_Concurrent_8Threads(t *testing.T) {
	cfg := loadScaleConfig()
	producers := cfg.Producers
	if producers <= 0 {
		producers = 8
	}
	perProducer := cfg.Msgs / producers
	if perProducer < 1000 {
		perProducer = 50000
	}
	if testing.Short() {
		perProducer = 2000
	}
	const sizeBytes = 1024
	body := payload(sizeBytes)

	t.Logf("=== Concurrent producers | topic=%s (12 parts) | producers=%d | msgs/producer=%d | size=%d ===",
		TopicHigh, producers, perProducer, sizeBytes)

	type result struct {
		idx     int
		elapsed time.Duration
		p95     time.Duration
		errs    int
	}

	results := make(chan result, producers)
	var wg sync.WaitGroup
	startBarrier := make(chan struct{})
	overallStart := time.Now()

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Linger=10ms + BatchSize=200 is a reasonable production-ish setting that lets
			// kafka-go batch within each producer.
			w := newWriter(cfg, TopicHigh, kafkago.RequireOne, 10, 200)
			defer w.Close()
			samples := make([]time.Duration, 0, perProducer)
			<-startBarrier
			localStart := time.Now()
			errs := 0
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			defer cancel()
			for j := 0; j < perProducer; j++ {
				t0 := time.Now()
				err := w.WriteMessages(ctx, kafkago.Message{
					Key:   []byte(fmt.Sprintf("p%d-%d", idx, j)),
					Value: body,
				})
				if err != nil {
					errs++
					continue
				}
				samples = append(samples, time.Since(t0))
			}
			p := percentiles(samples)
			results <- result{idx: idx, elapsed: time.Since(localStart), p95: p.P95, errs: errs}
		}(i)
	}

	close(startBarrier)
	wg.Wait()
	close(results)
	totalElapsed := time.Since(overallStart)

	totalMsgs := producers * perProducer
	totalErrs := 0
	for r := range results {
		t.Logf("  producer[%d] elapsed=%s p95=%s errs=%d", r.idx, r.elapsed, r.p95, r.errs)
		totalErrs += r.errs
	}
	rate := float64(totalMsgs-totalErrs) / totalElapsed.Seconds()
	mbps := (float64(totalMsgs-totalErrs) * float64(sizeBytes)) / totalElapsed.Seconds() / (1024 * 1024)
	t.Logf("AGGREGATE: msgs=%d errs=%d elapsed=%s rate=%.1f msgs/s = %.2f MB/s",
		totalMsgs, totalErrs, totalElapsed, rate, mbps)
}

func TestProducer_MessageSizeSweep(t *testing.T) {
	cfg := loadScaleConfig()
	const count = 10000
	sizes := []int{1024, 16 * 1024, 256 * 1024, 1024 * 1024}
	smallCount := count
	if testing.Short() {
		smallCount = 1000
	}

	t.Logf("=== Message size sweep | topic=%s | per-size count=%d ===", TopicHigh, smallCount)
	t.Logf("%-10s | %10s | %12s | %10s | %10s", "SIZE", "MSGS/S", "MB/S", "ACK p50", "ACK p95")

	for _, size := range sizes {
		size := size
		// 1MiB × 10k = 10 GiB; that's a lot for a single local broker. Scale down the
		// count for large sizes so the test finishes within the 60min budget.
		n := smallCount
		if size >= 256*1024 {
			n = smallCount / 4
		}
		if size >= 1024*1024 {
			n = smallCount / 10
		}
		if n < 100 {
			n = 100
		}
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			body := payload(size)
			w := newWriter(cfg, TopicHigh, kafkago.RequireOne, 10, 100)
			defer w.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
			defer cancel()
			samples := make([]time.Duration, 0, n)
			start := time.Now()
			for i := 0; i < n; i++ {
				t0 := time.Now()
				err := w.WriteMessages(ctx, kafkago.Message{
					Key:   []byte(fmt.Sprintf("s%d-%d", size, i)),
					Value: body,
				})
				if err != nil {
					t.Fatalf("write size=%d i=%d: %v", size, i, err)
				}
				samples = append(samples, time.Since(t0))
			}
			elapsed := time.Since(start)
			rate := float64(n) / elapsed.Seconds()
			mbps := (float64(n) * float64(size)) / elapsed.Seconds() / (1024 * 1024)
			p := percentiles(samples)
			t.Logf("%-10d | %10.1f | %12.2f | %10s | %10s", size, rate, mbps, p.P50, p.P95)
		})
	}
}

func TestProducer_BatchedVsUnbatched(t *testing.T) {
	cfg := loadScaleConfig()
	const sizeBytes = 1024
	body := payload(sizeBytes)
	msgs := cfg.Msgs
	if msgs > 50000 {
		msgs = 50000 // bound this comparison
	}
	if testing.Short() {
		msgs = 3000
	}

	t.Logf("=== Batch size sweep | topic=%s | msgs=%d | size=%d ===", TopicLow, msgs, sizeBytes)
	t.Logf("%-12s | %10s | %10s", "BATCH SIZE", "MSGS/S", "MB/S")

	for _, bs := range []int{1, 100, 1000} {
		bs := bs
		t.Run(fmt.Sprintf("batch=%d", bs), func(t *testing.T) {
			// Linger=50ms lets the writer fill the batch when BatchSize is large enough
			// that we're not flushing on count alone.
			w := newWriter(cfg, TopicLow, kafkago.RequireOne, 50, bs)
			defer w.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			start := time.Now()
			// To exercise batching we feed messages in groups equal to bs in a single
			// WriteMessages call (kafka-go also batches across calls when async, but a
			// single batched call is the cleanest comparison).
			pending := make([]kafkago.Message, 0, bs)
			flush := func() {
				if len(pending) == 0 {
					return
				}
				if err := w.WriteMessages(ctx, pending...); err != nil {
					t.Fatalf("flush batch=%d: %v", bs, err)
				}
				pending = pending[:0]
			}
			for i := 0; i < msgs; i++ {
				pending = append(pending, kafkago.Message{
					Key:   []byte(fmt.Sprintf("b%d-%d", bs, i)),
					Value: body,
				})
				if len(pending) >= bs {
					flush()
				}
			}
			flush()
			elapsed := time.Since(start)
			rate := float64(msgs) / elapsed.Seconds()
			mbps := (float64(msgs) * float64(sizeBytes)) / elapsed.Seconds() / (1024 * 1024)
			t.Logf("%-12d | %10.1f | %10.2f", bs, rate, mbps)
		})
	}
}
