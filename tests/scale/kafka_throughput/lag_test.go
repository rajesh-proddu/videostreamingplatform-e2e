package kafka_throughput

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

func TestLag_UnderBurst(t *testing.T) {
	cfg := loadScaleConfig()
	topic := TopicLow
	if err := resetTopic(cfg.Brokers, topic, 3); err != nil {
		t.Fatalf("resetTopic: %v", err)
	}
	n := 50000
	if testing.Short() {
		n = 3000
	}
	const size = 512
	groupID := uniqueGroupID("scale-lag-burst")

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	// 1) Start the slow consumer first so its group is registered before we sample lag.
	slowDone := make(chan struct{})
	var consumed int64
	go func() {
		defer close(slowDone)
		r := newReader(cfg, topic, groupID)
		defer r.Close()
		for {
			mctx, mcancel := context.WithTimeout(ctx, 30*time.Second)
			_, err := r.ReadMessage(mctx)
			mcancel()
			if err != nil {
				return
			}
			atomic.AddInt64(&consumed, 1)
			time.Sleep(50 * time.Millisecond) // artificial slowness
			if atomic.LoadInt64(&consumed) >= int64(n) {
				return
			}
		}
	}()
	// give the consumer a moment to join + commit so kafka-consumer-groups.sh can find it
	time.Sleep(3 * time.Second)

	// 2) Produce a burst as fast as possible.
	w := newWriter(cfg, topic, kafkago.RequireOne, 10, 500)
	defer w.Close()
	body := payload(size)
	prodStart := time.Now()
	batch := make([]kafkago.Message, 0, 500)
	for i := 0; i < n; i++ {
		batch = append(batch, kafkago.Message{
			Key:   []byte(fmt.Sprintf("burst-%d", i)),
			Value: body,
		})
		if len(batch) >= 500 {
			if err := w.WriteMessages(ctx, batch...); err != nil {
				t.Fatalf("burst write: %v", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := w.WriteMessages(ctx, batch...); err != nil {
			t.Fatalf("burst tail: %v", err)
		}
	}
	t.Logf("=== Lag under burst | topic=%s | burst=%d msgs produced in %s ===", topic, n, time.Since(prodStart))

	// 3) Sample lag for up to maxSamples * interval.
	t.Logf("%-10s | %12s | %12s", "T+", "LAG", "CONSUMED")
	const samples = 20
	const interval = 3 * time.Second
	prev := int64(-1)
	for i := 0; i < samples; i++ {
		lagCtx, lc := context.WithTimeout(ctx, 8*time.Second)
		lag, err := describeLagViaCLI(lagCtx, groupID, topic)
		lc()
		c := atomic.LoadInt64(&consumed)
		if err != nil {
			t.Logf("%-10s | %12s | %12d (lag query: %v)", time.Duration(i)*interval, "?", c, err)
		} else {
			t.Logf("%-10s | %12d | %12d", time.Duration(i)*interval, lag, c)
		}
		if lag == 0 && prev == 0 && c >= int64(n) {
			break
		}
		prev = lag
		time.Sleep(interval)
	}
	// don't block on the consumer; it'll exit when it hits n or ctx times out
	select {
	case <-slowDone:
	case <-time.After(5 * time.Second):
	}
}

func TestLag_ConsumerCold_BootstrapTime(t *testing.T) {
	cfg := loadScaleConfig()
	topic := TopicLow
	// Make sure there's at least one in-flight message so the first poll has data.
	w := newWriter(cfg, topic, kafkago.RequireOne, 0, 1)
	defer w.Close()
	prodCtx, prodCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer prodCancel()
	body := payload(64)
	// Put some recent messages so a from-latest consumer would have something to read;
	// but we use FirstOffset so this just guarantees nonempty partitions.
	for i := 0; i < 10; i++ {
		if err := w.WriteMessages(prodCtx, kafkago.Message{Key: []byte(fmt.Sprintf("bs-%d", i)), Value: body}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	const trials = 10
	samples := make([]time.Duration, 0, trials)
	t.Logf("=== Cold-bootstrap consumer | topic=%s | trials=%d ===", topic, trials)
	for i := 0; i < trials; i++ {
		groupID := uniqueGroupID(fmt.Sprintf("scale-cold-%d", i))
		// Re-seed a unique message so the new group has something fresh to deliver.
		uniq := fmt.Sprintf("trial-%d-%d", i, time.Now().UnixNano())
		if err := w.WriteMessages(prodCtx, kafkago.Message{Key: []byte(uniq), Value: []byte(uniq)}); err != nil {
			t.Fatalf("seed trial %d: %v", i, err)
		}

		r := newReader(cfg, topic, groupID)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		start := time.Now()
		_, err := r.ReadMessage(ctx)
		dur := time.Since(start)
		cancel()
		r.Close()
		if err != nil {
			t.Logf("  trial %d: error after %s: %v", i, dur, err)
			continue
		}
		samples = append(samples, dur)
		t.Logf("  trial %d: first-message after %s", i, dur)
	}
	if len(samples) == 0 {
		t.Fatalf("no successful bootstrap trials")
	}
	p := percentiles(samples)
	t.Logf("RESULT: bootstrap latency over %d trials: p50=%s p95=%s p99=%s max=%s",
		len(samples), p.P50, p.P95, p.P99, p.Max)
}

func TestLag_EndToEnd_Latency(t *testing.T) {
	cfg := loadScaleConfig()
	topic := TopicLow
	if err := resetTopic(cfg.Brokers, topic, 3); err != nil {
		t.Fatalf("resetTopic: %v", err)
	}
	duration := 60 * time.Second
	if testing.Short() {
		duration = 10 * time.Second
	}
	const targetRate = 100 // msgs/sec
	groupID := uniqueGroupID("scale-e2e")

	ctx, cancel := context.WithTimeout(context.Background(), duration+90*time.Second)
	defer cancel()

	// Start the consumer first so first messages aren't lost to the bootstrap window.
	type sample struct{ latency time.Duration }
	samplesCh := make(chan sample, 4096)
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		r := newReader(cfg, topic, groupID)
		defer r.Close()
		idleDeadline := time.Now().Add(duration + 60*time.Second)
		for {
			if time.Now().After(idleDeadline) {
				return
			}
			mctx, mcancel := context.WithTimeout(ctx, 3*time.Second)
			m, err := r.ReadMessage(mctx)
			mcancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				return
			}
			if len(m.Value) < 8 {
				continue
			}
			tsNanos := int64(binary.BigEndian.Uint64(m.Value[:8]))
			lat := time.Since(time.Unix(0, tsNanos))
			select {
			case samplesCh <- sample{lat}:
			default:
				// drop if the channel is saturated; we still get statistical coverage
			}
		}
	}()

	// Give the consumer a chance to join (bootstrap tax is ~3-7s).
	time.Sleep(5 * time.Second)

	// Produce at ~100/s.
	w := newWriter(cfg, topic, kafkago.RequireOne, 0, 1)
	defer w.Close()
	tick := time.NewTicker(time.Second / targetRate)
	defer tick.Stop()
	deadline := time.Now().Add(duration)
	produced := 0
	for time.Now().Before(deadline) {
		<-tick.C
		buf := make([]byte, 8+64)
		binary.BigEndian.PutUint64(buf[:8], uint64(time.Now().UnixNano()))
		copy(buf[8:], "e2e-payload-padding")
		pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
		err := w.WriteMessages(pctx, kafkago.Message{Key: []byte(fmt.Sprintf("e2e-%d", produced)), Value: buf})
		pcancel()
		if err != nil {
			continue
		}
		produced++
	}

	// Wait for consumer to drain the tail or for its idle timeout.
	collector := make([]time.Duration, 0, produced)
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for s := range samplesCh {
			collector = append(collector, s.latency)
		}
	}()
	// Let the consumer keep reading for a few more seconds, then unblock.
	time.Sleep(8 * time.Second)
	cancel()
	<-consumerDone
	close(samplesCh)
	<-collectorDone

	t.Logf("=== End-to-end latency | topic=%s | duration=%s | target=%d/s ===", topic, duration, targetRate)
	t.Logf("produced=%d delivered_sampled=%d", produced, len(collector))
	if len(collector) == 0 {
		t.Fatalf("no latency samples collected")
	}
	p := percentiles(collector)
	t.Logf("RESULT: e2e latency p50=%s p95=%s p99=%s max=%s", p.P50, p.P95, p.P99, p.Max)
}

