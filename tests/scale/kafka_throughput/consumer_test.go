package kafka_throughput

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// produceN pumps n messages of `size` bytes to `topic` using a single high-throughput writer.
// Used as the "pre-produce" step for consumer benchmarks.
func produceN(ctx context.Context, t *testing.T, cfg scaleConfig, topic string, n, size int) {
	t.Helper()
	w := newWriter(cfg, topic, kafkago.RequireOne, 50, 1000)
	defer w.Close()
	body := payload(size)
	const chunk = 1000
	buf := make([]kafkago.Message, 0, chunk)
	for i := 0; i < n; i++ {
		buf = append(buf, kafkago.Message{
			Key:   []byte(fmt.Sprintf("k-%d", i)),
			Value: body,
		})
		if len(buf) >= chunk {
			if err := w.WriteMessages(ctx, buf...); err != nil {
				t.Fatalf("produceN flush at %d: %v", i, err)
			}
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		if err := w.WriteMessages(ctx, buf...); err != nil {
			t.Fatalf("produceN tail flush: %v", err)
		}
	}
}

// newReader builds a Reader subscribed to the topic via a consumer group, starting
// from the earliest offset. Returns nil-dialer when SASL/TLS aren't configured.
func newReader(cfg scaleConfig, topic, groupID string) *kafkago.Reader {
	rc := kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024,
		StartOffset:    kafkago.FirstOffset,
		CommitInterval: time.Second,
		MaxWait:        500 * time.Millisecond,
	}
	if d := cfg.dialer(); d != nil {
		rc.Dialer = d
	}
	return kafkago.NewReader(rc)
}

func TestConsumer_SingleConsumer_Throughput(t *testing.T) {
	cfg := loadScaleConfig()
	n := 200000
	if testing.Short() {
		n = 10000
	}
	const size = 1024
	// Use a fresh topic instance so prior runs don't bleed in.
	topic := TopicHigh
	if err := resetTopic(cfg.Brokers, topic, 12); err != nil {
		t.Fatalf("resetTopic: %v", err)
	}

	t.Logf("=== Single consumer throughput | topic=%s | pre-produce=%d msgs ===", topic, n)
	prodCtx, prodCancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer prodCancel()
	prodStart := time.Now()
	produceN(prodCtx, t, cfg, topic, n, size)
	t.Logf("  pre-produce: %d msgs in %s", n, time.Since(prodStart))

	r := newReader(cfg, topic, uniqueGroupID("scale-cons-single"))
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	var firstMsg time.Time
	var consumed int
	start := time.Now()
	for {
		// Bound the per-message read so we exit promptly when the topic drains.
		mctx, mcancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := r.ReadMessage(mctx)
		mcancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("ReadMessage: %v", err)
		}
		consumed++
		if consumed == 1 {
			firstMsg = time.Now()
		}
		if consumed >= n {
			break
		}
	}
	drain := time.Since(firstMsg)
	bootstrap := firstMsg.Sub(start)
	rate := float64(consumed) / drain.Seconds()
	mbps := (float64(consumed) * float64(size)) / drain.Seconds() / (1024 * 1024)
	t.Logf("RESULT: consumed=%d bootstrap=%s drain=%s rate=%.1f msgs/s = %.2f MB/s",
		consumed, bootstrap, drain, rate, mbps)
}

func TestConsumer_GroupOfN_Parallelism(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 500k consumer-group test in -short")
	}
	cfg := loadScaleConfig()
	n := 500000
	if cfg.Msgs > 0 && cfg.Msgs < n {
		// Honor the knob so a downsized run actually downsizes.
		n = cfg.Msgs
	}
	const size = 1024
	topic := TopicHigh
	if err := resetTopic(cfg.Brokers, topic, 12); err != nil {
		t.Fatalf("resetTopic: %v", err)
	}

	prodCtx, prodCancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer prodCancel()
	t.Logf("=== Consumer group parallelism | topic=%s (12 parts) | pre-produce=%d ===", topic, n)
	prodStart := time.Now()
	produceN(prodCtx, t, cfg, topic, n, size)
	t.Logf("  pre-produce: %d msgs in %s = %.1f msgs/s",
		n, time.Since(prodStart), float64(n)/time.Since(prodStart).Seconds())

	t.Logf("%-12s | %12s | %14s | %14s", "CONSUMERS", "DRAIN", "TOTAL MSGS/S", "PER-CONS MSGS/S")
	for _, c := range []int{1, 3, 6, 12} {
		c := c
		t.Run(fmt.Sprintf("consumers=%d", c), func(t *testing.T) {
			groupID := uniqueGroupID(fmt.Sprintf("scale-cons-group-%d", c))
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			defer cancel()
			var wg sync.WaitGroup
			perMember := make([]int64, c)
			startCh := make(chan struct{})
			start := time.Now()
			for i := 0; i < c; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					r := newReader(cfg, topic, groupID)
					defer r.Close()
					<-startCh
					for {
						mctx, mcancel := context.WithTimeout(ctx, 10*time.Second)
						_, err := r.ReadMessage(mctx)
						mcancel()
						if err != nil {
							// idle timeout — topic drained for this member
							return
						}
						atomic.AddInt64(&perMember[idx], 1)
					}
				}(i)
			}
			close(startCh)
			wg.Wait()
			elapsed := time.Since(start)
			var total int64
			for i, v := range perMember {
				total += v
				t.Logf("  member[%d]: %d msgs", i, v)
			}
			rate := float64(total) / elapsed.Seconds()
			perCons := rate / float64(c)
			t.Logf("%-12d | %12s | %14.1f | %14.1f", c, elapsed, rate, perCons)
		})
	}
}

func TestConsumer_RebalanceLatency(t *testing.T) {
	cfg := loadScaleConfig()
	topic := TopicHigh
	if err := resetTopic(cfg.Brokers, topic, 12); err != nil {
		t.Fatalf("resetTopic: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// Continuous producer in background.
	stopProd := make(chan struct{})
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		w := newWriter(cfg, topic, kafkago.RequireOne, 10, 200)
		defer w.Close()
		body := payload(512)
		i := 0
		for {
			select {
			case <-stopProd:
				return
			default:
			}
			pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
			err := w.WriteMessages(pctx, kafkago.Message{
				Key: []byte(fmt.Sprintf("rb-%d", i)), Value: body,
			})
			pcancel()
			if err != nil {
				return
			}
			i++
			time.Sleep(2 * time.Millisecond) // ~500 msgs/s
		}
	}()
	defer func() {
		close(stopProd)
		<-prodDone
	}()

	// 4 consumers staggered by 1s. Each records the timestamps of every delivered
	// message; a gap > ~750ms in the steady-state stream signals a rebalance pause.
	const numCons = 4
	groupID := uniqueGroupID("scale-rebalance")
	type sample struct {
		idx int
		ts  time.Time
	}
	deliveries := make([][]time.Time, numCons)
	var wg sync.WaitGroup
	stopCons := make(chan struct{})

	startCons := func(idx int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := newReader(cfg, topic, groupID)
			defer r.Close()
			for {
				select {
				case <-stopCons:
					return
				default:
				}
				mctx, mcancel := context.WithTimeout(ctx, 1*time.Second)
				_, err := r.ReadMessage(mctx)
				mcancel()
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						continue
					}
					return
				}
				deliveries[idx] = append(deliveries[idx], time.Now())
			}
		}()
	}

	t.Logf("=== Rebalance latency | topic=%s | staggered %d consumers ===", topic, numCons)
	joinTimes := make([]time.Time, numCons)
	for i := 0; i < numCons; i++ {
		joinTimes[i] = time.Now()
		startCons(i)
		t.Logf("  consumer[%d] joined at t+%s", i, time.Since(joinTimes[0]))
		time.Sleep(1 * time.Second)
	}

	// Let the final group settle and consume for a while.
	time.Sleep(8 * time.Second)
	close(stopCons)
	wg.Wait()

	// Detect gaps: look at the overall delivered timestamps and report any inter-arrival
	// gap that exceeds 750ms, which under steady-state ~500msgs/s indicates a stop-the-world rebalance.
	var allTS []time.Time
	for _, d := range deliveries {
		allTS = append(allTS, d...)
	}
	sort.Slice(allTS, func(i, j int) bool { return allTS[i].Before(allTS[j]) })
	const gapThreshold = 750 * time.Millisecond
	var gaps []time.Duration
	for i := 1; i < len(allTS); i++ {
		g := allTS[i].Sub(allTS[i-1])
		if g >= gapThreshold {
			gaps = append(gaps, g)
		}
	}
	for i, d := range deliveries {
		t.Logf("  member[%d] delivered=%d", i, len(d))
	}
	if len(gaps) == 0 {
		t.Logf("RESULT: no gaps >= %s detected (rebalances may have been < threshold)", gapThreshold)
		return
	}
	t.Logf("RESULT: %d gaps >= %s detected (rebalance pauses):", len(gaps), gapThreshold)
	p := percentiles(gaps)
	t.Logf("  gap p50=%s p95=%s p99=%s max=%s", p.P50, p.P95, p.P99, p.Max)
}
