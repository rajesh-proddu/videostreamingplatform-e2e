// Package kafka_throughput contains Kafka producer/consumer throughput and lag scale tests.
//
// These tests target dedicated `scaletest-events-low` / `scaletest-events-high` topics
// to avoid polluting the live `video-events` / `watch-events` topics that are populated
// by the metadata-service and data-service.
package kafka_throughput

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	TopicLow  = "scaletest-events-low"  // 3 partitions
	TopicHigh = "scaletest-events-high" // 12 partitions
)

// scaleConfig pulls the Kafka-specific knobs from the environment.
type scaleConfig struct {
	Brokers      []string
	BrokersCSV   string
	UseTLS       bool
	SASLUser     string
	SASLPassword string
	// Knobs
	Msgs      int
	Producers int
	Consumers int
	Duration  time.Duration
}

func loadScaleConfig() scaleConfig {
	brokersCSV := envOr("KAFKA_BROKERS", "127.0.0.1:9092")
	return scaleConfig{
		Brokers:      strings.Split(brokersCSV, ","),
		BrokersCSV:   brokersCSV,
		UseTLS:       strings.EqualFold(os.Getenv("KAFKA_TLS"), "true"),
		SASLUser:     os.Getenv("KAFKA_SASL_USERNAME"),
		SASLPassword: os.Getenv("KAFKA_SASL_PASSWORD"),
		Msgs:         intEnv("SCALE_KAFKA_MSGS", 100000),
		Producers:    intEnv("SCALE_KAFKA_PRODUCERS", 8),
		Consumers:    intEnv("SCALE_KAFKA_CONSUMERS", 12),
		Duration:     durEnv("SCALE_KAFKA_DURATION", 60*time.Second),
	}
}

func envOr(k, v string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return v
}

func intEnv(k string, v int) int {
	if s := os.Getenv(k); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return v
}

func durEnv(k string, v time.Duration) time.Duration {
	if s := os.Getenv(k); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			return d
		}
	}
	return v
}

// saslMechanism returns the SASL mechanism if KAFKA_SASL_USERNAME is set (MSK / SASL_PLAIN).
// For local dev (no SASL) it returns nil.
func (c scaleConfig) saslMechanism() sasl.Mechanism {
	if c.SASLUser == "" {
		return nil
	}
	return plain.Mechanism{Username: c.SASLUser, Password: c.SASLPassword}
}

// tlsConfig returns a minimal TLS config when KAFKA_TLS=true.
func (c scaleConfig) tlsConfig() *tls.Config {
	if !c.UseTLS {
		return nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

// transport returns a Writer Transport with SASL/TLS wired in when configured.
// Returns nil when neither is set so the default plaintext transport is used.
func (c scaleConfig) transport() *kafkago.Transport {
	mech := c.saslMechanism()
	tlsCfg := c.tlsConfig()
	if mech == nil && tlsCfg == nil {
		return nil
	}
	return &kafkago.Transport{SASL: mech, TLS: tlsCfg}
}

// dialer returns a Reader Dialer with SASL/TLS wired in when configured.
// Returns nil for default behavior when neither is set.
func (c scaleConfig) dialer() *kafkago.Dialer {
	mech := c.saslMechanism()
	tlsCfg := c.tlsConfig()
	if mech == nil && tlsCfg == nil {
		return nil
	}
	return &kafkago.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mech,
		TLS:           tlsCfg,
	}
}

// ensureTopic creates a topic with the given partition count if it doesn't exist.
// Treats TopicAlreadyExistsError as success (idempotent).
func ensureTopic(brokers []string, topic string, partitions int) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial %s: %w", brokers[0], err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	cConn, err := kafkago.Dial("tcp", controllerAddr)
	if err != nil {
		// Some single-broker setups report a controller host (e.g. container name)
		// that's not resolvable from outside the docker network. Fall back to the
		// original broker address — for a single-node cluster that node IS the controller.
		cConn, err = kafkago.Dial("tcp", brokers[0])
		if err != nil {
			return fmt.Errorf("dial controller: %w", err)
		}
	}
	defer cConn.Close()

	err = cConn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err == nil {
		// Wait for metadata to propagate before returning: on a multi-broker
		// cluster a produce immediately after create can otherwise hit
		// UNKNOWN_TOPIC_OR_PARTITION until all partition leaders are elected.
		return waitTopicReady(brokers, topic, partitions, 20*time.Second)
	}
	// kafka-go returns this as an Error value with code 36 (TopicAlreadyExists)
	var kerr kafkago.Error
	if errors.As(err, &kerr) && kerr == kafkago.TopicAlreadyExists {
		return nil
	}
	// Some kafka-go versions wrap differently; fall back to string match.
	if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "ALREADY_EXISTS") {
		return nil
	}
	return fmt.Errorf("create topic %s: %w", topic, err)
}

// deleteTopic is best-effort; we leave topics in place so the run is repeatable
// without re-creation cost. Kept here in case we need to purge between runs.
func deleteTopic(brokers []string, topic string) {
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.DeleteTopics(topic)
}

// waitTopicReady blocks until the topic's metadata has propagated — all
// partitions present with an elected leader — or the timeout elapses. After
// CreateTopics returns, a produce/consume on a multi-broker cluster can briefly
// observe UNKNOWN_TOPIC_OR_PARTITION until metadata settles; this closes that race.
func waitTopicReady(brokers []string, topic string, partitions int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := kafkago.Dial("tcp", brokers[0])
		if err != nil {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		parts, perr := conn.ReadPartitions(topic)
		conn.Close()
		if perr == nil && len(parts) >= partitions {
			ready := true
			for _, p := range parts {
				if p.Leader.Host == "" {
					ready = false
					break
				}
			}
			if ready {
				return nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return nil // best effort — let the test surface a real failure if still not ready
}

// resetTopic deletes (best effort) and recreates a topic. Useful when we want a
// clean slate for end-to-end latency / drain measurements.
func resetTopic(brokers []string, topic string, partitions int) error {
	deleteTopic(brokers, topic)
	// Topic deletion is async; poll up to a few seconds for it to disappear,
	// otherwise CreateTopics will report AlreadyExists and we'll keep the old data.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := kafkago.Dial("tcp", brokers[0])
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		parts, perr := conn.ReadPartitions(topic)
		conn.Close()
		if perr != nil || len(parts) == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	return ensureTopic(brokers, topic, partitions)
}

// TestMain creates the load-test topics once for the package.
func TestMain(m *testing.M) {
	cfg := loadScaleConfig()
	for _, t := range []struct {
		name       string
		partitions int
	}{
		{TopicLow, 3},
		{TopicHigh, 12},
	} {
		if err := ensureTopic(cfg.Brokers, t.name, t.partitions); err != nil {
			fmt.Fprintf(os.Stderr, "setup: ensureTopic %s: %v\n", t.name, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

// ---------- shared helpers ----------

type latencyStats struct {
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
	Max time.Duration
}

func percentiles(samples []time.Duration) latencyStats {
	if len(samples) == 0 {
		return latencyStats{}
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pick := func(p float64) time.Duration {
		idx := int(float64(len(sorted)-1) * p)
		return sorted[idx]
	}
	return latencyStats{
		P50: pick(0.50),
		P95: pick(0.95),
		P99: pick(0.99),
		Max: sorted[len(sorted)-1],
	}
}

// payload generates a deterministic payload of n bytes (cheap pattern, no crypto).
func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('A' + (i % 26))
	}
	return b
}

// uniqueGroupID returns a non-colliding group id per test run.
func uniqueGroupID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// describeLagViaCLI shells out to kafka-consumer-groups.sh inside the kafka container
// and returns the aggregate LAG across all partitions for a given group/topic.
// Returns -1 if the group isn't yet known to the broker.
func describeLagViaCLI(ctx context.Context, group, topic string) (int64, error) {
	cmd := []string{
		"docker", "exec", "kafka",
		"/opt/kafka/bin/kafka-consumer-groups.sh",
		"--bootstrap-server", "localhost:9092",
		"--describe", "--group", group,
	}
	out, err := runCmd(ctx, cmd[0], cmd[1:]...)
	if err != nil {
		return -1, err
	}
	var total int64
	var seen bool
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, topic) {
			continue
		}
		fields := strings.Fields(line)
		// Header: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
		// We want the LAG column. It's typically index 5 when the GROUP column is present.
		if len(fields) < 6 {
			continue
		}
		lagStr := fields[5]
		if lagStr == "-" {
			continue
		}
		n, err := strconv.ParseInt(lagStr, 10, 64)
		if err != nil {
			continue
		}
		total += n
		seen = true
	}
	if !seen {
		return -1, nil
	}
	return total, nil
}
