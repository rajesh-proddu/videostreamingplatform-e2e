package client

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// KafkaEvent is the envelope for events on the video-events and watch-events topics.
type KafkaEvent struct {
	Version   string          `json:"version"`
	Type      string          `json:"type"`
	Timestamp string          `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// KafkaConsumer reads events from a Kafka topic for test assertions.
type KafkaConsumer struct {
	reader *kafkago.Reader
}

func NewKafkaConsumer(brokers, topic, groupID string) *KafkaConsumer {
	return &KafkaConsumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:        strings.Split(brokers, ","),
			Topic:          topic,
			GroupID:        groupID,
			MinBytes:       1,
			MaxBytes:       10e6,
			StartOffset:    kafkago.LastOffset,
			CommitInterval: time.Second,
		}),
	}
}

// ReadEvents reads events from the topic until timeout, returning all collected events.
func (c *KafkaConsumer) ReadEvents(ctx context.Context, timeout time.Duration) ([]KafkaEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var events []KafkaEvent
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // timeout — return what we have
			}
			return events, err
		}
		var evt KafkaEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			continue
		}
		events = append(events, evt)
	}
	return events, nil
}

func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}
