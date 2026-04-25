package client

import (
	"context"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafkago.Writer
}

func NewKafkaProducer(brokers, topic string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(strings.Split(brokers, ",")...),
			Topic:        topic,
			Balancer:     &kafkago.Hash{},
			BatchTimeout: 100 * time.Millisecond,
			RequiredAcks: kafkago.RequireAll,
		},
	}
}

func (p *KafkaProducer) WriteJSON(ctx context.Context, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(key),
		Value: value,
	})
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
