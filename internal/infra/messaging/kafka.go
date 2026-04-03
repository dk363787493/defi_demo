package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
)

// Producer wraps a kafka.Writer for sending messages.
type Producer struct {
	writer *kafka.Writer
}

func NewProducer(cfg config.KafkaConfig, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	return &Producer{writer: w}
}

// Publish serializes value to JSON and sends it with the given key.
func (p *Producer) Publish(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal kafka message: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: data,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// Consumer wraps a kafka.Reader for receiving messages.
type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(cfg config.KafkaConfig, topic string, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	return &Consumer{reader: r}
}

// Read blocks until a message is available, then returns it.
func (c *Consumer) Read(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
