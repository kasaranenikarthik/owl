// =============================================================================
// internal/kafka/producer.go
// =============================================================================

package kafka

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

type Producer struct {
	p      sarama.SyncProducer
	logger *slog.Logger
}

func NewProducer(brokers []string, logger *slog.Logger) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Return.Successes = true
	cfg.Producer.MaxMessageBytes = 2 * 1024 * 1024

	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	logger.Info("kafka producer connected", "brokers", brokers)
	return &Producer{p: p, logger: logger}, nil
}

func (p *Producer) Publish(topic, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	_, _, err = p.p.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	})
	return err
}

func (p *Producer) Close() error { return p.p.Close() }
