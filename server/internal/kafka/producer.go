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

type Header struct {
	Key   string
	Value []byte
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
	return p.PublishBytes(topic, key, data, nil)
}

func (p *Producer) PublishBytes(topic, key string, value []byte, headers []Header) error {
	recordHeaders := make([]sarama.RecordHeader, 0, len(headers))
	for _, header := range headers {
		recordHeaders = append(recordHeaders, sarama.RecordHeader{
			Key:   []byte(header.Key),
			Value: header.Value,
		})
	}
	_, _, err := p.p.SendMessage(&sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.StringEncoder(key),
		Value:   sarama.ByteEncoder(value),
		Headers: recordHeaders,
	})
	return err
}

func (p *Producer) Close() error { return p.p.Close() }
