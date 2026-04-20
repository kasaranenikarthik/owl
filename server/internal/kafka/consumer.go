// =============================================================================
// internal/kafka/consumer.go
// =============================================================================

package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

type Message struct {
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

type MessageHandler func(ctx context.Context, msg Message) error

type Consumer struct {
	group   sarama.ConsumerGroup
	topics  []string
	handler MessageHandler
	logger  *slog.Logger
}

func NewConsumer(brokers []string, groupID string, topics []string, handler MessageHandler, logger *slog.Logger) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	g, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("consumer group: %w", err)
	}
	logger.Info("kafka consumer created", "group", groupID, "topics", topics)
	return &Consumer{group: g, topics: topics, handler: handler, logger: logger}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	h := &groupHandler{handler: c.handler, logger: c.logger}
	for {
		select {
		case <-ctx.Done():
			return c.group.Close()
		default:
			if err := c.group.Consume(ctx, c.topics, h); err != nil {
				c.logger.Error("consume error", "error", err)
			}
		}
	}
}

type groupHandler struct {
	handler MessageHandler
	logger  *slog.Logger
}

func (h *groupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *groupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *groupHandler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for msg := range c.Messages() {
		headers := make(map[string][]byte, len(msg.Headers))
		for _, header := range msg.Headers {
			headers[string(header.Key)] = header.Value
		}
		if err := h.handler(s.Context(), Message{
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: headers,
		}); err != nil {
			h.logger.Error("handler error", "topic", msg.Topic, "error", err)
			continue
		}
		s.MarkMessage(msg, "")
	}
	return nil
}
