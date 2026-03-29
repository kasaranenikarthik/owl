// =============================================================================
// internal/similarity/cache.go
// =============================================================================

package similarity

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"yolo-realtime/internal/models"
)

type Cache struct {
	client    *redis.Client
	ttl       time.Duration
	threshold int
	logger    *slog.Logger
}

func NewCache(addr string, ttlSeconds, threshold int, logger *slog.Logger) (*Cache, error) {
	c := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     50,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	logger.Info("similarity cache connected", "addr", addr, "threshold", threshold)
	return &Cache{
		client:    c,
		ttl:       time.Duration(ttlSeconds) * time.Second,
		threshold: threshold,
		logger:    logger,
	}, nil
}

func (c *Cache) CheckAndUpdate(ctx context.Context, clientID string, frameHash uint64, frameData []byte) (*models.InternalResult, bool, error) {
	hashKey := "hash:" + clientID
	resultKey := "result:" + clientID

	lastHashStr, err := c.client.Get(ctx, hashKey).Result()
	if err == nil {
		lastHash, _ := strconv.ParseUint(lastHashStr, 10, 64)
		dist := HammingDistance(frameHash, lastHash)

		if dist <= c.threshold {
			resultData, err := c.client.Get(ctx, resultKey).Result()
			if err == nil {
				var result models.InternalResult
				if json.Unmarshal([]byte(resultData), &result) == nil {
					return &result, true, nil
				}
			}
		}
	}

	pipe := c.client.Pipeline()
	pipe.Set(ctx, hashKey, strconv.FormatUint(frameHash, 10), c.ttl)
	pipe.Exec(ctx)

	return nil, false, nil
}

func (c *Cache) StoreResult(ctx context.Context, clientID string, result models.InternalResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, "result:"+clientID, string(data), c.ttl).Err()
}

func (c *Cache) RemoveClient(ctx context.Context, clientID string) {
	c.client.Del(ctx, "hash:"+clientID, "result:"+clientID)
}

func (c *Cache) Close() error { return c.client.Close() }
