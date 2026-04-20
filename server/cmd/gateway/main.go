// =============================================================================
// cmd/gateway/main.go
// =============================================================================

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"yolo-realtime/internal/config"
	"yolo-realtime/internal/kafka"
	"yolo-realtime/internal/metrics"
	"yolo-realtime/internal/models"
	"yolo-realtime/internal/shutdown"
	"yolo-realtime/internal/similarity"
)

// ---------------------------------------------------------------------------
// Client registry
// ---------------------------------------------------------------------------

type client struct {
	conn         *websocket.Conn
	mu           sync.Mutex
	frameSeq     atomic.Uint64 // gateway-assigned, monotonically increasing
	latestSentID atomic.Uint64 // highest frame_id whose result was sent to client
	pending      sync.Map      // frame_id → time.Time (for e2e latency)
}

type registry struct {
	mu      sync.RWMutex
	clients map[string]*client
}

func newRegistry() *registry {
	return &registry{clients: make(map[string]*client)}
}

func (r *registry) Add(id string, conn *websocket.Conn) *client {
	c := &client{conn: conn}
	r.mu.Lock()
	r.clients[id] = c
	r.mu.Unlock()
	metrics.ActiveClients.Inc()
	return c
}

func (r *registry) Remove(id string) {
	r.mu.Lock()
	if c, ok := r.clients[id]; ok {
		c.conn.Close()
		delete(r.clients, id)
	}
	r.mu.Unlock()
	metrics.ActiveClients.Dec()
}

func (r *registry) Get(id string) (*client, bool) {
	r.mu.RLock()
	c, ok := r.clients[id]
	r.mu.RUnlock()
	return c, ok
}

func (c *client) SendJSON(v any) error {
	start := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.conn.WriteJSON(v)
	metrics.WebsocketWriteDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.WebsocketWriteFailures.Inc()
	}
	return err
}

// ---------------------------------------------------------------------------
// Main — reads as a high-level table of contents
// ---------------------------------------------------------------------------

func main() {
	logger := newLogger()
	cfg := config.Load()

	ctx, cancel := shutdown.WithSignal(context.Background(), logger)
	defer cancel()

	simCache := mustCreateSimilarityCache(cfg, logger)
	if simCache != nil {
		defer simCache.Close()
	}

	producer := mustCreateProducer(cfg, logger)
	defer producer.Close()

	reg := newRegistry()
	startDetectionsConsumer(ctx, cfg, logger, simCache, reg)

	go func() { metrics.Serve(ctx, cfg.MetricsPort, logger) }()

	mux := buildMux(ctx, cfg, logger, simCache, producer, reg)
	runHTTPServer(ctx, cfg, logger, mux)
}

// ---------------------------------------------------------------------------
// Initialization helpers
// ---------------------------------------------------------------------------

func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func mustCreateSimilarityCache(cfg config.Config, logger *slog.Logger) *similarity.Cache {
	if cfg.SimilarityThreshold < 0 {
		logger.Info("similarity cache disabled", "threshold", cfg.SimilarityThreshold)
		return nil
	}
	simCache, err := similarity.NewCache(cfg.RedisAddr, cfg.CacheTTLSeconds, cfg.SimilarityThreshold, logger)
	if err != nil {
		logger.Error("redis connect failed", "error", err)
		os.Exit(1)
	}
	return simCache
}

func mustCreateProducer(cfg config.Config, logger *slog.Logger) *kafka.Producer {
	producer, err := kafka.NewProducer(cfg.KafkaBrokers, logger)
	if err != nil {
		logger.Error("kafka producer failed", "error", err)
		os.Exit(1)
	}
	return producer
}

// ---------------------------------------------------------------------------
// Detection result consumer — routes Kafka results to WebSocket clients
// ---------------------------------------------------------------------------

func startDetectionsConsumer(ctx context.Context, cfg config.Config, logger *slog.Logger, simCache *similarity.Cache, reg *registry) {
	detConsumer, err := kafka.NewConsumer(
		cfg.KafkaBrokers,
		"gateway-detections",
		[]string{cfg.DetectionsTopic},
		func(ctx context.Context, msg kafka.Message) error {
			return handleDetectionResult(ctx, msg.Value, simCache, reg)
		},
		logger,
	)
	if err != nil {
		logger.Error("detection consumer failed", "error", err)
		os.Exit(1)
	}

	go func() {
		if err := detConsumer.Run(ctx); err != nil {
			logger.Error("consumer exited", "error", err)
		}
	}()
}

func handleDetectionResult(ctx context.Context, value []byte, simCache *similarity.Cache, reg *registry) error {
	metrics.DetectionsConsumed.Inc()

	var internal models.InternalResult
	if err := json.Unmarshal(value, &internal); err != nil {
		return fmt.Errorf("unmarshal detection: %w", err)
	}

	if simCache != nil {
		simCache.StoreResult(ctx, internal.ClientID, internal)
	}

	c, ok := reg.Get(internal.ClientID)
	if !ok {
		return nil // client disconnected
	}

	// Drop stale: if we already sent a newer result, discard this one
	if internal.FrameID <= c.latestSentID.Load() {
		metrics.FramesDroppedStale.Inc()
		c.pending.Delete(internal.FrameID)
		return nil
	}
	c.latestSentID.Store(internal.FrameID)

	if sentAt, loaded := c.pending.LoadAndDelete(internal.FrameID); loaded {
		metrics.E2ELatency.Observe(time.Since(sentAt.(time.Time)).Seconds())
	}

	clientResult := models.ClientResult{
		FrameID:     internal.FrameID,
		Detections:  internal.Detections,
		InferenceMs: internal.InferenceMs,
		FromCache:   false,
		Timestamp:   internal.Timestamp,
	}

	return c.SendJSON(clientResult)
}

// ---------------------------------------------------------------------------
// HTTP routing
// ---------------------------------------------------------------------------

func buildMux(ctx context.Context, cfg config.Config, logger *slog.Logger, simCache *similarity.Cache, producer *kafka.Producer, reg *registry) *http.ServeMux {
	upgrader := websocket.Upgrader{
		CheckOrigin:     func(r *http.Request) bool { return true },
		ReadBufferSize:  256 * 1024,
		WriteBufferSize: 16 * 1024,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		handleWS(ctx, cfg, logger, simCache, producer, reg, upgrader, w, r)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})

	return mux
}

// ---------------------------------------------------------------------------
// WebSocket handler
// ---------------------------------------------------------------------------

func handleWS(ctx context.Context, cfg config.Config, logger *slog.Logger, simCache *similarity.Cache, producer *kafka.Producer, reg *registry, upgrader websocket.Upgrader, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("ws upgrade failed", "error", err)
		return
	}

	clientID := uuid.New().String()
	c := reg.Add(clientID, conn)
	logger.Info("client connected", "client_id", clientID)

	c.SendJSON(map[string]string{"type": "connected", "client_id": clientID})

	defer func() {
		reg.Remove(clientID)
		if simCache != nil {
			simCache.RemoveClient(ctx, clientID)
		}
		logger.Info("client disconnected", "client_id", clientID)
	}()

	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		if msgType != websocket.BinaryMessage || len(data) == 0 {
			continue
		}

		if err := processFrame(ctx, cfg, logger, simCache, producer, c, clientID, data); err != nil {
			logger.Warn("frame processing failed", "client_id", clientID, "error", err)
		}
	}
}

// ---------------------------------------------------------------------------
// Frame processing — cache check → Kafka publish
// ---------------------------------------------------------------------------

func processFrame(ctx context.Context, cfg config.Config, logger *slog.Logger, simCache *similarity.Cache, producer *kafka.Producer, c *client, clientID string, data []byte) error {
	metrics.FramesReceived.Inc()
	frameID := c.frameSeq.Add(1)
	now := time.Now()

	if simCache != nil {
		cacheStart := time.Now()
		frameHash, hashErr := similarity.AverageHash(data)
		if hashErr == nil {
			cached, hit, err := simCache.CheckAndUpdate(ctx, clientID, frameHash, data)
			metrics.CacheLookupDuration.Observe(time.Since(cacheStart).Seconds())
			if err != nil {
				logger.Warn("similarity cache check failed", "client_id", clientID, "error", err)
			}
			if hit && cached != nil {
				metrics.CacheHits.Inc()
				c.latestSentID.Store(frameID)
				clientResult := models.ClientResult{
					FrameID:     frameID,
					Detections:  cached.Detections,
					InferenceMs: cached.InferenceMs,
					FromCache:   true,
					Timestamp:   now,
				}
				if err := c.SendJSON(clientResult); err != nil {
					return fmt.Errorf("send cached result: %w", err)
				}
				return nil
			}
		}
		metrics.CacheLookupDuration.Observe(time.Since(cacheStart).Seconds())
	}

	metrics.CacheMisses.Inc()
	c.pending.Store(frameID, now)

	publishStart := time.Now()
	if err := producer.PublishBytes(
		cfg.FramesTopic,
		clientID,
		data,
		[]kafka.Header{
			{Key: "frame_id", Value: []byte(fmt.Sprintf("%d", frameID))},
			{Key: "timestamp_unix_nano", Value: []byte(fmt.Sprintf("%d", now.UnixNano()))},
		},
	); err != nil {
		metrics.FramePublishDuration.Observe(time.Since(publishStart).Seconds())
		metrics.FramePublishFailures.Inc()
		return fmt.Errorf("kafka publish: %w", err)
	}
	metrics.FramePublishDuration.Observe(time.Since(publishStart).Seconds())
	metrics.FramesPublished.Inc()

	return nil
}

// ---------------------------------------------------------------------------
// HTTP server lifecycle
// ---------------------------------------------------------------------------

func runHTTPServer(ctx context.Context, cfg config.Config, logger *slog.Logger, handler http.Handler) {
	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: handler,
	}

	go func() {
		<-ctx.Done()
		shutCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		srv.Shutdown(shutCtx)
	}()

	logger.Info("gateway starting", "port", cfg.HTTPPort)
	logger.Info("frame transport mode", "service", "gateway", "frames_topic_encoding", "raw_bytes_with_headers_v1")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}
