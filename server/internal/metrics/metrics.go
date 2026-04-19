// =============================================================================
// internal/metrics/metrics.go
// =============================================================================

package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Gateway metrics
var (
	ActiveClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_active_clients",
		Help: "Number of connected WebSocket clients",
	})
	FramesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_frames_received_total",
		Help: "Total frames received from clients",
	})
	CacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_cache_hits_total",
		Help: "Frames served from similarity cache",
	})
	CacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_cache_misses_total",
		Help: "Frames sent to GPU for inference",
	})
	CacheLookupDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_cache_lookup_seconds",
		Help:    "Time spent checking and updating the similarity cache",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	})
	FramePublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_frame_publish_seconds",
		Help:    "Time spent publishing a frame to Kafka",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	})
	FramePublishFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_frame_publish_failures_total",
		Help: "Total Kafka publish failures for frames",
	})
	FramesDroppedStale = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_frames_dropped_stale_total",
		Help: "Results dropped because a newer result was already sent",
	})
	WebsocketWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_websocket_write_seconds",
		Help:    "Time spent writing a JSON message to the websocket client",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	})
	WebsocketWriteFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_websocket_write_failures_total",
		Help: "Total websocket write failures",
	})
	E2ELatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_e2e_latency_seconds",
		Help:    "End-to-end latency from frame received to result sent",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	})
)

// Inference metrics
var (
	FramesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "yolo_frames_processed_total",
		Help: "Total frames processed by YOLO",
	})
	FramesDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "yolo_frames_dropped_total",
		Help: "Frames dropped before inference",
	})
	FramesDroppedByReason = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "yolo_frames_dropped_reason_total",
		Help: "Frames dropped before inference, split by reason",
	}, []string{"reason"})
	WorkerQueueWaitDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "yolo_worker_queue_wait_seconds",
		Help:    "Time a frame spends waiting in the local worker queue before inference starts",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	})
	InferenceDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "yolo_inference_duration_seconds",
		Help:    "YOLO inference latency per frame (includes pre/post processing)",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	})
	DetectionsPerFrame = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "yolo_detections_per_frame",
		Help:    "Number of detections per frame",
		Buckets: prometheus.LinearBuckets(0, 5, 20),
	})
	WorkerPoolActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "yolo_worker_pool_active",
		Help: "Workers currently processing a frame",
	})
	WorkerPoolQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "yolo_worker_pool_queue_depth",
		Help: "Frames waiting in the worker pool buffer",
	})
	WorkerPoolCapacity = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "yolo_worker_pool_capacity",
		Help: "Total worker pool size",
	})
	ResultPublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "yolo_result_publish_seconds",
		Help:    "Time spent publishing detections back to Kafka",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	})
	ResultPublishFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "yolo_result_publish_failures_total",
		Help: "Total Kafka publish failures for detection results",
	})
)

// Kafka/Message queue metrics
var (
	// Gateway → Frames topic (producer)
	FramesPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_frames_published_total",
		Help: "Total frames published to Kafka frames topic",
	})

	// Gateway ← Detections topic (consumer)
	DetectionsConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_detections_consumed_total",
		Help: "Total detection results consumed from Kafka detections topic",
	})

	// Inference ← Frames topic (consumer)
	InferenceFramesConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "yolo_frames_consumed_total",
		Help: "Total frames consumed from Kafka frames topic (includes stale drops)",
	})

	// Inference → Detections topic (producer)
	InferenceResultsPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "yolo_results_published_total",
		Help: "Total detection results published to Kafka detections topic",
	})
)

func Serve(ctx context.Context, port string, logger *slog.Logger) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "ok") })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "ok") })

	srv := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutCtx)
	}()

	logger.Info("metrics server starting", "port", port)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}
