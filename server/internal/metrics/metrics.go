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
	FramesDroppedStale = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_frames_dropped_stale_total",
		Help: "Results dropped because a newer result was already sent",
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
		Help: "Stale frames dropped before inference",
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
