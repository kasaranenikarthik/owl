from prometheus_client import Counter, Histogram, Gauge, start_http_server

# --- Ingest metrics ---
frames_published_total = Counter(
    "frames_published_total",
    "Total frames published to Kafka",
    ["stream_id"],
)

# --- Inference metrics ---
frames_consumed_total = Counter(
    "frames_consumed_total",
    "Total frames consumed by inference workers",
    ["worker_id"],
)

inference_duration_seconds = Histogram(
    "inference_duration_seconds",
    "Time spent on model inference per batch",
    ["worker_id"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

batch_size_actual = Histogram(
    "batch_size_actual",
    "Actual micro-batch sizes used for inference",
    buckets=(1, 2, 4, 8, 16, 32),
)

detections_published_total = Counter(
    "detections_published_total",
    "Total detection messages published",
)

triton_requests_total = Counter(
    "triton_requests_total",
    "Total Triton inference requests attempted by the bridge",
    ["worker_id"],
)

triton_request_failures_total = Counter(
    "triton_request_failures_total",
    "Total Triton inference requests that failed in the bridge",
    ["worker_id", "reason"],
)

triton_inflight_requests = Gauge(
    "triton_inflight_requests",
    "Current number of in-flight Triton requests per bridge worker",
    ["worker_id"],
)

# --- Aggregator metrics ---
detections_consumed_total = Counter(
    "detections_consumed_total",
    "Total detection messages consumed by aggregator",
)

reorder_buffer_size = Gauge(
    "reorder_buffer_size",
    "Current size of per-stream reorder buffer",
    ["stream_id"],
)

websocket_connections = Gauge(
    "websocket_connections",
    "Number of active WebSocket connections",
)

# --- Shared ---
kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Kafka consumer lag per topic/partition",
    ["topic", "partition"],
)


def start_metrics_server(port: int):
    start_http_server(port)
