import os
from dataclasses import dataclass


def _resolve_kafka_broker() -> str:
    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")

    # Docker service names do not resolve from a native Windows host process.
    if os.name == "nt" and broker.startswith("kafka:"):
        return broker.replace("kafka:", "localhost:", 1)

    return broker


def _resolve_triton_http_url() -> str:
    url = os.environ.get("TRITON_HTTP_URL", "http://localhost:8004")

    # Docker service names do not resolve from a native Windows host process.
    if os.name == "nt" and "://triton:" in url:
        return url.replace("://triton:", "://localhost:", 1)

    return url


@dataclass(frozen=True)
class Settings:
    kafka_broker: str = _resolve_kafka_broker()
    frames_topic: str = os.environ.get("FRAMES_TOPIC", "video.frames")
    detections_topic: str = os.environ.get("DETECTIONS_TOPIC", "video.detections")
    inference_backend: str = os.environ.get("INFERENCE_BACKEND", "triton").lower()
    batch_size: int = int(os.environ.get("BATCH_SIZE", "8"))
    batch_timeout_ms: int = int(os.environ.get("BATCH_TIMEOUT_MS", "50"))
    yolo_model_path: str = os.environ.get("YOLO_MODEL_PATH", "yolov8n.pt")
    triton_http_url: str = _resolve_triton_http_url()
    triton_model_name: str = os.environ.get("TRITON_MODEL_NAME", "yolo")
    triton_max_inflight: int = int(os.environ.get("TRITON_MAX_INFLIGHT", "8"))
    triton_collect_timeout_ms: int = int(
        os.environ.get("TRITON_COLLECT_TIMEOUT_MS", "50")
    )
    triton_request_timeout_ms: int = int(
        os.environ.get("TRITON_REQUEST_TIMEOUT_MS", "2000")
    )
    triton_dynamic_batch_delay_us: int = int(
        os.environ.get("TRITON_DYNAMIC_BATCH_DELAY_US", "50000")
    )
    triton_model_repository: str = os.environ.get("TRITON_MODEL_REPOSITORY", "/models")
    ingest_source: str = os.environ.get("INGEST_SOURCE", "0")
    frame_skip: int = int(os.environ.get("FRAME_SKIP", "1"))
    log_level: str = os.environ.get("LOG_LEVEL", "INFO")

    @property
    def use_triton(self) -> bool:
        return self.inference_backend == "triton"


settings = Settings()
