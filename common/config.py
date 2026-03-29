import os
from dataclasses import dataclass


def _resolve_kafka_broker() -> str:
    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")

    # Docker service names do not resolve from a native Windows host process.
    if os.name == "nt" and broker.startswith("kafka:"):
        return broker.replace("kafka:", "localhost:", 1)

    return broker


@dataclass(frozen=True)
class Settings:
    kafka_broker: str = _resolve_kafka_broker()
    frames_topic: str = os.environ.get("FRAMES_TOPIC", "video.frames")
    detections_topic: str = os.environ.get("DETECTIONS_TOPIC", "video.detections")
    batch_size: int = int(os.environ.get("BATCH_SIZE", "8"))
    batch_timeout_ms: int = int(os.environ.get("BATCH_TIMEOUT_MS", "50"))
    yolo_model_path: str = os.environ.get("YOLO_MODEL_PATH", "yolov8n.pt")
    ingest_source: str = os.environ.get("INGEST_SOURCE", "0")
    frame_skip: int = int(os.environ.get("FRAME_SKIP", "1"))
    log_level: str = os.environ.get("LOG_LEVEL", "INFO")


settings = Settings()
