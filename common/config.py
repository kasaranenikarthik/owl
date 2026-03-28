import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_broker: str = os.environ.get("KAFKA_BROKER", "localhost:9092")
    frames_topic: str = os.environ.get("FRAMES_TOPIC", "video.frames")
    detections_topic: str = os.environ.get("DETECTIONS_TOPIC", "video.detections")
    batch_size: int = int(os.environ.get("BATCH_SIZE", "8"))
    batch_timeout_ms: int = int(os.environ.get("BATCH_TIMEOUT_MS", "50"))
    yolo_model_path: str = os.environ.get("YOLO_MODEL_PATH", "yolov8n.pt")
    ingest_source: str = os.environ.get("INGEST_SOURCE", "0")
    frame_skip: int = int(os.environ.get("FRAME_SKIP", "1"))
    log_level: str = os.environ.get("LOG_LEVEL", "INFO")


settings = Settings()
