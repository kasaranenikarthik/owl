from pydantic import BaseModel


class BoundingBox(BaseModel):
    x1: float
    y1: float
    x2: float
    y2: float
    confidence: float
    class_id: int
    class_name: str


class DetectionMessage(BaseModel):
    stream_id: str
    frame_id: int
    timestamp: float  # original frame capture timestamp
    detections: list[BoundingBox]
    inference_time_ms: float

    def serialize(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def deserialize(cls, raw: bytes) -> "DetectionMessage":
        return cls.model_validate_json(raw)
