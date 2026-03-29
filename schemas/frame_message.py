import base64
import json

from pydantic import BaseModel


class FrameMessage(BaseModel):
    stream_id: str
    frame_id: int
    timestamp: float  # epoch seconds
    width: int
    height: int
    encoding: str = "jpeg"  # "jpeg" or "raw"
    data: str  # base64-encoded frame bytes

    def serialize(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def deserialize(cls, raw: bytes) -> "FrameMessage":
        return cls.model_validate_json(raw)

    @classmethod
    def from_frame_bytes(
        cls,
        stream_id: str,
        frame_id: int,
        timestamp: float,
        width: int,
        height: int,
        frame_bytes: bytes,
        encoding: str = "jpeg",
    ) -> "FrameMessage":
        return cls(
            stream_id=stream_id,
            frame_id=frame_id,
            timestamp=timestamp,
            width=width,
            height=height,
            encoding=encoding,
            data=base64.b64encode(frame_bytes).decode("ascii"),
        )

    def decode_data(self) -> bytes:
        return base64.b64decode(self.data)
