import logging

import cv2
import numpy as np
import torch

from schemas.frame_message import FrameMessage
from schemas.detection_message import BoundingBox, DetectionMessage

logger = logging.getLogger(__name__)

INPUT_SIZE = 640


class MicroBatcher:
    def __init__(self, device: torch.device):
        self.device = device

    def prepare_batch(self, frames: list[FrameMessage]) -> np.ndarray:
        """Decode JPEG frames and stack into a numpy array for YOLO."""
        images = []
        for frame in frames:
            raw = frame.decode_data()
            arr = np.frombuffer(raw, dtype=np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if img is None:
                logger.warning("Failed to decode frame %d", frame.frame_id)
                # Use a black frame as fallback
                img = np.zeros((INPUT_SIZE, INPUT_SIZE, 3), dtype=np.uint8)
            images.append(img)
        return images

    def unpack_results(
        self, results: list, frames: list[FrameMessage]
    ) -> list[DetectionMessage]:
        """Map YOLO results back to DetectionMessage per frame."""
        detections = []
        for result, frame in zip(results, frames):
            boxes = []
            if result.boxes is not None:
                for box in result.boxes:
                    xyxy = box.xyxy[0].cpu().numpy()
                    boxes.append(
                        BoundingBox(
                            x1=float(xyxy[0]),
                            y1=float(xyxy[1]),
                            x2=float(xyxy[2]),
                            y2=float(xyxy[3]),
                            confidence=float(box.conf[0]),
                            class_id=int(box.cls[0]),
                            class_name=result.names[int(box.cls[0])],
                        )
                    )
            detections.append(
                DetectionMessage(
                    stream_id=frame.stream_id,
                    frame_id=frame.frame_id,
                    timestamp=frame.timestamp,
                    detections=boxes,
                    inference_time_ms=result.speed.get("inference", 0.0),
                )
            )
        return detections
