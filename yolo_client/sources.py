
# =============================================================================
# yolo_client/sources.py
# =============================================================================

"""Video frame sources — webcam, video file, RTSP stream."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from pathlib import Path

import cv2
import numpy as np


class FrameSource(ABC):
    """Base class for video frame sources."""

    @abstractmethod
    def read(self) -> tuple[bool, np.ndarray | None]:
        """Read next frame. Returns (success, BGR frame)."""
        ...

    @abstractmethod
    def release(self):
        ...

    @property
    @abstractmethod
    def fps(self) -> float:
        ...


class WebcamSource(FrameSource):
    """Captures frames from a local webcam."""

    def __init__(self, device_id: int = 0, width: int = 640, height: int = 480):
        self._cap = cv2.VideoCapture(device_id)
        self._cap.set(cv2.CAP_PROP_FRAME_WIDTH, width)
        self._cap.set(cv2.CAP_PROP_FRAME_HEIGHT, height)

        if not self._cap.isOpened():
            raise RuntimeError(f"Cannot open webcam device {device_id}")

    def read(self):
        return self._cap.read()

    def release(self):
        self._cap.release()

    @property
    def fps(self) -> float:
        return self._cap.get(cv2.CAP_PROP_FPS) or 30.0


class VideoFileSource(FrameSource):
    """Reads frames from a video file."""

    def __init__(self, path: str | Path):
        self._path = str(path)
        self._cap = cv2.VideoCapture(self._path)
        if not self._cap.isOpened():
            raise RuntimeError(f"Cannot open video: {path}")

    def read(self):
        ret, frame = self._cap.read()
        if not ret:
            return False, None
        return True, frame

    def release(self):
        self._cap.release()

    @property
    def fps(self) -> float:
        return self._cap.get(cv2.CAP_PROP_FPS) or 30.0

    @property
    def total_frames(self) -> int:
        return int(self._cap.get(cv2.CAP_PROP_FRAME_COUNT))


class RTSPSource(FrameSource):
    """Reads frames from an RTSP/RTMP stream."""

    def __init__(self, url: str):
        self._url = url
        self._cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
        if not self._cap.isOpened():
            raise RuntimeError(f"Cannot connect to stream: {url}")

    def read(self):
        return self._cap.read()

    def release(self):
        self._cap.release()

    @property
    def fps(self) -> float:
        return self._cap.get(cv2.CAP_PROP_FPS) or 30.0


def encode_jpeg(frame: np.ndarray, quality: int = 80) -> bytes:
    """Encode a BGR frame as JPEG bytes."""
    ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    if not ok:
        raise RuntimeError("JPEG encode failed")
    return buf.tobytes()
