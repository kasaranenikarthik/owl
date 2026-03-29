import logging
import os
import time

import cv2
import numpy as np

from common.config import settings
from common.kafka_utils import create_producer, delivery_callback
from common.metrics import frames_published_total
from schemas.frame_message import FrameMessage

logger = logging.getLogger(__name__)


class FramePublisher:
    def __init__(self, source: str, stream_id: str = "stream-0"):
        self.stream_id = stream_id
        self.frame_count = 0
        self.source = source

        # Treat numeric sources as webcam indexes and open them with Windows-
        # friendly fallbacks when needed.
        self.source_val = self._parse_source(source)
        self.is_live_source = isinstance(self.source_val, int)
        self.cap = self._open_capture()

        self.fps = self._resolve_fps()
        logger.info(
            "Opened video source=%s stream_id=%s fps=%.1f",
            source, self.stream_id, self.fps,
        )

        self.producer = create_producer(settings.kafka_broker)

    def run(self):
        frame_skip = settings.frame_skip
        logger.info("Starting frame publishing (frame_skip=%d)", frame_skip)

        try:
            while True:
                ret, frame = self.cap.read()
                if not ret:
                    self._handle_read_failure()
                    continue

                self.frame_count += 1
                if self.frame_count % frame_skip != 0:
                    continue

                frame_id = self.frame_count // frame_skip
                timestamp = time.time()
                h, w = frame.shape[:2]

                # JPEG encode
                success, buf = cv2.imencode(".jpg", frame)
                if not success:
                    logger.warning("Failed to encode frame %d", frame_id)
                    continue

                msg = FrameMessage.from_frame_bytes(
                    stream_id=self.stream_id,
                    frame_id=frame_id,
                    timestamp=timestamp,
                    width=w,
                    height=h,
                    frame_bytes=buf.tobytes(),
                )

                self.producer.produce(
                    topic=settings.frames_topic,
                    key=self.stream_id.encode("utf-8"),
                    value=msg.serialize(),
                    callback=delivery_callback,
                )
                self.producer.poll(0)

                frames_published_total.labels(stream_id=self.stream_id).inc()

                # Pace to source FPS
                time.sleep(1.0 / self.fps)

        except KeyboardInterrupt:
            logger.info("Shutting down frame publisher")
        finally:
            self.shutdown()

    def shutdown(self):
        logger.info("Flushing producer and releasing capture")
        self.producer.flush(timeout=5)
        self.cap.release()

    @staticmethod
    def _parse_source(source: str):
        try:
            return int(source)
        except ValueError:
            return source

    def _open_capture(self):
        if not self.is_live_source:
            cap = cv2.VideoCapture(self.source_val)
            if cap.isOpened():
                return cap
            raise RuntimeError(f"Cannot open video source: {self.source}")

        attempts = self._camera_backend_attempts()
        for backend_name, backend_id in attempts:
            logger.info(
                "Trying webcam source=%s with backend=%s",
                self.source_val,
                backend_name,
            )
            cap = cv2.VideoCapture(self.source_val, backend_id)
            if cap.isOpened():
                logger.info("Opened webcam with backend=%s", backend_name)
                return cap
            cap.release()

        raise RuntimeError(
            f"Cannot open webcam source {self.source}. "
            "Set OPENCV_CAMERA_BACKEND to one of: any, dshow, msmf"
        )

    def _resolve_fps(self) -> float:
        fps = self.cap.get(cv2.CAP_PROP_FPS)
        if fps and fps > 1:
            return fps
        return 30.0

    def _handle_read_failure(self):
        if self.is_live_source:
            logger.warning(
                "Webcam frame read failed for source=%s; retrying",
                self.source,
            )
            time.sleep(0.1)
            return

        logger.warning("End of file source reached for %s; rewinding", self.source)
        if not self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0):
            logger.warning("Re-open file source after rewind failure: %s", self.source)
            self.cap.release()
            self.cap = self._open_capture()
            self.fps = self._resolve_fps()

    @staticmethod
    def _camera_backend_attempts():
        backend_map = {"any": cv2.CAP_ANY}

        if hasattr(cv2, "CAP_DSHOW"):
            backend_map["dshow"] = cv2.CAP_DSHOW
        if hasattr(cv2, "CAP_MSMF"):
            backend_map["msmf"] = cv2.CAP_MSMF

        requested = os.environ.get("OPENCV_CAMERA_BACKEND", "").strip().lower()
        if requested:
            backend_id = backend_map.get(requested)
            if backend_id is None:
                logger.warning(
                    "Unknown OPENCV_CAMERA_BACKEND=%s; falling back to auto",
                    requested,
                )
            else:
                return [(requested.upper(), backend_id)]

        attempts = [("ANY", cv2.CAP_ANY)]
        for backend_name in ("dshow", "msmf"):
            backend_id = backend_map.get(backend_name)
            if backend_id is not None:
                attempts.append((backend_name.upper(), backend_id))
        return attempts
