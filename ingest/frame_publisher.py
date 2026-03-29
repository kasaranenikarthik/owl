import logging
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

        # Try to parse as int (webcam index), otherwise treat as URL/path
        try:
            source_val = int(source)
        except ValueError:
            source_val = source

        self.cap = cv2.VideoCapture(source_val)
        if not self.cap.isOpened():
            raise RuntimeError(f"Cannot open video source: {source}")

        self.fps = self.cap.get(cv2.CAP_PROP_FPS) or 30.0
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
                    logger.warning("End of video stream or read failure, looping...")
                    self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
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
