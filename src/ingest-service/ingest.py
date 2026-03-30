"""
Ingest Service — reads a video file, extracts frames at a target FPS,
and publishes each frame as a Kafka message to the "raw-frames" topic.

Message format:
  key:     stream_id (bytes)
  value:   JPEG-encoded frame (binary)
  headers: frame_id, timestamp, stream_id
"""

import os
import time
import logging

import cv2
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW_FRAMES = os.environ.get("TOPIC_RAW_FRAMES", "raw-frames")
VIDEO_SOURCE = os.environ.get("VIDEO_SOURCE", "/videos/test.mp4")
STREAM_ID = os.environ.get("STREAM_ID", "stream-0")
TARGET_FPS = int(os.environ.get("TARGET_FPS", "10"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [INGEST] %(message)s")
logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Frame delivery failed: {err}")


def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "message.timeout.ms": 10000,
        "message.max.bytes": 2_000_000,
        "linger.ms": 10,
    })


def wait_for_kafka(max_retries=30, delay=2):
    for attempt in range(max_retries):
        try:
            p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "socket.timeout.ms": 2000})
            p.list_topics(timeout=5)
            logger.info("Kafka is ready")
            return
        except Exception:
            logger.info(f"Waiting for Kafka... ({attempt + 1}/{max_retries})")
            time.sleep(delay)
    raise RuntimeError("Kafka not available after retries")


def run_ingest():
    wait_for_kafka()
    producer = create_producer()
    logger.info(f"Starting: source={VIDEO_SOURCE}, stream={STREAM_ID}, target_fps={TARGET_FPS}")

    frame_interval = 1.0 / TARGET_FPS
    frame_id = 0

    while True:
        cap = cv2.VideoCapture(VIDEO_SOURCE)
        if not cap.isOpened():
            logger.error(f"Cannot open video: {VIDEO_SOURCE}")
            time.sleep(5)
            continue

        source_fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        logger.info(f"Video opened: {source_fps:.1f} fps, {total_frames} frames, publishing at {TARGET_FPS} fps")

        while True:
            loop_start = time.time()

            ret, frame = cap.read()
            if not ret:
                logger.info("Video ended, looping...")
                break

            success, jpeg_buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
            if not success:
                continue

            jpeg_bytes = jpeg_buffer.tobytes()

            producer.produce(
                topic=TOPIC_RAW_FRAMES,
                value=jpeg_bytes,
                key=STREAM_ID.encode("utf-8"),
                headers={
                    "frame_id": str(frame_id).encode("utf-8"),
                    "timestamp": str(time.time()).encode("utf-8"),
                    "stream_id": STREAM_ID.encode("utf-8"),
                },
                callback=delivery_report,
            )
            producer.poll(0)

            frame_id += 1
            if frame_id % 100 == 0:
                logger.info(f"Published {frame_id} frames | size={len(jpeg_bytes)} bytes")

            elapsed = time.time() - loop_start
            sleep_time = frame_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

        cap.release()


if __name__ == "__main__":
    run_ingest()
