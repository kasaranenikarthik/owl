import logging
import os
import signal
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import settings
from common.kafka_utils import create_producer, delivery_callback
from common.metrics import (
    start_metrics_server,
    frames_consumed_total,
    inference_duration_seconds,
    batch_size_actual,
    detections_published_total,
)
from consumer import FrameConsumer
from batcher import MicroBatcher
from model import YOLOInferenceEngine

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("inference")

running = True


def main():
    global running

    worker_id = os.environ.get("WORKER_ID", socket.gethostname())
    logger.info("Starting inference worker: %s", worker_id)

    start_metrics_server(port=8002)

    engine = YOLOInferenceEngine(settings.yolo_model_path)
    batcher = MicroBatcher(device=engine.device)
    consumer = FrameConsumer()
    producer = create_producer(settings.kafka_broker)

    def handle_signal(signum, frame):
        global running
        logger.info("Received signal %d, shutting down", signum)
        running = False

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        "Consuming from %s (batch_size=%d, timeout=%dms)",
        settings.frames_topic,
        settings.batch_size,
        settings.batch_timeout_ms,
    )

    try:
        while running:
            frames = consumer.poll_batch(
                max_size=settings.batch_size,
                timeout_ms=settings.batch_timeout_ms,
            )

            if not frames:
                continue

            batch_size_actual.observe(len(frames))
            frames_consumed_total.labels(worker_id=worker_id).inc(len(frames))

            # Prepare and infer
            images = batcher.prepare_batch(frames)

            start = time.monotonic()
            results = engine.infer(images)
            elapsed = time.monotonic() - start

            inference_duration_seconds.labels(worker_id=worker_id).observe(elapsed)

            # Unpack and publish detections
            detection_msgs = batcher.unpack_results(results, frames)
            for det in detection_msgs:
                producer.produce(
                    topic=settings.detections_topic,
                    key=det.stream_id.encode("utf-8"),
                    value=det.serialize(),
                    callback=delivery_callback,
                )
                detections_published_total.inc()

            producer.poll(0)

            # Commit after successful processing
            consumer.commit()

    finally:
        logger.info("Shutting down inference worker")
        producer.flush(timeout=5)
        consumer.close()


if __name__ == "__main__":
    main()
