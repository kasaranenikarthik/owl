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
from model import create_inference_engine

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("inference")

running = True


def main():
    global running

    worker_id = os.environ.get("WORKER_ID", socket.gethostname())
    logger.info(
        "Starting inference worker: %s (backend=%s)",
        worker_id,
        settings.inference_backend,
    )

    start_metrics_server(port=8002)

    engine = create_inference_engine(worker_id=worker_id)
    batcher = MicroBatcher()
    consumer = FrameConsumer()
    producer = create_producer(settings.kafka_broker)

    if settings.use_triton:
        max_size = settings.triton_max_inflight
        timeout_ms = settings.triton_collect_timeout_ms
    else:
        max_size = settings.batch_size
        timeout_ms = settings.batch_timeout_ms

    def handle_signal(signum, frame):
        global running
        logger.info("Received signal %d, shutting down", signum)
        running = False

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        "Consuming from %s (window_size=%d, timeout=%dms)",
        settings.frames_topic,
        max_size,
        timeout_ms,
    )

    try:
        while running:
            frames = consumer.poll_batch(
                max_size=max_size,
                timeout_ms=timeout_ms,
            )

            if not frames:
                continue

            batch_size_actual.observe(len(frames))
            frames_consumed_total.labels(worker_id=worker_id).inc(len(frames))

            # Prepare and infer
            images = batcher.prepare_batch(frames)

            start = time.monotonic()
            try:
                outputs = engine.infer(images)
            except Exception:
                logger.exception(
                    "Inference failed for %d frame(s); leaving offsets uncommitted",
                    len(frames),
                )
                continue
            elapsed = time.monotonic() - start

            if len(outputs) != len(frames):
                logger.error(
                    "Inference output count mismatch: expected %d, got %d; "
                    "leaving offsets uncommitted",
                    len(frames),
                    len(outputs),
                )
                continue

            inference_duration_seconds.labels(worker_id=worker_id).observe(elapsed)

            # Unpack and publish detections
            detection_msgs = batcher.unpack_results(outputs, frames)
            for det in detection_msgs:
                producer.produce(
                    topic=settings.detections_topic,
                    key=det.stream_id.encode("utf-8"),
                    value=det.serialize(),
                    callback=delivery_callback,
                )
                detections_published_total.inc()

            remaining = producer.flush(timeout=5)
            if remaining:
                logger.error(
                    "Failed to publish %d detection message(s); leaving offsets uncommitted",
                    remaining,
                )
                continue

            # Commit after successful processing
            consumer.commit()

    finally:
        logger.info("Shutting down inference worker")
        engine.close()
        producer.flush(timeout=5)
        consumer.close()


if __name__ == "__main__":
    main()
