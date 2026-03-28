import logging
import time

from confluent_kafka import KafkaError

from common.kafka_utils import create_consumer
from common.config import settings
from schemas.frame_message import FrameMessage

logger = logging.getLogger(__name__)


class FrameConsumer:
    def __init__(self, group_id: str = "inference-workers"):
        self.consumer = create_consumer(
            broker=settings.kafka_broker,
            group_id=group_id,
            topics=[settings.frames_topic],
        )

    def poll_batch(self, max_size: int, timeout_ms: int) -> list[FrameMessage]:
        """Collect up to max_size frames within timeout_ms."""
        batch = []
        deadline = time.monotonic() + timeout_ms / 1000.0

        while len(batch) < max_size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            msg = self.consumer.poll(timeout=remaining)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue

            try:
                frame = FrameMessage.deserialize(msg.value())
                batch.append(frame)
            except Exception:
                logger.exception("Failed to deserialize frame message")

        return batch

    def commit(self):
        self.consumer.commit(asynchronous=False)

    def close(self):
        self.consumer.close()
