import logging

from confluent_kafka import Producer, Consumer

logger = logging.getLogger(__name__)


def create_producer(broker: str, **overrides) -> Producer:
    config = {
        "bootstrap.servers": broker,
        "linger.ms": 5,
        "batch.num.messages": 1000,
        "compression.type": "lz4",
        "message.max.bytes": 10_485_760,
    }
    config.update(overrides)
    return Producer(config)


def create_consumer(
    broker: str, group_id: str, topics: list[str], **overrides
) -> Consumer:
    config = {
        "bootstrap.servers": broker,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "max.partition.fetch.bytes": 10_485_760,
    }
    config.update(overrides)
    consumer = Consumer(config)
    consumer.subscribe(topics)
    return consumer


def delivery_callback(err, msg):
    if err is not None:
        logger.error("Delivery failed for %s: %s", msg.key(), err)
    else:
        logger.debug(
            "Delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset()
        )
