"""
Aggregator Service - consumes detections from Kafka, maintains a
per-stream reorder buffer, and exposes results via a REST API.

The reorder buffer works like TCP's receive window:
  - Tracks "next expected" frame_id per stream
  - Buffers out-of-order results
  - Emits in-order when the expected frame arrives
  - Force-advances if the window fills up (frame lost)

REST endpoints:
  GET /health            — health check
  GET /streams           — list all streams with stats
  GET /streams/{id}      — latest detections for one stream
  GET /stats             — overall pipeline statistics
"""

import os
import time
import json
import logging
import threading

from flask import Flask, jsonify
from confluent_kafka import Consumer, Producer, KafkaError

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_DETECTIONS = os.environ.get("TOPIC_DETECTIONS", "detections")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "aggregator")
REORDER_WINDOW = int(os.environ.get("REORDER_WINDOW", "30"))
API_PORT = int(os.environ.get("API_PORT", "8080"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [AGGREGATOR] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)


class ReorderBuffer:
    def __init__(self, stream_id, window_size=30):
        self.stream_id = stream_id
        self.window_size = window_size
        self.buffer = {}
        self.next_expected = 0
        self.emitted = []
        self.max_emitted = 200
        self.dropped_count = 0
        self.reordered_count = 0
        self.total_received = 0
        self.lock = threading.Lock()

    def add(self, result):
        with self.lock:
            frame_id = result["frame_id"]
            self.total_received += 1

            if frame_id < self.next_expected:
                self.dropped_count += 1
                return

            if frame_id != self.next_expected:
                self.reordered_count += 1

            self.buffer[frame_id] = result

            while self.next_expected in self.buffer:
                self.emitted.append(self.buffer.pop(self.next_expected))
                self.next_expected += 1

            if len(self.emitted) > self.max_emitted:
                self.emitted = self.emitted[-self.max_emitted:]

            if len(self.buffer) > self.window_size:
                min_buffered = min(self.buffer.keys())
                while self.next_expected <= min_buffered:
                    self.dropped_count += 1
                    self.next_expected += 1

    def get_latest(self, n=10):
        with self.lock:
            return list(self.emitted[-n:])

    def get_stats(self):
        with self.lock:
            return {
                "stream_id": self.stream_id,
                "total_received": self.total_received,
                "total_emitted": len(self.emitted),
                "next_expected": self.next_expected,
                "buffer_size": len(self.buffer),
                "dropped_count": self.dropped_count,
                "reordered_count": self.reordered_count,
            }


stream_buffers = {}
pipeline_stats = {"start_time": time.time(), "total_messages_consumed": 0}
stats_lock = threading.Lock()


def get_or_create_buffer(stream_id):
    if stream_id not in stream_buffers:
        stream_buffers[stream_id] = ReorderBuffer(stream_id, REORDER_WINDOW)
    return stream_buffers[stream_id]


def wait_for_kafka(max_retries=30, delay=2):
    for attempt in range(max_retries):
        try:
            p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "socket.timeout.ms": 2000})
            p.list_topics(timeout=5)
            logger.info("Kafka is ready")
            return
        except Exception:
            logger.info(f"Waiting for Kafka... ({attempt+1}/{max_retries})")
            time.sleep(delay)
    raise RuntimeError("Kafka not available after retries")


def kafka_consumer_loop():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_DETECTIONS])
    logger.info(f"Consuming from '{TOPIC_DETECTIONS}'")

    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue

            result = json.loads(msg.value().decode("utf-8"))
            stream_id = result.get("stream_id", "unknown")
            get_or_create_buffer(stream_id).add(result)

            with stats_lock:
                pipeline_stats["total_messages_consumed"] += 1

        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(1)


@app.route("/health")
def health():
    return jsonify({"status": "healthy", "service": "aggregator"})


@app.route("/streams")
def list_streams():
    return jsonify({"streams": [b.get_stats() for b in stream_buffers.values()]})


@app.route("/streams/<stream_id>")
def get_stream(stream_id):
    if stream_id not in stream_buffers:
        return jsonify({"error": f"Stream '{stream_id}' not found"}), 404
    buf = stream_buffers[stream_id]
    return jsonify({
        "stream_id": stream_id,
        "stats": buf.get_stats(),
        "latest_detections": buf.get_latest(n=10),
    })


@app.route("/stats")
def get_stats():
    with stats_lock:
        uptime = time.time() - pipeline_stats["start_time"]
        total = pipeline_stats["total_messages_consumed"]
    return jsonify({
        "uptime_seconds": round(uptime, 1),
        "total_messages_consumed": total,
        "throughput_fps": round(total / max(uptime, 1), 2),
        "num_streams": len(stream_buffers),
        "streams": [b.get_stats() for b in stream_buffers.values()],
    })


def main():
    wait_for_kafka()
    threading.Thread(target=kafka_consumer_loop, daemon=True).start()
    logger.info("Kafka consumer thread started")
    logger.info(f"Starting REST API on port {API_PORT}")
    app.run(host="0.0.0.0", port=API_PORT, debug=False)


if __name__ == "__main__":
    main()
