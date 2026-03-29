"""
Inference Worker - consumes frames from "raw-frames" Kafka topic,
runs YOLOv8 object detection on the GPU, and publishes detection
results to the "detections" Kafka topic.

YOLOv8 ("You Only Look Once" version 8):
  - Input: an image (your video frame)
  - Output: bounding boxes + class labels + confidence scores
  - Example: "person at (x1,y1,x2,y2) with 0.92 confidence"

On the T4 GPU: ~200+ fps with yolov8n
On CPU:        ~5-15 fps
"""

import os
import time
import json
import logging

import cv2
import numpy as np
import torch
from ultralytics import YOLO
from confluent_kafka import Consumer, Producer, KafkaError

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW_FRAMES = os.environ.get("TOPIC_RAW_FRAMES", "raw-frames")
TOPIC_DETECTIONS = os.environ.get("TOPIC_DETECTIONS", "detections")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "inference-workers")
WORKER_ID = os.environ.get("WORKER_ID", "worker-0")
MODEL_SIZE = os.environ.get("MODEL_SIZE", "yolov8n")
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.5"))

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{WORKER_ID}] %(message)s")
logger = logging.getLogger(__name__)


def wait_for_kafka(max_retries=30, delay=2):
    """Block until Kafka is reachable."""
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


def create_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "fetch.max.bytes": 10_000_000,
        "max.partition.fetch.bytes": 2_000_000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_RAW_FRAMES])
    return consumer


def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "message.timeout.ms": 10000,
        "linger.ms": 5,
    })


def load_model():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info(f"Loading model {MODEL_SIZE}.pt on device: {device}")
    if device == "cuda":
        gpu_name = torch.cuda.get_device_name(0)
        gpu_mem = torch.cuda.get_device_properties(0).total_mem / 1e9
        logger.info(f"GPU: {gpu_name}, VRAM: {gpu_mem:.1f} GB")

    model = YOLO(f"{MODEL_SIZE}.pt")
    model.to(device)

    # Warm up — first inference is slow due to CUDA JIT compilation
    dummy = np.zeros((480, 640, 3), dtype=np.uint8)
    model(dummy, verbose=False)
    logger.info("Model loaded and warmed up")
    return model, device


def run_inference(model, jpeg_bytes):
    np_arr = np.frombuffer(jpeg_bytes, np.uint8)
    frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    if frame is None:
        return []

    results = model(frame, conf=CONFIDENCE_THRESHOLD, verbose=False)

    detections = []
    for result in results:
        for box in result.boxes:
            detections.append({
                "class_name": result.names[int(box.cls[0])],
                "confidence": round(float(box.conf[0]), 4),
                "bbox": [round(float(x), 2) for x in box.xyxy[0].tolist()],
            })
    return detections


def run_worker():
    wait_for_kafka()
    consumer = create_consumer()
    producer = create_producer()
    model, device = load_model()

    logger.info(f"Worker started on {device}, consuming from '{TOPIC_RAW_FRAMES}'")

    frames_processed = 0
    total_inference_time = 0.0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue

            headers = {h[0]: h[1].decode("utf-8") for h in msg.headers()}
            frame_id = headers.get("frame_id", "unknown")
            timestamp = headers.get("timestamp", "0")
            stream_id = headers.get("stream_id", "unknown")

            inference_start = time.time()
            detections = run_inference(model, msg.value())
            inference_time = time.time() - inference_start

            total_inference_time += inference_time
            frames_processed += 1

            result = {
                "stream_id": stream_id,
                "frame_id": int(frame_id),
                "capture_timestamp": float(timestamp),
                "inference_timestamp": time.time(),
                "inference_time_ms": round(inference_time * 1000, 2),
                "worker_id": WORKER_ID,
                "num_detections": len(detections),
                "detections": detections,
            }

            producer.produce(
                topic=TOPIC_DETECTIONS,
                value=json.dumps(result).encode("utf-8"),
                key=stream_id.encode("utf-8"),
                headers={
                    "frame_id": frame_id.encode("utf-8"),
                    "stream_id": stream_id.encode("utf-8"),
                },
            )
            producer.poll(0)
            consumer.commit(asynchronous=False)

            if frames_processed % 50 == 0:
                avg_ms = (total_inference_time / frames_processed) * 1000
                logger.info(
                    f"Processed {frames_processed} frames | "
                    f"avg inference: {avg_ms:.1f}ms | device: {device} | "
                    f"last frame: {len(detections)} objects"
                )

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run_worker()
