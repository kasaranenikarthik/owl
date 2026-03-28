import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from contextlib import asynccontextmanager

from confluent_kafka import KafkaError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from prometheus_fastapi_instrumentator import Instrumentator

from common.config import settings
from common.kafka_utils import create_consumer
from common.metrics import (
    start_metrics_server,
    detections_consumed_total,
    reorder_buffer_size,
    websocket_connections,
)
from schemas.detection_message import DetectionMessage
from ordering import OrderingBuffer
from ws_manager import WSManager

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("aggregator")

ordering_buffer = OrderingBuffer()
ws_manager = WSManager()


async def kafka_consumer_loop():
    """Background task that consumes detections from Kafka."""
    consumer = create_consumer(
        broker=settings.kafka_broker,
        group_id="aggregator",
        topics=[settings.detections_topic],
    )
    logger.info("Aggregator consuming from %s", settings.detections_topic)

    loop = asyncio.get_event_loop()

    try:
        while True:
            # Poll in a thread to avoid blocking the event loop
            msg = await loop.run_in_executor(None, consumer.poll, 0.1)

            if msg is None:
                await asyncio.sleep(0.01)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue

            try:
                detection = DetectionMessage.deserialize(msg.value())
            except Exception:
                logger.exception("Failed to deserialize detection")
                continue

            detections_consumed_total.inc()

            # Add to ordering buffer and get any newly ordered detections
            ordered = ordering_buffer.add(detection)

            # Broadcast ordered detections via WebSocket
            for det in ordered:
                await ws_manager.broadcast(det.stream_id, det)

            # Update buffer size metrics
            for sid in ordering_buffer.active_streams():
                reorder_buffer_size.labels(stream_id=sid).set(
                    ordering_buffer.buffer_size(sid)
                )

            consumer.commit(asynchronous=True)

    except asyncio.CancelledError:
        logger.info("Kafka consumer loop cancelled")
    finally:
        consumer.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_metrics_server(port=8003)
    task = asyncio.create_task(kafka_consumer_loop())
    logger.info("Aggregator started")
    yield
    task.cancel()
    await task


app = FastAPI(title="Video Inference Aggregator", lifespan=lifespan)
Instrumentator().instrument(app).expose(app, endpoint="/metrics")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/streams")
async def list_streams():
    return {"streams": ordering_buffer.active_streams()}


@app.get("/api/streams/{stream_id}/detections")
async def get_detections(
    stream_id: str,
    since_frame_id: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
):
    results = ordering_buffer.get_recent(stream_id, since_frame_id, limit)
    return {"stream_id": stream_id, "count": len(results), "detections": results}


@app.websocket("/ws/{stream_id}")
async def websocket_endpoint(websocket: WebSocket, stream_id: str):
    await ws_manager.connect(stream_id, websocket)
    websocket_connections.set(ws_manager.total_connections)

    try:
        while True:
            # Keep connection alive; client can send pings
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(stream_id, websocket)
        websocket_connections.set(ws_manager.total_connections)
