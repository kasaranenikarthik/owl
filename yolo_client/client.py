
# =============================================================================
# yolo_client/client.py
# =============================================================================

"""Core bidirectional WebSocket client.

Sends binary JPEG frames to the server, receives JSON detection results.
Fully async — runs a send loop and receive loop concurrently.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import websockets

logger = logging.getLogger("yolo_client")


@dataclass
class Detection:
    x1: float
    y1: float
    x2: float
    y2: float
    confidence: float
    class_id: int
    class_name: str


@dataclass
class FrameResult:
    frame_id: int
    detections: list[Detection]
    inference_ms: float
    from_cache: bool
    timestamp: float


@dataclass
class Stats:
    frames_sent: int = 0
    frames_dropped: int = 0
    results_received: int = 0
    cache_hits: int = 0
    total_inference_ms: float = 0.0
    total_e2e_ms: float = 0.0
    start_time: float = field(default_factory=time.monotonic)

    @property
    def avg_inference_ms(self) -> float:
        if self.results_received == 0:
            return 0
        return self.total_inference_ms / self.results_received

    @property
    def avg_e2e_ms(self) -> float:
        if self.results_received == 0:
            return 0
        return self.total_e2e_ms / self.results_received

    @property
    def fps(self) -> float:
        elapsed = time.monotonic() - self.start_time
        if elapsed == 0:
            return 0
        return self.results_received / elapsed

    @property
    def cache_hit_rate(self) -> float:
        if self.results_received == 0:
            return 0
        return self.cache_hits / self.results_received * 100


class RealtimeClient:
    """Bidirectional WebSocket client for real-time YOLO inference.

    Usage:
        client = RealtimeClient("ws://gateway:80/ws")
        await client.connect()

        # Send frames (call from your capture loop)
        client.send_frame(jpeg_bytes)

        # Receive results (runs callback on each result)
        client.on_result = my_callback

        await client.run()
    """

    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self.client_id: str | None = None
        self.on_result: Callable[[FrameResult], None] | None = None
        self.stats = Stats()

        self._ws = None
        self._send_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=5)
        self._running = False
        self._send_times: dict[int, float] = {}
        self._sent_seq = 0
        self._last_frame_id = 0

    async def connect(self):
        """Connect to the gateway WebSocket."""
        logger.info("Connecting to %s", self.ws_url)
        self._ws = await websockets.connect(
            self.ws_url,
            max_size=2 * 1024 * 1024,
            ping_interval=20,
            ping_timeout=10,
        )

        # First message from server is the connection ack with client_id
        raw = await self._ws.recv()
        msg = json.loads(raw)
        self.client_id = msg.get("client_id")
        logger.info("Connected as client %s", self.client_id)

    def send_frame(self, jpeg_data: bytes):
        """Queue a JPEG frame for sending. Non-blocking.
        Drops the frame if the send queue is full.
        """
        try:
            self._send_queue.put_nowait(jpeg_data)
            self.stats.frames_sent += 1
        except asyncio.QueueFull:
            self.stats.frames_dropped += 1

    async def run(self):
        """Run the send and receive loops concurrently."""
        self._running = True
        try:
            await asyncio.gather(
                self._send_loop(),
                self._recv_loop(),
            )
        except websockets.ConnectionClosed:
            logger.warning("Connection closed")
        finally:
            self._running = False

    async def _send_loop(self):
        """Send queued frames as binary WebSocket messages."""
        if self._ws is None:
            return
        ws = self._ws

        while self._running:
            try:
                data = await asyncio.wait_for(self._send_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            await ws.send(data)
            self._sent_seq += 1
            self._send_times[self._sent_seq] = time.monotonic()

    async def _recv_loop(self):
        """Receive JSON detection results from server.
        Server already drops stale results — client accepts everything."""
        if self._ws is None:
            return
        ws = self._ws

        async for raw in ws:
            now = time.monotonic()

            data = json.loads(raw)
            if data.get("type") == "connected":
                continue

            frame_id = int(data.get("frame_id", 0))

            if frame_id > 0:
                if self._last_frame_id > 0 and frame_id > self._last_frame_id + 1:
                    self.stats.frames_dropped += frame_id - self._last_frame_id - 1
                if frame_id > self._last_frame_id:
                    self._last_frame_id = frame_id

            result = FrameResult(
                frame_id=frame_id,
                detections=[
                    Detection(
                        x1=d["bounding_box"]["x1"],
                        y1=d["bounding_box"]["y1"],
                        x2=d["bounding_box"]["x2"],
                        y2=d["bounding_box"]["y2"],
                        confidence=d["bounding_box"]["confidence"],
                        class_id=d["bounding_box"]["class_id"],
                        class_name=d["bounding_box"]["class_name"],
                    )
                    for d in data.get("detections", [])
                ],
                inference_ms=data.get("inference_ms", 0),
                from_cache=data.get("from_cache", False),
                timestamp=data.get("timestamp", time.time()),
            )

            self.stats.results_received += 1
            self.stats.total_inference_ms += result.inference_ms
            if result.from_cache:
                self.stats.cache_hits += 1

            send_time = self._send_times.pop(frame_id, None)
            if send_time is not None:
                e2e = (now - send_time) * 1000
                self.stats.total_e2e_ms += e2e

            # Cleanup in case old frames were dropped before a result arrived.
            if frame_id > 0:
                stale = [k for k in self._send_times if k < frame_id-300]
                for k in stale:
                    self._send_times.pop(k, None)

            if self.on_result:
                self.on_result(result)

    async def close(self):
        self._running = False
        if self._ws:
            await self._ws.close()
