
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
import random
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
    detections: list[Detection]
    inference_ms: float
    from_cache: bool
    timestamp: float


@dataclass
class Stats:
    frames_sent: int = 0
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

    def __init__(
        self,
        ws_url: str,
        max_retries: int | None = None,
        max_retry_delay_s: float = 10.0,
    ):
        self.ws_url = ws_url
        self.max_retries = max_retries
        self.max_retry_delay_s = max_retry_delay_s
        self.client_id: str | None = None
        self.on_result: Callable[[FrameResult], None] | None = None
        self.stats = Stats()

        self._ws = None
        self._send_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=5)
        self._running = False
        self._send_times: asyncio.Queue[float] = asyncio.Queue()  # FIFO of send timestamps

    async def connect(self):
        """Connect to the gateway WebSocket."""
        await self._connect_with_retry()

    async def _connect_once(self):
        logger.info("Connecting to %s", self.ws_url)
        ws = await websockets.connect(
            self.ws_url,
            max_size=256 * 1024,
            ping_interval=20,
            ping_timeout=10,
        )

        # First message from server is the connection ack with client_id
        raw = await ws.recv()
        msg = json.loads(raw)
        self._ws = ws
        self.client_id = msg.get("client_id")
        logger.info("Connected as client %s", self.client_id)

    async def _connect_with_retry(self):
        attempt = 0
        while True:
            try:
                await self._connect_once()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._close_ws()
                attempt += 1

                if self.max_retries is not None and attempt > self.max_retries:
                    logger.error("Connect failed after %s retries", self.max_retries)
                    raise

                base_delay = min(2 ** (attempt - 1), self.max_retry_delay_s)
                jitter = random.uniform(0.0, min(1.0, base_delay * 0.2))
                delay = min(base_delay + jitter, self.max_retry_delay_s)
                logger.warning(
                    "Connect attempt %s failed: %s; retrying in %.1fs",
                    attempt,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)

    def send_frame(self, jpeg_data: bytes):
        """Queue a JPEG frame for sending. Non-blocking.

        If the send queue is full (server can't keep up), drops the frame.
        This prevents backpressure from stalling the capture loop.
        """
        try:
            self._send_queue.put_nowait(jpeg_data)
            self.stats.frames_sent += 1
        except asyncio.QueueFull:
            pass  # drop frame — server can't keep up

    async def run(self):
        """Run the send and receive loops concurrently."""
        self._running = True
        try:
            while self._running:
                if self._ws is None:
                    await self._connect_with_retry()

                try:
                    await self._run_connected_loops()
                except websockets.ConnectionClosed:
                    if not self._running:
                        break
                    logger.warning("Connection closed; reconnecting")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if not self._running:
                        break
                    logger.exception("Connection loop failed; reconnecting")
                finally:
                    await self._close_ws()
        finally:
            self._running = False

    async def _run_connected_loops(self):
        send_task = asyncio.create_task(self._send_loop())
        recv_task = asyncio.create_task(self._recv_loop())
        done, pending = await asyncio.wait(
            {send_task, recv_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        for task in done:
            if task.cancelled():
                continue
            exc = task.exception()
            if exc is not None:
                raise exc

    async def _send_loop(self):
        """Send queued frames as binary WebSocket messages."""
        ws = self._ws
        if ws is None:
            return

        while self._running:
            try:
                data = await asyncio.wait_for(self._send_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            send_time = time.monotonic()
            await ws.send(data)
            # Track send time for E2E latency (FIFO — results return in order)
            try:
                self._send_times.put_nowait(send_time)
            except asyncio.QueueFull:
                pass

    async def _recv_loop(self):
        """Receive JSON detection results from server."""
        ws = self._ws
        if ws is None:
            return

        async for raw in ws:
            now = time.monotonic()

            data = json.loads(raw)
            if data.get("type") == "connected":
                continue  # skip the initial ack

            result = FrameResult(
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
                timestamp=now,
            )

            # Update stats
            self.stats.results_received += 1
            self.stats.total_inference_ms += result.inference_ms
            if result.from_cache:
                self.stats.cache_hits += 1

            # E2E latency — match with oldest unmatched send time
            try:
                send_time = self._send_times.get_nowait()
                e2e = (now - send_time) * 1000
                self.stats.total_e2e_ms += e2e
            except asyncio.QueueEmpty:
                pass

            if self.on_result:
                self.on_result(result)

    async def close(self):
        self._running = False
        await self._close_ws()

    async def _close_ws(self):
        ws = self._ws
        self._ws = None
        if ws:
            await ws.close()
