from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass, field
from statistics import fmean

import cv2
import websockets

try:
    from .sources import VideoFileSource, encode_jpeg
except ImportError:  # pragma: no cover - allows direct script execution
    from sources import VideoFileSource, encode_jpeg

logger = logging.getLogger("yolo_benchmark")


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]

    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile / 100.0
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return ordered[lo]
    lower = ordered[lo]
    upper = ordered[hi]
    return lower + (upper - lower) * (rank - lo)


@dataclass
class BenchmarkClientStats:
    frames_read: int = 0
    frames_attempted: int = 0
    frames_enqueued: int = 0
    frames_sent: int = 0
    send_queue_drops: int = 0
    results_received: int = 0
    result_gap_drops: int = 0
    cache_hits: int = 0
    e2e_samples_ms: list[float] = field(default_factory=list)
    inference_samples_ms: list[float] = field(default_factory=list)

    def to_summary(self, duration_seconds: float) -> dict[str, float | int]:
        avg_inference_ms = fmean(self.inference_samples_ms) if self.inference_samples_ms else 0.0
        avg_e2e_ms = fmean(self.e2e_samples_ms) if self.e2e_samples_ms else 0.0
        client_drop_count = self.send_queue_drops + self.result_gap_drops
        return {
            "frames_read": self.frames_read,
            "frames_attempted": self.frames_attempted,
            "frames_enqueued": self.frames_enqueued,
            "frames_sent": self.frames_sent,
            "send_queue_drops": self.send_queue_drops,
            "results_received": self.results_received,
            "result_gap_drops": self.result_gap_drops,
            "client_drop_count": client_drop_count,
            "cache_hits": self.cache_hits,
            "avg_inference_ms": avg_inference_ms,
            "p95_inference_ms": _percentile(self.inference_samples_ms, 95.0),
            "avg_e2e_ms": avg_e2e_ms,
            "p95_e2e_ms": _percentile(self.e2e_samples_ms, 95.0),
            "delivered_fps": self.results_received / duration_seconds if duration_seconds > 0 else 0.0,
            "cache_hit_rate": (self.cache_hits / self.results_received * 100.0) if self.results_received else 0.0,
            "send_queue_drop_rate": (self.send_queue_drops / self.frames_attempted * 100.0) if self.frames_attempted else 0.0,
            "result_gap_drop_rate": (self.result_gap_drops / self.frames_attempted * 100.0) if self.frames_attempted else 0.0,
            "client_drop_rate": (client_drop_count / self.frames_attempted * 100.0) if self.frames_attempted else 0.0,
        }


class BenchmarkRealtimeClient:
    def __init__(self, ws_url: str, *, send_queue_size: int = 10, name: str = "client") -> None:
        self.ws_url = ws_url
        self.name = name
        self.stats = BenchmarkClientStats()

        self._ws = None
        self._running = False
        self._send_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=send_queue_size)
        self._send_times: dict[int, float] = {}
        self._sent_seq = 0
        self._last_frame_id = 0

    async def connect(self) -> None:
        logger.info("%s connecting to %s", self.name, self.ws_url)
        self._ws = await websockets.connect(
            self.ws_url,
            max_size=2 * 1024 * 1024,
            ping_interval=20,
            ping_timeout=10,
        )
        raw = await self._ws.recv()
        msg = json.loads(raw)
        logger.info("%s connected as %s", self.name, msg.get("client_id"))

    async def run(self) -> None:
        self._running = True
        try:
            await asyncio.gather(self._send_loop(), self._recv_loop())
        except websockets.ConnectionClosed:
            logger.info("%s connection closed", self.name)
        finally:
            self._running = False

    def queue_frame(self, jpeg_data: bytes) -> None:
        self.stats.frames_attempted += 1
        try:
            self._send_queue.put_nowait(jpeg_data)
            self.stats.frames_enqueued += 1
        except asyncio.QueueFull:
            self.stats.send_queue_drops += 1

    async def stream_video(
        self,
        video_path: str,
        *,
        fps: int,
        duration_seconds: float,
        jpeg_quality: int = 80,
        resize_width: int | None = None,
        resize_height: int | None = None,
    ) -> None:
        source = VideoFileSource(video_path)
        interval = 1.0 / fps
        deadline = time.monotonic() + duration_seconds

        try:
            while time.monotonic() < deadline:
                loop_started_at = time.monotonic()
                ok, frame = source.read()
                if not ok:
                    break

                self.stats.frames_read += 1
                if resize_width is not None and resize_height is not None:
                    frame = cv2.resize(frame, (resize_width, resize_height), interpolation=cv2.INTER_AREA)
                self.queue_frame(encode_jpeg(frame, jpeg_quality))

                delay = interval - (time.monotonic() - loop_started_at)
                if delay > 0:
                    await asyncio.sleep(delay)
        finally:
            source.release()

    async def close(self) -> None:
        self._running = False
        if self._ws is not None:
            await self._ws.close()

    async def _send_loop(self) -> None:
        if self._ws is None:
            return
        ws = self._ws

        while self._running or not self._send_queue.empty():
            try:
                payload = await asyncio.wait_for(self._send_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            await ws.send(payload)
            self._sent_seq += 1
            self.stats.frames_sent += 1
            self._send_times[self._sent_seq] = time.monotonic()

    async def _recv_loop(self) -> None:
        if self._ws is None:
            return
        ws = self._ws

        async for raw in ws:
            received_at = time.monotonic()
            payload = json.loads(raw)
            if payload.get("type") == "connected":
                continue

            frame_id = int(payload.get("frame_id", 0))
            if frame_id > 0:
                if self._last_frame_id > 0 and frame_id > self._last_frame_id + 1:
                    self.stats.result_gap_drops += frame_id - self._last_frame_id - 1
                if frame_id > self._last_frame_id:
                    self._last_frame_id = frame_id

            self.stats.results_received += 1
            inference_ms = float(payload.get("inference_ms", 0.0))
            self.stats.inference_samples_ms.append(inference_ms)
            if payload.get("from_cache", False):
                self.stats.cache_hits += 1

            if frame_id > 0:
                sent_at = self._send_times.pop(frame_id, None)
                if sent_at is not None:
                    self.stats.e2e_samples_ms.append((received_at - sent_at) * 1000.0)

                stale_keys = [key for key in self._send_times if key < frame_id - 300]
                for key in stale_keys:
                    self._send_times.pop(key, None)


async def run_benchmark_session(
    *,
    server_url: str,
    video_path: str,
    fps: int,
    client_count: int,
    duration_seconds: float,
    settle_seconds: float = 5.0,
    jpeg_quality: int = 80,
    send_queue_size: int = 10,
    resize_width: int | None = None,
    resize_height: int | None = None,
) -> dict[str, object]:
    clients = [
        BenchmarkRealtimeClient(server_url, send_queue_size=send_queue_size, name=f"client-{index + 1}")
        for index in range(client_count)
    ]

    for client in clients:
        await client.connect()

    run_tasks = [asyncio.create_task(client.run()) for client in clients]
    stream_tasks = [
        asyncio.create_task(
            client.stream_video(
                video_path,
                fps=fps,
                duration_seconds=duration_seconds,
                jpeg_quality=jpeg_quality,
                resize_width=resize_width,
                resize_height=resize_height,
            )
        )
        for client in clients
    ]

    await asyncio.gather(*stream_tasks)
    if settle_seconds > 0:
        await asyncio.sleep(settle_seconds)

    for client in clients:
        await client.close()
    await asyncio.gather(*run_tasks, return_exceptions=True)

    per_client = [client.stats.to_summary(duration_seconds) for client in clients]

    aggregate = BenchmarkClientStats()
    for client in clients:
        stats = client.stats
        aggregate.frames_read += stats.frames_read
        aggregate.frames_attempted += stats.frames_attempted
        aggregate.frames_enqueued += stats.frames_enqueued
        aggregate.frames_sent += stats.frames_sent
        aggregate.send_queue_drops += stats.send_queue_drops
        aggregate.results_received += stats.results_received
        aggregate.result_gap_drops += stats.result_gap_drops
        aggregate.cache_hits += stats.cache_hits
        aggregate.e2e_samples_ms.extend(stats.e2e_samples_ms)
        aggregate.inference_samples_ms.extend(stats.inference_samples_ms)

    return {
        "aggregate": aggregate.to_summary(duration_seconds),
        "per_client": per_client,
    }
