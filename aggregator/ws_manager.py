import logging
from collections import defaultdict

from fastapi import WebSocket

from schemas.detection_message import DetectionMessage

logger = logging.getLogger(__name__)


class WSManager:
    """Manages WebSocket connections per stream_id."""

    def __init__(self):
        self._connections: dict[str, set[WebSocket]] = defaultdict(set)

    async def connect(self, stream_id: str, ws: WebSocket):
        await ws.accept()
        self._connections[stream_id].add(ws)
        logger.info("WebSocket connected for stream %s (total=%d)",
                     stream_id, len(self._connections[stream_id]))

    def disconnect(self, stream_id: str, ws: WebSocket):
        self._connections[stream_id].discard(ws)
        logger.info("WebSocket disconnected for stream %s", stream_id)

    async def broadcast(self, stream_id: str, detection: DetectionMessage):
        clients = self._connections.get(stream_id, set())
        if not clients:
            return

        payload = detection.model_dump_json()
        disconnected = []
        for ws in clients:
            try:
                await ws.send_text(payload)
            except Exception:
                disconnected.append(ws)

        for ws in disconnected:
            self.disconnect(stream_id, ws)

    @property
    def total_connections(self) -> int:
        return sum(len(s) for s in self._connections.values())
