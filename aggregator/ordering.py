import heapq
import logging
from collections import defaultdict, deque

from schemas.detection_message import DetectionMessage

logger = logging.getLogger(__name__)

MAX_RECENT = 500  # max recent detections to keep per stream


class OrderingBuffer:
    """Per-stream reorder buffer that yields detections in frame_id order."""

    def __init__(self):
        # stream_id -> min-heap of (frame_id, DetectionMessage)
        self._heaps: dict[str, list[tuple[int, DetectionMessage]]] = defaultdict(list)
        # stream_id -> next expected frame_id (initialized on first message)
        self._next_expected: dict[str, int] = {}
        # stream_id -> recent ordered detections (bounded deque)
        self._recent: dict[str, deque[DetectionMessage]] = defaultdict(
            lambda: deque(maxlen=MAX_RECENT)
        )

    def add(self, detection: DetectionMessage) -> list[DetectionMessage]:
        """Add a detection and return any newly ordered detections."""
        sid = detection.stream_id
        fid = detection.frame_id

        # Initialize next_expected to the first frame_id we see for this stream
        if sid not in self._next_expected:
            self._next_expected[sid] = fid
            logger.info("Stream %s: initialized next_expected to frame %d", sid, fid)

        # Drop duplicates / already-seen frames
        if fid < self._next_expected[sid]:
            logger.debug(
                "Dropping duplicate frame %d for stream %s (expected >= %d)",
                fid, sid, self._next_expected[sid],
            )
            return []

        heapq.heappush(self._heaps[sid], (fid, detection))

        # Drain consecutive detections
        ordered = []
        heap = self._heaps[sid]
        while heap and heap[0][0] == self._next_expected[sid]:
            _, det = heapq.heappop(heap)
            ordered.append(det)
            self._recent[sid].append(det)
            self._next_expected[sid] += 1

        return ordered

    def get_recent(
        self, stream_id: str, since_frame_id: int = 0, limit: int = 100
    ) -> list[DetectionMessage]:
        """Return recent ordered detections for a stream after since_frame_id."""
        recent = self._recent.get(stream_id, deque())
        results = [d for d in recent if d.frame_id > since_frame_id]
        return results[-limit:]

    def active_streams(self) -> list[str]:
        return list(self._next_expected.keys())

    def buffer_size(self, stream_id: str) -> int:
        return len(self._heaps.get(stream_id, []))
