"""
Real-time viewer service.

Consumes frames and detections from Kafka, draws bounding boxes,
and serves an MJPEG stream viewable in any browser.
"""

import base64
import logging
import threading
import time
from collections import defaultdict

import cv2
import numpy as np
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse

from common.config import settings
from common.kafka_utils import create_consumer
from schemas.frame_message import FrameMessage
from schemas.detection_message import DetectionMessage

logging.basicConfig(
    level=getattr(logging, settings.log_level, logging.INFO),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("viewer")

# ---------------------------------------------------------------------------
# Shared state: latest annotated frame per stream
# ---------------------------------------------------------------------------

# frame_id -> decoded numpy frame
_frame_buffer: dict[int, np.ndarray] = {}
# frame_id -> DetectionMessage
_detection_buffer: dict[int, DetectionMessage] = {}
# latest fully-annotated JPEG per stream_id
_latest_annotated: dict[str, bytes] = {}

_lock = threading.Lock()

# How many raw frames to keep in the buffer waiting for detections
_BUFFER_MAX = 120

# Colour palette for bounding-box classes (BGR)
_COLORS = [
    (0, 255, 0),    # green
    (255, 0, 0),    # blue
    (0, 0, 255),    # red
    (0, 255, 255),  # yellow
    (255, 0, 255),  # magenta
    (255, 255, 0),  # cyan
    (128, 0, 255),  # purple
    (0, 128, 255),  # orange
]


def _color_for_class(class_id: int) -> tuple[int, int, int]:
    return _COLORS[class_id % len(_COLORS)]


def _annotate(frame: np.ndarray, det: DetectionMessage) -> np.ndarray:
    """Draw bounding boxes + labels on a copy of the frame."""
    img = frame.copy()
    for box in det.detections:
        color = _color_for_class(box.class_id)
        p1 = (int(box.x1), int(box.y1))
        p2 = (int(box.x2), int(box.y2))
        cv2.rectangle(img, p1, p2, color, 2)

        label = f"{box.class_name} {box.confidence:.0%}"
        font = cv2.FONT_HERSHEY_SIMPLEX
        font_scale = 0.55
        thickness = 1
        (tw, th), baseline = cv2.getTextSize(label, font, font_scale, thickness)

        # label background
        cv2.rectangle(
            img,
            (p1[0], p1[1] - th - baseline - 4),
            (p1[0] + tw, p1[1]),
            color,
            cv2.FILLED,
        )
        cv2.putText(
            img, label, (p1[0], p1[1] - baseline - 2),
            font, font_scale, (0, 0, 0), thickness, cv2.LINE_AA,
        )

    # overlay inference latency
    cv2.putText(
        img,
        f"Inference: {det.inference_time_ms:.1f}ms  |  {len(det.detections)} objects",
        (10, 30),
        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2, cv2.LINE_AA,
    )
    return img


def _try_match(frame_id: int, stream_id: str):
    """If both frame and detection exist for frame_id, annotate and store."""
    if frame_id in _frame_buffer and frame_id in _detection_buffer:
        frame = _frame_buffer.pop(frame_id)
        det = _detection_buffer.pop(frame_id)
        annotated = _annotate(frame, det)
        _, jpeg = cv2.imencode(".jpg", annotated, [cv2.IMWRITE_JPEG_QUALITY, 80])
        _latest_annotated[stream_id] = jpeg.tobytes()


def _evict_old_buffers():
    """Remove old entries to bound memory usage."""
    if len(_frame_buffer) > _BUFFER_MAX:
        sorted_ids = sorted(_frame_buffer.keys())
        for fid in sorted_ids[: len(sorted_ids) - _BUFFER_MAX // 2]:
            _frame_buffer.pop(fid, None)
    if len(_detection_buffer) > _BUFFER_MAX:
        sorted_ids = sorted(_detection_buffer.keys())
        for fid in sorted_ids[: len(sorted_ids) - _BUFFER_MAX // 2]:
            _detection_buffer.pop(fid, None)


# ---------------------------------------------------------------------------
# Kafka consumer threads
# ---------------------------------------------------------------------------

def _consume_frames():
    """Background thread: consume frames from Kafka."""
    consumer = create_consumer(
        settings.kafka_broker,
        group_id="viewer-frames",
        topics=[settings.frames_topic],
    )
    logger.info("Frame consumer started on %s", settings.frames_topic)
    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            try:
                fm = FrameMessage.deserialize(msg.value())
                raw = base64.b64decode(fm.data)
                arr = np.frombuffer(raw, dtype=np.uint8)
                frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                if frame is None:
                    continue
                with _lock:
                    _frame_buffer[fm.frame_id] = frame
                    _try_match(fm.frame_id, fm.stream_id)
                    _evict_old_buffers()
                consumer.commit(asynchronous=True)
            except Exception:
                logger.exception("Error processing frame message")
    finally:
        consumer.close()


def _consume_detections():
    """Background thread: consume detections from Kafka."""
    consumer = create_consumer(
        settings.kafka_broker,
        group_id="viewer-detections",
        topics=[settings.detections_topic],
    )
    logger.info("Detection consumer started on %s", settings.detections_topic)
    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            try:
                det = DetectionMessage.deserialize(msg.value())
                with _lock:
                    _detection_buffer[det.frame_id] = det
                    _try_match(det.frame_id, det.stream_id)
                    _evict_old_buffers()
                consumer.commit(asynchronous=True)
            except Exception:
                logger.exception("Error processing detection message")
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Pipeline Viewer")


@app.on_event("startup")
def _start_consumers():
    threading.Thread(target=_consume_frames, daemon=True).start()
    threading.Thread(target=_consume_detections, daemon=True).start()
    logger.info("Viewer consumers launched")


def _mjpeg_generator(stream_id: str):
    """Yield MJPEG frames for the given stream."""
    while True:
        with _lock:
            jpeg = _latest_annotated.get(stream_id)
        if jpeg is not None:
            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n\r\n"
                + jpeg
                + b"\r\n"
            )
        time.sleep(0.033)  # ~30 fps cap


@app.get("/video/{stream_id}")
def video_feed(stream_id: str):
    """MJPEG stream endpoint."""
    return StreamingResponse(
        _mjpeg_generator(stream_id),
        media_type="multipart/x-mixed-replace; boundary=frame",
    )


@app.get("/", response_class=HTMLResponse)
def index():
    """Simple viewer page."""
    return """<!DOCTYPE html>
<html>
<head>
    <title>Pipeline Viewer</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            background: #1a1a2e; color: #eee;
            font-family: 'Segoe UI', sans-serif;
            display: flex; flex-direction: column;
            align-items: center; min-height: 100vh;
        }
        h1 { margin: 20px 0 10px; font-size: 1.4rem; color: #0ff; }
        .controls {
            margin-bottom: 12px; display: flex;
            gap: 10px; align-items: center;
        }
        label { font-size: 0.9rem; }
        input {
            background: #16213e; border: 1px solid #0f3460;
            color: #eee; padding: 6px 10px; border-radius: 4px;
            font-size: 0.9rem; width: 160px;
        }
        button {
            background: #0f3460; color: #eee; border: none;
            padding: 6px 16px; border-radius: 4px; cursor: pointer;
            font-size: 0.9rem;
        }
        button:hover { background: #533483; }
        .stream-container {
            border: 2px solid #0f3460; border-radius: 8px;
            overflow: hidden; background: #000;
            max-width: 95vw;
        }
        img {
            display: block; max-width: 95vw;
            max-height: 80vh; object-fit: contain;
        }
        .status {
            margin-top: 10px; font-size: 0.8rem; color: #888;
        }
    </style>
</head>
<body>
    <h1>Real-Time Object Detection Viewer</h1>
    <div class="controls">
        <label>Stream ID:</label>
        <input id="sid" value="stream-0" />
        <button onclick="connect()">Connect</button>
    </div>
    <div class="stream-container">
        <img id="feed" alt="Waiting for stream..." />
    </div>
    <div class="status" id="status">Not connected</div>
    <script>
        function connect() {
            const sid = document.getElementById('sid').value;
            const img = document.getElementById('feed');
            img.src = '/video/' + encodeURIComponent(sid);
            document.getElementById('status').textContent =
                'Connected to ' + sid + ' \u2014 streaming...';
        }
        // Auto-connect on load
        window.addEventListener('load', connect);
    </script>
</body>
</html>"""


@app.get("/health")
def health():
    return {"status": "ok", "streams": list(_latest_annotated.keys())}
