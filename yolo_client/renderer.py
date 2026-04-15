
# =============================================================================
# yolo_client/renderer.py
# =============================================================================

"""Draws detection bounding boxes on frames."""

from __future__ import annotations

import colorsys

import cv2
import numpy as np

from client import Detection, FrameResult, Stats


def _colors(n: int) -> list[tuple[int, int, int]]:
    palette: list[tuple[int, int, int]] = []
    for i in range(n):
        r, g, b = colorsys.hsv_to_rgb(i / n, 0.8, 0.9)
        palette.append((int(b * 255), int(g * 255), int(r * 255)))
    return palette

COLORS = _colors(80)


def draw_detections(frame: np.ndarray, result: FrameResult) -> np.ndarray:
    """Draw bounding boxes and labels on a frame."""
    h, w = frame.shape[:2]

    for det in result.detections:
        color = COLORS[det.class_id % len(COLORS)]
        x1 = max(0, min(w - 1, int(det.x1)))
        y1 = max(0, min(h - 1, int(det.y1)))
        x2 = max(0, min(w - 1, int(det.x2)))
        y2 = max(0, min(h - 1, int(det.y2)))

        if x2 <= x1 or y2 <= y1:
            continue

        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

        label = f"{det.class_name} {det.confidence:.0%}"
        (tw, th), _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
        label_x1 = x1
        label_x2 = min(w - 1, x1 + tw + 4)

        if label_x2 <= label_x1:
            continue

        if y1 >= th + 6:
            label_y1 = y1 - th - 6
            label_y2 = y1
            text_y = y1 - 4
        else:
            label_y1 = y2
            label_y2 = min(h - 1, y2 + th + 6)
            text_y = min(h - 4, y2 + th + 2)

        cv2.rectangle(frame, (label_x1, label_y1), (label_x2, label_y2), color, cv2.FILLED)
        cv2.putText(frame, label, (label_x1 + 2, text_y),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1, cv2.LINE_AA)

    return frame


def draw_overlay(frame: np.ndarray, result: FrameResult, stats: Stats) -> np.ndarray:
    """Draw a stats overlay on the frame."""
    h, w = frame.shape[:2]

    lines = [
        f"FPS: {stats.fps:.1f}",
        f"Inference: {result.inference_ms:.1f}ms",
        f"E2E: {stats.avg_e2e_ms:.1f}ms",
        f"Cache: {stats.cache_hit_rate:.0f}%",
        f"Dropped: {stats.frames_dropped}",
        f"Objects: {len(result.detections)}",
    ]

    if result.from_cache:
        lines.append("CACHED")

    y = 20
    for line in lines:
        cv2.putText(frame, line, (10, y),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 1, cv2.LINE_AA)
        y += 20

    return frame
