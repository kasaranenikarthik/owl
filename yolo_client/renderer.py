
# =============================================================================
# yolo_client/renderer.py
# =============================================================================

"""Draws detection bounding boxes on frames."""

from __future__ import annotations

import colorsys

import cv2
import numpy as np

from yolo_client.client import Detection, FrameResult, Stats


def _colors(n: int) -> list[tuple[int, int, int]]:
    return [
        tuple(int(c * 255) for c in colorsys.hsv_to_rgb(i / n, 0.8, 0.9))[::-1]
        for i in range(n)
    ]

COLORS = _colors(80)


def draw_detections(frame: np.ndarray, result: FrameResult) -> np.ndarray:
    """Draw bounding boxes and labels on a frame."""
    for det in result.detections:
        color = COLORS[det.class_id % len(COLORS)]
        x1, y1 = int(det.x1), int(det.y1)
        x2, y2 = int(det.x2), int(det.y2)

        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

        label = f"{det.class_name} {det.confidence:.0%}"
        (tw, th), _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
        cv2.rectangle(frame, (x1, y1 - th - 6), (x1 + tw + 4, y1), color, cv2.FILLED)
        cv2.putText(frame, label, (x1 + 2, y1 - 4),
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
