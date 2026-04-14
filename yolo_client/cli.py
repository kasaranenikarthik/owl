
# =============================================================================
# yolo_client/cli.py
# =============================================================================

"""CLI entrypoint — real-time YOLO detection with live preview."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import threading
import time

import cv2

from client import RealtimeClient, FrameResult
from sources import (
    WebcamSource,
    VideoFileSource,
    RTSPSource,
    encode_jpeg,
    FrameSource,
)
from renderer import draw_detections, draw_overlay

logger = logging.getLogger("yolo_client")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="yolo", description="Real-time YOLO client")
    p.add_argument("--server", default="ws://localhost:8080/ws", help="Gateway WebSocket URL")
    p.add_argument("-v", "--verbose", action="store_true")

    sub = p.add_subparsers(dest="command", required=True)

    # webcam
    cam = sub.add_parser("webcam", help="Stream from webcam")
    cam.add_argument("--device", type=int, default=0, help="Webcam device ID")
    cam.add_argument("--fps", type=int, default=15, help="Target send FPS")
    cam.add_argument("--width", type=int, default=640)
    cam.add_argument("--height", type=int, default=480)

    # video file
    vid = sub.add_parser("video", help="Stream from video file")
    vid.add_argument("file", help="Path to video file")
    vid.add_argument("--fps", type=int, default=15, help="Target send FPS")

    # RTSP/RTMP stream
    stream = sub.add_parser("stream", help="Stream from RTSP/RTMP URL")
    stream.add_argument("url", help="RTSP or RTMP URL")
    stream.add_argument("--fps", type=int, default=15, help="Target send FPS")

    # headless mode (no preview window)
    for s in [cam, vid, stream]:
        s.add_argument("--headless", action="store_true", help="No preview window")
        s.add_argument("--quality", type=int, default=80, help="JPEG quality (1-100)")

    return p


def get_source(args) -> FrameSource:
    if args.command == "webcam":
        return WebcamSource(args.device, args.width, args.height)
    elif args.command == "video":
        return VideoFileSource(args.file)
    elif args.command == "stream":
        return RTSPSource(args.url)
    raise ValueError(f"Unknown command: {args.command}")


async def run_pipeline(args):
    """Main pipeline: capture → encode → send → receive → render."""

    source = get_source(args)
    client = RealtimeClient(args.server)

    # Shared state between capture thread and async event loop
    latest_result: FrameResult | None = None
    result_lock = threading.Lock()

    def on_result(result: FrameResult):
        nonlocal latest_result
        with result_lock:
            latest_result = result

    client.on_result = on_result

    # Connect to server
    await client.connect()
    print(f"Connected as {client.client_id}")

    # Run WebSocket send/recv loops in background
    ws_task = asyncio.create_task(client.run())

    target_fps = args.fps
    frame_interval = 1.0 / target_fps
    show_preview = not args.headless

    def disable_preview(exc: cv2.error):
        nonlocal show_preview
        if not show_preview:
            return
        show_preview = False
        logger.warning(
            "Preview window unavailable; continuing in headless mode. "
            "Use --headless to silence this warning or replace "
            "opencv-python-headless with opencv-python for local preview "
            "support. OpenCV error: %s",
            exc,
        )
        print("Preview unavailable; continuing in headless mode. Press Ctrl+C to quit.\n")

    if show_preview:
        print("Press 'q' in the preview window to quit (or Ctrl+C).")
        print("If the OpenCV GUI backend is unavailable, the client will continue headless.\n")
    else:
        print("Running headless. Press Ctrl+C to quit.\n")

    try:
        while True:
            loop_start = time.monotonic()

            ret, frame = source.read()
            if not ret:
                if args.command == "video":
                    print("\nVideo complete.")
                    break
                continue

            # Encode and send
            jpeg = encode_jpeg(frame, args.quality)
            client.send_frame(jpeg)

            # Render detections on the frame
            if show_preview:
                with result_lock:
                    current_result = latest_result

                if current_result:
                    draw_detections(frame, current_result)
                    draw_overlay(frame, current_result, client.stats)

                try:
                    cv2.imshow("YOLO Realtime", frame)
                    if cv2.waitKey(1) & 0xFF == ord("q"):
                        break
                except cv2.error as exc:
                    disable_preview(exc)
            else:
                # Headless — print stats periodically
                if client.stats.results_received % 30 == 0 and client.stats.results_received > 0:
                    s = client.stats
                    print(
                        f"FPS: {s.fps:.1f} | "
                        f"Inference: {s.avg_inference_ms:.1f}ms | "
                        f"E2E: {s.avg_e2e_ms:.1f}ms | "
                        f"Cache: {s.cache_hit_rate:.0f}%"
                    )

            # Throttle to target FPS
            elapsed = time.monotonic() - loop_start
            sleep_time = frame_interval - elapsed
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    except KeyboardInterrupt:
        pass
    finally:
        source.release()
        if show_preview:
            try:
                cv2.destroyAllWindows()
            except cv2.error as exc:
                logger.debug("Ignoring OpenCV window cleanup failure: %s", exc)
        await client.close()
        ws_task.cancel()

    # Print final stats
    s = client.stats
    print(f"\n--- Session stats ---")
    print(f"Frames sent:     {s.frames_sent}")
    print(f"Results received: {s.results_received}")
    print(f"Avg FPS:         {s.fps:.1f}")
    print(f"Avg inference:   {s.avg_inference_ms:.1f}ms")
    print(f"Avg E2E:         {s.avg_e2e_ms:.1f}ms")
    print(f"Cache hit rate:  {s.cache_hit_rate:.1f}%")


def main():
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        asyncio.run(run_pipeline(args))
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(130)


if __name__ == "__main__":
    main()
