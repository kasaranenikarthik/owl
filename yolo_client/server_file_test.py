"""Dummy WebSocket server for local yolo_client testing.

Protocol:
- Accepts binary JPEG frames on /ws
- Sends an initial {"type":"connected","client_id":"..."} message
- Sends fake detection responses for each received frame
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
import uuid

import websockets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run dummy WebSocket YOLO server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=8080, help="Bind port")
    parser.add_argument("--path", default="/ws", help="Expected websocket path")
    return parser


async def make_handler(expected_path: str):
    async def handler(ws):
        if ws.request.path != expected_path:
            await ws.close(code=1008, reason="invalid path")
            return

        client_id = str(uuid.uuid4())
        await ws.send(json.dumps({"type": "connected", "client_id": client_id}))

        async for msg in ws:
            if not isinstance(msg, (bytes, bytearray)):
                continue

            now = time.time()
            detections = []
            if random.random() < 0.7:
                detections.append(
                    {
                        "bounding_box": {
                            "x1": random.uniform(20, 200),
                            "y1": random.uniform(20, 200),
                            "x2": random.uniform(220, 500),
                            "y2": random.uniform(220, 420),
                            "confidence": round(random.uniform(0.55, 0.98), 2),
                            "class_id": 0,
                            "class_name": "person",
                        }
                    }
                )

            payload = {
                "detections": detections,
                "inference_ms": round(random.uniform(8, 35), 2),
                "from_cache": random.random() < 0.15,
                "timestamp": now,
            }
            await ws.send(json.dumps(payload))

    return handler


async def run(host: str, port: int, path: str):
    handler = await make_handler(path)
    async with websockets.serve(handler, host, port, max_size=2**20):
        print(f"Dummy WS server listening on ws://{host}:{port}{path}")
        await asyncio.Future()


def main():
    args = build_parser().parse_args()
    asyncio.run(run(args.host, args.port, args.path))


if __name__ == "__main__":
    main()
