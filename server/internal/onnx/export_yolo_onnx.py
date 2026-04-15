#!/usr/bin/env python3
"""Export a YOLO .pt checkpoint to ONNX for Triton model loading.

Example:
  python server/internal/onnx/export_yolo_onnx.py \
        --weights /path/to/yolov8n.pt \
        --output server/internal/onnx/yolov8n.onnx
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export YOLO weights to ONNX (defaults tuned for Triton compatibility)."
    )
    src_group = parser.add_mutually_exclusive_group(required=True)
    src_group.add_argument(
        "--weights",
        help="Path to YOLO .pt weights file.",
    )
    src_group.add_argument(
        "--weights-url",
        help="GitHub URL to YOLO .pt weights file (blob/raw/releases URLs supported).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path for the exported .onnx model.",
    )
    parser.add_argument(
        "--imgsz",
        type=int,
        default=640,
        help="Export image size (default: 640).",
    )
    parser.add_argument(
        "--opset",
        type=int,
        default=12,
        help="ONNX opset version (default: 12 for broad Triton compatibility).",
    )
    parser.add_argument(
        "--simplify",
        action="store_true",
        help="Enable ONNX graph simplification during export.",
    )
    return parser.parse_args()


def normalize_github_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("weights URL must start with http:// or https://")

    host = parsed.netloc.lower()
    path = parsed.path

    # Convert github.com/<org>/<repo>/blob/<ref>/<path> to raw URL.
    if host == "github.com":
        parts = path.strip("/").split("/")
        if len(parts) >= 5 and parts[2] == "blob":
            owner, repo, _, ref = parts[:4]
            file_path = "/".join(parts[4:])
            return f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{file_path}"

    return url


def download_weights(url: str) -> Path:
    download_url = normalize_github_url(url)
    parsed = urllib.parse.urlparse(download_url)
    filename = Path(parsed.path).name or "weights.pt"
    if not filename.endswith(".pt"):
        filename = f"{filename}.pt"

    tmp_dir = Path(tempfile.mkdtemp(prefix="owl-yolo-weights-"))
    dst = tmp_dir / filename

    req = urllib.request.Request(download_url, headers={"User-Agent": "owl-export-script"})
    with urllib.request.urlopen(req, timeout=120) as resp:  # nosec B310 - user-provided URL is expected behavior
        if resp.status != 200:
            raise RuntimeError(f"download failed with status {resp.status}")
        dst.write_bytes(resp.read())

    if dst.stat().st_size == 0:
        raise RuntimeError("downloaded file is empty")

    return dst


def main() -> int:
    args = parse_args()

    if args.weights:
        weights = Path(args.weights).expanduser().resolve()
    else:
        try:
            weights = download_weights(args.weights_url)
            print(f"downloaded weights: {weights}")
        except Exception as exc:
            print(f"error: failed to download weights: {exc}", file=sys.stderr)
            return 1

    output = Path(args.output).expanduser().resolve()

    if not weights.exists():
        print(f"error: weights file not found: {weights}", file=sys.stderr)
        return 1

    try:
        from ultralytics import YOLO
    except ImportError:
        print(
            "error: ultralytics is not installed. Install it with: pip install ultralytics",
            file=sys.stderr,
        )
        return 1

    output.parent.mkdir(parents=True, exist_ok=True)

    model = YOLO(str(weights))
    result_path = Path(
        model.export(
            format="onnx",
            imgsz=args.imgsz,
            opset=args.opset,
            simplify=args.simplify,
        )
    )

    # Ultralytics writes next to the weights file by default; move it to the requested output path.
    if result_path.resolve() != output:
        output.write_bytes(result_path.read_bytes())

    print(f"exported: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
