import logging
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ultralytics import YOLO

from common.config import settings

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("triton-model-init")

TRITON_EXPORT_BATCH_SIZE = 8


def _render_config(metadata_json: str) -> str:
    return f"""name: "{settings.triton_model_name}"
platform: "onnxruntime_onnx"
max_batch_size: {TRITON_EXPORT_BATCH_SIZE}

dynamic_batching {{
  preferred_batch_size: [4, 8]
  max_queue_delay_microseconds: {settings.triton_dynamic_batch_delay_us}
}}

parameters {{
  key: "metadata"
  value {{
    string_value: "{metadata_json}"
  }}
}}
"""


def _serialize_metadata(metadata: dict) -> str:
    # Ultralytics' Triton client parses this field with ast.literal_eval,
    # so it must be a Python literal rather than JSON.
    literal = repr(metadata)
    return literal.replace("\\", "\\\\").replace('"', '\\"')


def main():
    logger.info("Exporting %s to Triton model repository", settings.yolo_model_path)
    model = YOLO(settings.yolo_model_path)
    metadata: list[dict] = []

    def export_callback(exporter):
        metadata.append(exporter.metadata)

    model.add_callback("on_export_end", export_callback)
    onnx_file = model.export(
        format="onnx",
        dynamic=True,
        nms=True,
        batch=TRITON_EXPORT_BATCH_SIZE,
        simplify=False,
    )

    repo_root = Path(settings.triton_model_repository)
    model_root = repo_root / settings.triton_model_name
    version_root = model_root / "1"

    if model_root.exists():
        shutil.rmtree(model_root)

    version_root.mkdir(parents=True, exist_ok=True)
    shutil.move(str(onnx_file), version_root / "model.onnx")

    escaped_metadata = _serialize_metadata(metadata[0] if metadata else {})
    config_path = model_root / "config.pbtxt"
    config_path.write_text(_render_config(escaped_metadata), encoding="utf-8")

    logger.info("Wrote Triton repository to %s", model_root)


if __name__ == "__main__":
    main()
