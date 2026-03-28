import logging

import torch
import numpy as np
from ultralytics import YOLO

logger = logging.getLogger(__name__)


class YOLOInferenceEngine:
    def __init__(self, model_path: str):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info("Loading YOLO model from %s on %s", model_path, self.device)

        self.model = YOLO(model_path)

        # Warm up
        dummy = np.zeros((640, 640, 3), dtype=np.uint8)
        self.model.predict(dummy, verbose=False, device=self.device)
        logger.info("Model loaded and warmed up")

    def infer(self, images: list[np.ndarray]) -> list:
        """Run inference on a list of images. Returns YOLO Results list."""
        results = self.model.predict(
            images,
            verbose=False,
            device=self.device,
        )
        return results
