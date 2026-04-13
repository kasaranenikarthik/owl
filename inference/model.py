import logging
import threading
import time
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass

import numpy as np
import torch
from ultralytics import YOLO

from common.config import settings
from common.metrics import (
    triton_inflight_requests,
    triton_request_failures_total,
    triton_requests_total,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InferenceOutput:
    result: object
    inference_time_ms: float


class InferenceEngine(ABC):
    @abstractmethod
    def infer(self, images: list[np.ndarray]) -> list[InferenceOutput]:
        raise NotImplementedError

    def close(self):
        """Release backend resources if needed."""


class LocalYOLOInferenceEngine(InferenceEngine):
    def __init__(self, model_path: str):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info("Loading local YOLO model from %s on %s", model_path, self.device)

        self.model = YOLO(model_path)

        dummy = np.zeros((640, 640, 3), dtype=np.uint8)
        self.model.predict(dummy, verbose=False, device=self.device)
        logger.info("Local YOLO model loaded and warmed up")

    def infer(self, images: list[np.ndarray]) -> list[InferenceOutput]:
        results = self.model.predict(
            images,
            verbose=False,
            device=self.device,
        )
        return [
            InferenceOutput(
                result=result,
                inference_time_ms=float(result.speed.get("inference", 0.0)),
            )
            for result in results
        ]


class TritonRemoteInferenceEngine(InferenceEngine):
    def __init__(
        self,
        base_url: str,
        model_name: str,
        max_workers: int,
        request_timeout_ms: int,
        worker_id: str,
    ):
        self.base_url = base_url.rstrip("/")
        self.model_name = model_name
        self.model_url = f"{self.base_url}/{self.model_name}"
        self.request_timeout_seconds = request_timeout_ms / 1000.0
        self.worker_id = worker_id
        self._thread_local = threading.local()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="triton-client",
        )

        logger.info("Connecting to Triton model at %s", self.model_url)
        self._verify_model_ready()
        self._warm_up()
        logger.info("Triton model is ready")

    def infer(self, images: list[np.ndarray]) -> list[InferenceOutput]:
        futures = [self._executor.submit(self._infer_one, image) for image in images]
        _, not_done = wait(
            futures,
            timeout=self.request_timeout_seconds,
            return_when=ALL_COMPLETED,
        )

        if not_done:
            for future in not_done:
                future.cancel()
            triton_request_failures_total.labels(
                worker_id=self.worker_id,
                reason="timeout",
            ).inc(len(not_done))
            raise TimeoutError(
                f"Triton timed out for {len(not_done)} request(s) after "
                f"{self.request_timeout_seconds:.3f}s"
            )

        return [future.result() for future in futures]

    def close(self):
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _verify_model_ready(self):
        readiness_paths = (
            "/v2/health/live",
            "/v2/health/ready",
            f"/v2/models/{self.model_name}/ready",
        )
        for path in readiness_paths:
            self._get(path)

    def _warm_up(self):
        dummy = np.zeros((640, 640, 3), dtype=np.uint8)
        self._get_model().predict(dummy, verbose=False)

    def _infer_one(self, image: np.ndarray) -> InferenceOutput:
        triton_requests_total.labels(worker_id=self.worker_id).inc()
        triton_inflight_requests.labels(worker_id=self.worker_id).inc()

        try:
            model = self._get_model()
            start = time.monotonic()
            results = model.predict(image, verbose=False)
            elapsed_ms = (time.monotonic() - start) * 1000.0

            if not results:
                raise RuntimeError("Triton returned no inference results")

            return InferenceOutput(result=results[0], inference_time_ms=elapsed_ms)
        except Exception as exc:
            triton_request_failures_total.labels(
                worker_id=self.worker_id,
                reason=exc.__class__.__name__.lower(),
            ).inc()
            raise
        finally:
            triton_inflight_requests.labels(worker_id=self.worker_id).dec()

    def _get_model(self) -> YOLO:
        model = getattr(self._thread_local, "model", None)
        if model is None:
            model = YOLO(self.model_url, task="detect")
            self._thread_local.model = model
        return model

    def _get(self, path: str):
        try:
            with urllib.request.urlopen(
                f"{self.base_url}{path}",
                timeout=self.request_timeout_seconds,
            ) as response:
                response.read()
        except urllib.error.URLError as exc:
            raise RuntimeError(
                f"Triton readiness check failed for {path}: {exc}"
            ) from exc


def create_inference_engine(worker_id: str) -> InferenceEngine:
    if settings.use_triton:
        try:
            return TritonRemoteInferenceEngine(
                base_url=settings.triton_http_url,
                model_name=settings.triton_model_name,
                max_workers=settings.triton_max_inflight,
                request_timeout_ms=settings.triton_request_timeout_ms,
                worker_id=worker_id,
            )
        except Exception:
            logger.exception(
                "Failed to initialize Triton backend; falling back to local YOLO"
            )

    return LocalYOLOInferenceEngine(settings.yolo_model_path)
