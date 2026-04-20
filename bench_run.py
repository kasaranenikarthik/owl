from __future__ import annotations

import argparse
import http.client
import json
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from yolo_client.benchmark import run_benchmark_session

logger = logging.getLogger("bench_run")

ROOT = Path(__file__).resolve().parent
REPORT_DIR = ROOT / "report"
RESULTS_PATH = REPORT_DIR / "bench_results.json"
SUMMARY_PATH = REPORT_DIR / "benchmark_summary.md"
TMP_CONFIG_DIR = ROOT / "report" / ".tmp" / "bench_configs"

VIDEO_PATH = ROOT / "data" / "Traffic_test_hd_1920_1080_30fps.mp4"
OFFERED_FPS_VALUES = (5, 7, 10, 15, 20, 30)
WORKER_VALUES = (1, 2, 3, 4, 5, 6, 8)
QUEUE_DELAY_VALUES_MS = (0, 5, 10, 25, 50)
BATCH_SIZE_VALUES = (1, 4, 8, 16)

DEFAULT_WARMUP_SECONDS = 30.0
DEFAULT_MEASURE_SECONDS = 120.0
DEFAULT_SETTLE_SECONDS = 5.0
DEFAULT_JPEG_QUALITY = 80
CACHE_SHORTLIST_SIZE = 3

BASELINE_WORKERS = 4
BASELINE_SIMILARITY_THRESHOLD = -1
BASELINE_MAX_BATCH_SIZE = 8
BASELINE_PREFERRED_BATCH_SIZE = (8,)
BASELINE_QUEUE_DELAY_US = 2000
BASELINE_INSTANCE_COUNT = 1
BASELINE_CLIENT_COUNT = 1

TOPIC_PARTITIONS = 3
TOPICS = ("frames", "detections")

PROM_QUERY_NAMES = (
    "gateway_p95_e2e_ms",
    "worker_queue_p95_ms",
    "decode_p95_ms",
    "preprocess_p95_ms",
    "triton_roundtrip_p95_ms",
    "postprocess_p95_ms",
    "microbatch_avg_size",
    "microbatch_assembly_p95_ms",
    "frames_consumed",
    "queue_full_count",
    "stale_count",
    "superseded_count",
    "cache_hit_rate_pct",
    "triton_request_ms",
    "triton_queue_ms",
    "triton_compute_ms",
    "triton_avg_batch_size",
    "gpu_util_avg_pct",
)


@dataclass(frozen=True)
class RunConfig:
    stage: str
    workers: int
    fps: int
    queue_delay_us: int
    max_batch_size: int
    preferred_batch_size: tuple[int, ...]
    similarity_threshold: int
    instance_count: int = 1
    client_count: int = 1

    @property
    def batch_label(self) -> str:
        return "-".join(str(size) for size in self.preferred_batch_size)

    @property
    def queue_delay_ms(self) -> float:
        return self.queue_delay_us / 1000.0

    def run_id(self) -> str:
        return (
            f"{self.stage}_w{self.workers}_fps{self.fps}_d{self.queue_delay_us}"
            f"_mb{self.max_batch_size}_pb{self.batch_label}_thr{self.similarity_threshold}"
            f"_i{self.instance_count}_c{self.client_count}"
        )

    def to_record(self) -> dict[str, Any]:
        data = asdict(self)
        data["preferred_batch_size"] = list(self.preferred_batch_size)
        data["queue_delay_ms"] = self.queue_delay_ms
        return data


@dataclass
class RunOutcome:
    run_id: str
    config: dict[str, Any]
    timings: dict[str, float]
    client: dict[str, Any]
    prometheus: dict[str, float]
    guardrails: dict[str, Any]
    ranking: dict[str, Any]
    notes: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "config": self.config,
            "timings": self.timings,
            "client": self.client,
            "prometheus": self.prometheus,
            "guardrails": self.guardrails,
            "ranking": self.ranking,
            "notes": self.notes,
        }


def parse_args() -> argparse.Namespace:
    def parse_csv_ints(value: str) -> tuple[int, ...]:
        items: list[int] = []
        for part in value.split(","):
            stripped = part.strip()
            if not stripped:
                continue
            try:
                items.append(int(stripped))
            except ValueError as exc:
                raise argparse.ArgumentTypeError(f"expected comma-separated integers, got {value!r}") from exc
        if not items:
            raise argparse.ArgumentTypeError("expected at least one integer")
        return tuple(dict.fromkeys(items))

    def validate_filter_values(
        parser: argparse.ArgumentParser,
        flag: str,
        selected: tuple[int, ...] | None,
        allowed: tuple[int, ...],
    ) -> None:
        if selected is None:
            return
        invalid = sorted(set(selected) - set(allowed))
        if invalid:
            parser.error(f"{flag} contains unsupported values {invalid}; allowed values are {sorted(allowed)}")

    parser = argparse.ArgumentParser(description="Run staged OWL benchmark sweeps.")
    parser.add_argument(
        "--video-path",
        default=str(VIDEO_PATH),
        help="Path to the benchmark video file.",
    )
    parser.add_argument(
        "--server-url",
        default="ws://localhost:8080/ws",
        help="Gateway WebSocket URL.",
    )
    parser.add_argument(
        "--prometheus-url",
        default=f"http://localhost:{os.getenv('PROMETHEUS_PORT', '9090')}",
        help="Prometheus base URL.",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=DEFAULT_WARMUP_SECONDS,
        help="Warmup duration before each measured run.",
    )
    parser.add_argument(
        "--measure-seconds",
        type=float,
        default=DEFAULT_MEASURE_SECONDS,
        help="Measured benchmark duration per run.",
    )
    parser.add_argument(
        "--settle-seconds",
        type=float,
        default=DEFAULT_SETTLE_SECONDS,
        help="How long to wait after sending finishes before closing clients.",
    )
    parser.add_argument(
        "--jpeg-quality",
        type=int,
        default=DEFAULT_JPEG_QUALITY,
        help="JPEG quality for encoded frames.",
    )
    parser.add_argument(
        "--send-queue-size",
        type=int,
        default=10,
        help="Per-client benchmark send queue size.",
    )
    parser.add_argument(
        "--client-count",
        type=int,
        default=1,
        help="Number of concurrent benchmark clients to run.",
    )
    parser.add_argument(
        "--resize-width",
        type=int,
        help="Optional client-side resize width before JPEG encode.",
    )
    parser.add_argument(
        "--resize-height",
        type=int,
        help="Optional client-side resize height before JPEG encode.",
    )
    parser.add_argument(
        "--through-stage",
        choices=("baseline", "stage2", "stage3", "stage4"),
        default="stage4",
        help="Stop after completing the selected stage.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reuse results already present in report/bench_results.json.",
    )
    parser.add_argument(
        "--workers",
        type=parse_csv_ints,
        help="Optional comma-separated worker-count filter, e.g. 2 or 1,2,3.",
    )
    parser.add_argument(
        "--fps",
        type=parse_csv_ints,
        help="Optional comma-separated offered-FPS filter, e.g. 5,30.",
    )
    parser.add_argument(
        "--queue-delay-ms",
        type=parse_csv_ints,
        help="Optional comma-separated Triton queue-delay filter in milliseconds, e.g. 0,5,10,25,50.",
    )
    parser.add_argument(
        "--batch-sizes",
        type=parse_csv_ints,
        help="Optional comma-separated max/preferred batch-size filter, e.g. 1,4,8,16.",
    )
    parser.add_argument(
        "--results-path",
        default=str(RESULTS_PATH),
        help="Where to write benchmark JSON results.",
    )
    parser.add_argument(
        "--summary-path",
        default=str(SUMMARY_PATH),
        help="Where to write the benchmark Markdown summary.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Log verbosity.",
    )
    args = parser.parse_args()
    if (args.resize_width is None) != (args.resize_height is None):
        parser.error("--resize-width and --resize-height must be provided together")
    if args.resize_width is not None and args.resize_width <= 0:
        parser.error("--resize-width must be > 0")
    if args.resize_height is not None and args.resize_height <= 0:
        parser.error("--resize-height must be > 0")
    if args.client_count <= 0:
        parser.error("--client-count must be > 0")
    validate_filter_values(parser, "--workers", args.workers, WORKER_VALUES)
    validate_filter_values(parser, "--fps", args.fps, OFFERED_FPS_VALUES)
    validate_filter_values(parser, "--queue-delay-ms", args.queue_delay_ms, QUEUE_DELAY_VALUES_MS)
    validate_filter_values(parser, "--batch-sizes", args.batch_sizes, BATCH_SIZE_VALUES)
    return args


def render_triton_gpu_config(config: RunConfig) -> str:
    preferred = ", ".join(str(size) for size in config.preferred_batch_size)
    return (
        'name: "yolov8s"\n'
        'platform: "onnxruntime_onnx"\n'
        f"max_batch_size: {config.max_batch_size}\n\n"
        "input [{\n"
        '  name: "images"\n'
        "  data_type: TYPE_FP32\n"
        "  dims: [3, 640, 640]\n"
        " }]\n\n"
        "output [{\n"
        '   name: "output0"\n'
        "   data_type: TYPE_FP32\n"
        "  dims: [-1, -1]\n"
        "}]\n\n"
        "dynamic_batching {\n"
        f"  preferred_batch_size: [{preferred}]\n"
        f"  max_queue_delay_microseconds: {config.queue_delay_us}\n"
        "}\n\n"
        "instance_group [{\n"
        f"  count: {config.instance_count}\n"
        "  kind: KIND_GPU\n"
        "}]\n"
    )


def stage_limits() -> dict[str, int]:
    return {"baseline": 1, "stage2": 2, "stage3": 3, "stage4": 4}


def run_stage_number(stage_name: str) -> int:
    return stage_limits()[stage_name]


def matches_selected(value: int, selected: tuple[int, ...] | None) -> bool:
    return selected is None or value in selected


def matches_seed_filters(*, workers: int, fps: int, args: argparse.Namespace) -> bool:
    return matches_selected(workers, args.workers) and matches_selected(fps, args.fps)


def matches_stage3_knob_filters(
    *,
    queue_delay_ms: int,
    batch_size: int,
    args: argparse.Namespace,
) -> bool:
    return matches_selected(queue_delay_ms, args.queue_delay_ms) and matches_selected(batch_size, args.batch_sizes)


def init_results_store() -> dict[str, Any]:
    return {
        "metadata": {
            "version": 1,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "baseline_by_fps": {},
        "runs": [],
    }


def load_results(path: Path, resume: bool) -> dict[str, Any]:
    if resume and path.exists():
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    return init_results_store()


def save_results(path: Path, results: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2, sort_keys=True)
        handle.write("\n")


def upsert_run(results: dict[str, Any], outcome: RunOutcome) -> None:
    record = outcome.to_record()
    for index, existing in enumerate(results["runs"]):
        if existing["run_id"] == outcome.run_id:
            results["runs"][index] = record
            break
    else:
        results["runs"].append(record)


def run_command(
    args: list[str],
    *,
    env: dict[str, str] | None = None,
    check: bool = True,
    allow_failure: bool = False,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    result = subprocess.run(
        args,
        cwd=str(ROOT),
        env=merged_env,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0 and check and not allow_failure:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(args)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


def docker_compose(args: list[str], *, env: dict[str, str] | None = None, allow_failure: bool = False) -> subprocess.CompletedProcess[str]:
    return run_command(["docker", "compose", *args], env=env, allow_failure=allow_failure)


def validate_compose_config(env: dict[str, str]) -> None:
    services = docker_compose(["config", "--services"], env=env)
    resolved = docker_compose(["config"], env=env)
    logger.info("Compose preflight services:\n%s", services.stdout.strip())
    logger.info("Compose preflight config:\n%s", resolved.stdout.strip())


def ensure_static_stack(env: dict[str, str]) -> None:
    validate_compose_config(env)
    docker_compose(
        [
            "up",
            "-d",
            "--no-deps",
            "zookeeper",
            "kafka",
            "redis",
        ],
        env=env,
    )
    logger.info("Started static benchmark services: zookeeper, kafka, redis")
    docker_compose(
        [
            "up",
            "-d",
            "--no-deps",
            "prometheus",
        ],
        env=env,
    )
    logger.info("Started prometheus without kafka-exporter or grafana dependencies")


def stop_run_services() -> None:
    docker_compose(["rm", "-sf", "gateway", "inference", "triton", "model-loader"], allow_failure=True)


def wait_for_topic_absence(topic: str, timeout_seconds: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        result = docker_compose(
            [
                "exec",
                "-T",
                "kafka",
                "bash",
                "-lc",
                "kafka-topics --bootstrap-server kafka:9092 --list",
            ]
        )
        topics = {line.strip() for line in result.stdout.splitlines() if line.strip()}
        if topic not in topics:
            return
        time.sleep(1.0)
    raise RuntimeError(f"Timed out waiting for topic deletion: {topic}")


def reset_runtime_state() -> None:
    docker_compose(["exec", "-T", "redis", "redis-cli", "FLUSHALL"])
    for topic in TOPICS:
        docker_compose(
            [
                "exec",
                "-T",
                "kafka",
                "bash",
                "-lc",
                f"kafka-topics --bootstrap-server kafka:9092 --delete --if-exists --topic {topic}",
            ],
            allow_failure=True,
        )
        wait_for_topic_absence(topic)
        docker_compose(
            [
                "exec",
                "-T",
                "kafka",
                "bash",
                "-lc",
                (
                    "kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists "
                    f"--topic {topic} --partitions {TOPIC_PARTITIONS} --replication-factor 1"
                ),
            ]
        )


def wait_for_http(url: str, timeout_seconds: float = 60.0, expected_status: int = 200) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5.0) as response:
                if response.status == expected_status:
                    return
                last_error = RuntimeError(f"unexpected status {response.status} for {url}")
        # Services can accept a TCP connection before they are ready to return a
        # complete HTTP response, so retry transport-level disconnects as well.
        except (urllib.error.URLError, urllib.error.HTTPError, http.client.HTTPException, OSError, TimeoutError) as exc:
            last_error = exc
        time.sleep(1.0)
    raise RuntimeError(f"Timed out waiting for {url}: {last_error}")


def wait_for_container_exit_success(service: str, timeout_seconds: float = 180.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        result = docker_compose(
            ["ps", "-a", "--format", "json", service],
            allow_failure=True,
        )
        payload = result.stdout.strip()
        if not payload:
            time.sleep(1.0)
            continue

        state: dict[str, Any] | None = None
        parsed = json.loads(payload)
        if isinstance(parsed, list):
            if parsed:
                state = parsed[0]
        elif isinstance(parsed, dict):
            state = parsed

        if state is None:
            time.sleep(1.0)
            continue

        status = state.get("State", "")
        exit_code = state.get("ExitCode")
        if status == "exited" and exit_code == 0:
            return
        if status == "exited" and exit_code not in (None, 0):
            logs = docker_compose(["logs", "--no-color", service], allow_failure=True)
            raise RuntimeError(
                f"{service} exited with code {exit_code}\nstdout/stderr:\n{logs.stdout}\n{logs.stderr}"
            )
        time.sleep(1.0)
    raise RuntimeError(f"Timed out waiting for {service} to exit successfully")


def start_run_services(env: dict[str, str]) -> None:
    logger.info(
        "Benchmark startup order: model-loader -> triton -> gateway/inference; "
        "prometheus is started without kafka-exporter or grafana"
    )
    docker_compose(
        ["up", "-d", "--build", "--force-recreate", "--no-deps", "model-loader"],
        env=env,
    )
    wait_for_container_exit_success("model-loader")

    docker_compose(
        ["up", "-d", "--force-recreate", "--no-deps", "triton"],
        env=env,
    )
    wait_for_http("http://localhost:8000/v2/health/ready")
    wait_for_http("http://localhost:8000/v2/models/yolov8s/ready")

    docker_compose(
        ["up", "-d", "--build", "--force-recreate", "--no-deps", "gateway", "inference"],
        env=env,
    )
    wait_for_http("http://localhost:8080/health")
    wait_for_http("http://localhost:19090/readyz")
    wait_for_http("http://localhost:19091/readyz")


def host_path_to_container_path(path: Path) -> str:
    relative = path.resolve().relative_to(ROOT.resolve())
    return "/repo/" + relative.as_posix()


def write_temp_triton_config(config: RunConfig) -> Path:
    TMP_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    target = TMP_CONFIG_DIR / f"{config.run_id()}.pbtxt"
    target.write_text(render_triton_gpu_config(config), encoding="utf-8")
    return target


def build_compose_env(config: RunConfig, temp_config_path: Path) -> dict[str, str]:
    return {
        "TRITON_EXECUTION_TARGET": "gpu",
        "TRITON_GPU_CONFIG_PATH": host_path_to_container_path(temp_config_path),
        "NUM_WORKERS": str(config.workers),
        "SIMILARITY_THRESHOLD": str(config.similarity_threshold),
    }


def prom_query(base_url: str, query: str, query_time: float) -> float | None:
    encoded = urllib.parse.urlencode({"query": query, "time": f"{query_time:.3f}"})
    url = f"{base_url.rstrip('/')}/api/v1/query?{encoded}"
    with urllib.request.urlopen(url, timeout=15.0) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {query}")
    results = payload.get("data", {}).get("result", [])
    if not results:
        return None
    value = results[0]["value"][1]
    if value in ("NaN", "Inf", "+Inf", "-Inf"):
        return None
    return float(value)


def collect_prometheus_metrics(base_url: str, query_time: float, window_seconds: float) -> dict[str, float]:
    window = f"{max(int(round(window_seconds)), 1)}s"
    queries = {
        "gateway_p95_e2e_ms": (
            f"histogram_quantile(0.95, sum(rate(gateway_e2e_latency_seconds_bucket[{window}])) by (le)) * 1000"
        ),
        "worker_queue_p95_ms": (
            f"histogram_quantile(0.95, sum(rate(yolo_worker_queue_wait_seconds_bucket[{window}])) by (le)) * 1000"
        ),
        "decode_p95_ms": (
            f"histogram_quantile(0.95, sum(rate(yolo_decode_seconds_bucket[{window}])) by (le)) * 1000"
        ),
        "preprocess_p95_ms": (
            f"histogram_quantile(0.95, sum(rate(yolo_preprocess_seconds_bucket[{window}])) by (le)) * 1000"
        ),
        "triton_roundtrip_p95_ms": (
            f"histogram_quantile(0.95, sum(rate(yolo_triton_roundtrip_seconds_bucket[{window}])) by (le)) * 1000"
        ),
        "postprocess_p95_ms": (
            f"histogram_quantile(0.95, sum(rate(yolo_postprocess_seconds_bucket[{window}])) by (le)) * 1000"
        ),
        "microbatch_avg_size": (
            f"sum(rate(yolo_microbatch_size_sum[{window}])) / "
            f"clamp_min(sum(rate(yolo_microbatch_size_count[{window}])), 1)"
        ),
        "microbatch_assembly_p95_ms": (
            f"histogram_quantile(0.95, sum(rate(yolo_microbatch_assembly_seconds_bucket[{window}])) by (le)) * 1000"
        ),
        "frames_consumed": f"sum(increase(yolo_frames_consumed_total[{window}]))",
        "queue_full_count": (
            f"sum(increase(yolo_frames_dropped_reason_total{{reason=\"queue_full\"}}[{window}]))"
        ),
        "stale_count": f"sum(increase(yolo_frames_dropped_reason_total{{reason=\"stale\"}}[{window}]))",
        "superseded_count": (
            f"sum(increase(yolo_frames_dropped_reason_total{{reason=\"superseded\"}}[{window}]))"
        ),
        "cache_hit_rate_pct": (
            f"100 * sum(increase(gateway_cache_hits_total[{window}])) / "
            f"clamp_min(sum(increase(gateway_cache_hits_total[{window}])) + sum(increase(gateway_cache_misses_total[{window}])), 1)"
        ),
        "triton_request_ms": (
            f"sum(rate(nv_inference_request_duration_us[{window}])) / "
            f"clamp_min(sum(rate(nv_inference_request_success[{window}])), 1) / 1000"
        ),
        "triton_queue_ms": (
            f"sum(rate(nv_inference_queue_duration_us[{window}])) / "
            f"clamp_min(sum(rate(nv_inference_request_success[{window}])), 1) / 1000"
        ),
        "triton_compute_ms": (
            f"sum(rate(nv_inference_compute_infer_duration_us[{window}])) / "
            f"clamp_min(sum(rate(nv_inference_count[{window}])), 1) / 1000"
        ),
        "triton_avg_batch_size": (
            f"sum(rate(nv_inference_request_success[{window}])) / "
            f"clamp_min(sum(rate(nv_inference_exec_count[{window}])), 1)"
        ),
        "gpu_util_avg_pct": "avg(nv_gpu_utilization) * 100",
    }

    metrics: dict[str, float] = {}
    for name in PROM_QUERY_NAMES:
        value = prom_query(base_url, queries[name], query_time)
        metrics[name] = value if value is not None else 0.0

    frames_consumed = metrics.get("frames_consumed", 0.0)
    if frames_consumed > 0:
        metrics["queue_full_rate_pct"] = metrics["queue_full_count"] / frames_consumed * 100.0
        metrics["stale_rate_pct"] = metrics["stale_count"] / frames_consumed * 100.0
        metrics["superseded_rate_pct"] = metrics["superseded_count"] / frames_consumed * 100.0
    else:
        metrics["queue_full_rate_pct"] = 0.0
        metrics["stale_rate_pct"] = 0.0
        metrics["superseded_rate_pct"] = 0.0

    return metrics


def build_baseline_map(results: dict[str, Any]) -> dict[int, dict[str, float]]:
    baseline: dict[int, dict[str, float]] = {}
    for run in results["runs"]:
        if run["config"]["stage"] != "baseline":
            continue
        fps = int(run["config"]["fps"])
        baseline[fps] = {
            "delivered_fps": float(run["client"]["aggregate"]["delivered_fps"]),
            "client_drop_rate": float(run["client"]["aggregate"]["client_drop_rate"]),
            "queue_full_rate_pct": float(run["prometheus"].get("queue_full_rate_pct", 0.0)),
            "stale_rate_pct": float(run["prometheus"].get("stale_rate_pct", 0.0)),
            "gateway_p95_e2e_ms": float(run["prometheus"].get("gateway_p95_e2e_ms", 0.0)),
        }
    results["baseline_by_fps"] = {str(fps): values for fps, values in sorted(baseline.items())}
    return baseline


def evaluate_guardrails(config: RunConfig, client: dict[str, Any], prometheus: dict[str, float], baseline: dict[int, dict[str, float]]) -> dict[str, Any]:
    aggregate = client["aggregate"]
    base = baseline.get(config.fps)
    if base is None:
        return {"hard_pass": False, "checks": {}, "reason": f"missing baseline for fps={config.fps}"}

    checks = {
        "results_received": {
            "metric": aggregate["results_received"],
            "threshold": 0,
            "pass": aggregate["results_received"] > 0,
        },
        "delivered_fps": {
            "metric": aggregate["delivered_fps"],
            "threshold": 0.60 * base["delivered_fps"],
            "pass": aggregate["delivered_fps"] >= 0.60 * base["delivered_fps"],
        },
        "client_drop_rate": {
            "metric": aggregate["client_drop_rate"],
            "threshold": base["client_drop_rate"] + 25.0,
            "pass": aggregate["client_drop_rate"] <= base["client_drop_rate"] + 25.0,
        },
        "queue_full_rate_pct": {
            "metric": prometheus["queue_full_rate_pct"],
            "threshold": base["queue_full_rate_pct"] + 20.0,
            "pass": prometheus["queue_full_rate_pct"] <= base["queue_full_rate_pct"] + 20.0,
        },
        "stale_rate_pct": {
            "metric": prometheus["stale_rate_pct"],
            "threshold": base["stale_rate_pct"] + 20.0,
            "pass": prometheus["stale_rate_pct"] <= base["stale_rate_pct"] + 20.0,
        },
        "gateway_p95_e2e_ms": {
            "metric": prometheus["gateway_p95_e2e_ms"],
            "threshold": 3.0 * base["gateway_p95_e2e_ms"],
            "pass": prometheus["gateway_p95_e2e_ms"] <= 3.0 * base["gateway_p95_e2e_ms"],
        },
    }
    return {
        "hard_pass": all(check["pass"] for check in checks.values()),
        "checks": checks,
    }


def soft_penalty_score(config: RunConfig, client: dict[str, Any], prometheus: dict[str, float], baseline: dict[int, dict[str, float]]) -> float:
    base = baseline[config.fps]
    delivered_delta = max(0.0, base["delivered_fps"] - client["aggregate"]["delivered_fps"])
    client_drop_delta = max(0.0, client["aggregate"]["client_drop_rate"] - base["client_drop_rate"])
    return (
        prometheus["superseded_rate_pct"]
        + prometheus["worker_queue_p95_ms"] / 100.0
        + client_drop_delta
        + delivered_delta
    )


def ranking_tuple(config: RunConfig, client: dict[str, Any], prometheus: dict[str, float], baseline: dict[int, dict[str, float]]) -> tuple[float, float, float, int, float]:
    return (
        prometheus["gateway_p95_e2e_ms"],
        -client["aggregate"]["delivered_fps"],
        prometheus["queue_full_rate_pct"] + prometheus["stale_rate_pct"],
        config.workers,
        soft_penalty_score(config, client, prometheus, baseline),
    )


def build_ranking(config: RunConfig, client: dict[str, Any], prometheus: dict[str, float], baseline: dict[int, dict[str, float]]) -> dict[str, Any]:
    sort_key = ranking_tuple(config, client, prometheus, baseline)
    return {
        "sort_key": list(sort_key),
        "soft_penalty_score": sort_key[-1],
    }


def execute_run(config: RunConfig, args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, float], dict[str, float], list[str]]:
    notes: list[str] = []
    if args.resize_width is not None and args.resize_height is not None:
        notes.append(f"client_resize={args.resize_width}x{args.resize_height}")
    temp_config = write_temp_triton_config(config)
    compose_env = build_compose_env(config, temp_config)

    ensure_static_stack(compose_env)
    stop_run_services()
    reset_runtime_state()
    start_run_services(compose_env)

    if args.warmup_seconds > 0:
        logger.info("Warmup %s for %.1fs", config.run_id(), args.warmup_seconds)
        asyncio_run_benchmark(
            server_url=args.server_url,
            video_path=args.video_path,
            fps=config.fps,
            client_count=config.client_count,
            duration_seconds=args.warmup_seconds,
            settle_seconds=min(args.settle_seconds, 2.0),
            jpeg_quality=args.jpeg_quality,
            send_queue_size=args.send_queue_size,
            resize_width=args.resize_width,
            resize_height=args.resize_height,
        )
        stop_run_services()
        reset_runtime_state()
        start_run_services(compose_env)

    logger.info("Measured run %s", config.run_id())
    measured_started_at = time.time()
    client = asyncio_run_benchmark(
        server_url=args.server_url,
        video_path=args.video_path,
        fps=config.fps,
        client_count=config.client_count,
        duration_seconds=args.measure_seconds,
        settle_seconds=args.settle_seconds,
        jpeg_quality=args.jpeg_quality,
        send_queue_size=args.send_queue_size,
        resize_width=args.resize_width,
        resize_height=args.resize_height,
    )
    measured_finished_at = time.time()

    window_seconds = measured_finished_at - measured_started_at
    prometheus = collect_prometheus_metrics(args.prometheus_url, measured_finished_at, window_seconds)
    timings = {
        "started_at": measured_started_at,
        "finished_at": measured_finished_at,
        "window_seconds": window_seconds,
    }

    if config.similarity_threshold == 5:
        notes.append("cache candidate")

    return client, prometheus, timings, notes


def asyncio_run_benchmark(**kwargs: Any) -> dict[str, Any]:
    import asyncio

    return asyncio.run(run_benchmark_session(**kwargs))


def baseline_config_for_fps(fps: int) -> RunConfig:
    return RunConfig(
        stage="baseline",
        workers=BASELINE_WORKERS,
        fps=fps,
        queue_delay_us=BASELINE_QUEUE_DELAY_US,
        max_batch_size=BASELINE_MAX_BATCH_SIZE,
        preferred_batch_size=BASELINE_PREFERRED_BATCH_SIZE,
        similarity_threshold=BASELINE_SIMILARITY_THRESHOLD,
        instance_count=BASELINE_INSTANCE_COUNT,
        client_count=BASELINE_CLIENT_COUNT,
    )


def build_stage2_configs(args: argparse.Namespace) -> list[RunConfig]:
    configs: list[RunConfig] = []
    for fps in OFFERED_FPS_VALUES:
        for workers in WORKER_VALUES:
            if workers == BASELINE_WORKERS:
                continue
            if not matches_seed_filters(workers=workers, fps=fps, args=args):
                continue
            configs.append(
                RunConfig(
                    stage="stage2",
                    workers=workers,
                    fps=fps,
                    queue_delay_us=BASELINE_QUEUE_DELAY_US,
                    max_batch_size=BASELINE_MAX_BATCH_SIZE,
                    preferred_batch_size=BASELINE_PREFERRED_BATCH_SIZE,
                    similarity_threshold=-1,
                    instance_count=1,
                    client_count=args.client_count,
                )
            )
    return configs


def build_stage3_configs(survivors: set[tuple[int, int]], args: argparse.Namespace) -> list[RunConfig]:
    configs: list[RunConfig] = []
    for workers, fps in sorted(survivors):
        if not matches_seed_filters(workers=workers, fps=fps, args=args):
            continue
        for queue_delay_ms in QUEUE_DELAY_VALUES_MS:
            for batch_size in BATCH_SIZE_VALUES:
                if not matches_stage3_knob_filters(
                    queue_delay_ms=queue_delay_ms,
                    batch_size=batch_size,
                    args=args,
                ):
                    continue
                configs.append(
                    RunConfig(
                        stage="stage3",
                        workers=workers,
                        fps=fps,
                        queue_delay_us=queue_delay_ms * 1000,
                        max_batch_size=batch_size,
                        preferred_batch_size=(batch_size,),
                        similarity_threshold=-1,
                        instance_count=1,
                        client_count=args.client_count,
                    )
                )
    return configs


def build_stage4_configs(
    results: dict[str, Any],
    baseline: dict[int, dict[str, float]],
    args: argparse.Namespace,
) -> list[RunConfig]:
    uncached = [
        run
        for run in results["runs"]
        if run["config"]["stage"] == "stage3"
        and run["config"]["similarity_threshold"] == -1
        and run["guardrails"].get("hard_pass")
    ]
    ranked = sorted(
        uncached,
        key=lambda run: tuple(run["ranking"]["sort_key"]),
    )

    configs: list[RunConfig] = []
    for run in ranked[:CACHE_SHORTLIST_SIZE]:
        cfg = run["config"]
        if not matches_seed_filters(workers=int(cfg["workers"]), fps=int(cfg["fps"]), args=args):
            continue
        if not matches_stage3_knob_filters(
            queue_delay_ms=int(round(float(cfg["queue_delay_ms"]))),
            batch_size=int(cfg["max_batch_size"]),
            args=args,
        ):
            continue
        configs.append(
            RunConfig(
                stage="stage4",
                workers=int(cfg["workers"]),
                fps=int(cfg["fps"]),
                queue_delay_us=int(cfg["queue_delay_us"]),
                max_batch_size=int(cfg["max_batch_size"]),
                preferred_batch_size=tuple(int(size) for size in cfg["preferred_batch_size"]),
                similarity_threshold=5,
                instance_count=int(cfg["instance_count"]),
                client_count=int(cfg["client_count"]),
            )
        )
    return configs


def find_run(results: dict[str, Any], run_id: str) -> dict[str, Any] | None:
    for run in results["runs"]:
        if run["run_id"] == run_id:
            return run
    return None


def find_uncached_peer(results: dict[str, Any], config: RunConfig) -> dict[str, Any] | None:
    for run in results["runs"]:
        peer = run["config"]
        if (
            peer["stage"] == "stage3"
            and peer["workers"] == config.workers
            and peer["fps"] == config.fps
            and peer["queue_delay_us"] == config.queue_delay_us
            and peer["max_batch_size"] == config.max_batch_size
            and tuple(peer["preferred_batch_size"]) == config.preferred_batch_size
            and peer["instance_count"] == config.instance_count
            and peer["client_count"] == config.client_count
            and peer["similarity_threshold"] == -1
        ):
            return run
    return None


def format_metric(value: float) -> str:
    return f"{value:.2f}"


def generate_summary(results: dict[str, Any], path: Path) -> None:
    baseline = build_baseline_map(results)
    lines = ["# OWL Benchmark Summary", ""]

    if baseline:
        lines.extend(
            [
                "## Baseline by FPS",
                "",
                "| Offered FPS | Delivered FPS | Client drop % | Queue full % | Stale % | Gateway p95 E2E ms |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for fps in sorted(baseline):
            item = baseline[fps]
            lines.append(
                f"| {fps} | {format_metric(item['delivered_fps'])} | "
                f"{format_metric(item['client_drop_rate'])} | {format_metric(item['queue_full_rate_pct'])} | "
                f"{format_metric(item['stale_rate_pct'])} | {format_metric(item['gateway_p95_e2e_ms'])} |"
            )
        lines.append("")

    passing = [run for run in results["runs"] if run["guardrails"].get("hard_pass")]
    passing_sorted = sorted(passing, key=lambda run: tuple(run["ranking"]["sort_key"]))

    lines.extend(
        [
            "## Top Passing Configurations",
            "",
            "| Run | Stage | Workers | FPS | Delay ms | Batch | Threshold | p95 E2E ms | Delivered FPS | Queue+Stale % | Decode p95 ms | Preprocess p95 ms | Triton roundtrip p95 ms | Postprocess p95 ms | Microbatch avg size | Microbatch assembly p95 ms |",
            "| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for run in passing_sorted[:10]:
        cfg = run["config"]
        prom = run["prometheus"]
        client = run["client"]["aggregate"]
        lines.append(
            f"| {run['run_id']} | {cfg['stage']} | {cfg['workers']} | {cfg['fps']} | "
            f"{format_metric(cfg['queue_delay_ms'])} | {cfg['preferred_batch_size']} | {cfg['similarity_threshold']} | "
            f"{format_metric(prom['gateway_p95_e2e_ms'])} | {format_metric(client['delivered_fps'])} | "
            f"{format_metric(prom['queue_full_rate_pct'] + prom['stale_rate_pct'])} | "
            f"{format_metric(prom.get('decode_p95_ms', 0.0))} | {format_metric(prom.get('preprocess_p95_ms', 0.0))} | "
            f"{format_metric(prom.get('triton_roundtrip_p95_ms', 0.0))} | {format_metric(prom.get('postprocess_p95_ms', 0.0))} | "
            f"{format_metric(prom.get('microbatch_avg_size', 0.0))} | {format_metric(prom.get('microbatch_assembly_p95_ms', 0.0))} |"
        )
    lines.append("")

    cache_runs = [run for run in results["runs"] if run["config"]["stage"] == "stage4"]
    if cache_runs:
        lines.extend(
            [
                "## Cache Comparison",
                "",
                "| Cached run | Cache accepted | Cached p95 E2E ms | Uncached p95 E2E ms | Cached delivered FPS | Uncached delivered FPS |",
                "| --- | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for run in cache_runs:
            compare = run.get("cache_comparison", {})
            lines.append(
                f"| {run['run_id']} | {compare.get('accepted', False)} | "
                f"{format_metric(run['prometheus']['gateway_p95_e2e_ms'])} | "
                f"{format_metric(compare.get('uncached_p95_e2e_ms', 0.0))} | "
                f"{format_metric(run['client']['aggregate']['delivered_fps'])} | "
                f"{format_metric(compare.get('uncached_delivered_fps', 0.0))} |"
            )
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def run_config_and_store(
    config: RunConfig,
    args: argparse.Namespace,
    results: dict[str, Any],
    baseline: dict[int, dict[str, float]],
) -> dict[str, Any]:
    existing = find_run(results, config.run_id())
    if existing is not None and args.resume:
        logger.info("Skipping existing run %s", config.run_id())
        return existing

    client, prometheus, timings, notes = execute_run(config, args)
    if config.stage == "baseline":
        baseline_guardrails = {
            "hard_pass": True,
            "checks": {
                "results_received": {
                    "metric": client["aggregate"]["results_received"],
                    "threshold": 0,
                    "pass": client["aggregate"]["results_received"] > 0,
                }
            },
        }
        guardrails = baseline_guardrails
        baseline_snapshot = build_baseline_map(results)
        baseline_snapshot[config.fps] = {
            "delivered_fps": client["aggregate"]["delivered_fps"],
            "client_drop_rate": client["aggregate"]["client_drop_rate"],
            "queue_full_rate_pct": prometheus["queue_full_rate_pct"],
            "stale_rate_pct": prometheus["stale_rate_pct"],
            "gateway_p95_e2e_ms": prometheus["gateway_p95_e2e_ms"],
        }
        ranking = build_ranking(config, client, prometheus, baseline_snapshot)
    else:
        guardrails = evaluate_guardrails(config, client, prometheus, baseline)
        ranking = build_ranking(config, client, prometheus, baseline)

    outcome = RunOutcome(
        run_id=config.run_id(),
        config=config.to_record(),
        timings=timings,
        client=client,
        prometheus=prometheus,
        guardrails=guardrails,
        ranking=ranking,
        notes=notes,
    )

    if config.stage == "stage4":
        uncached = find_uncached_peer(results, config)
        if uncached is not None:
            uncached_p95 = float(uncached["prometheus"]["gateway_p95_e2e_ms"])
            uncached_fps = float(uncached["client"]["aggregate"]["delivered_fps"])
            accepted = (
                outcome.guardrails["hard_pass"]
                and outcome.prometheus["gateway_p95_e2e_ms"] < uncached_p95
                and outcome.client["aggregate"]["delivered_fps"] >= uncached_fps * 0.95
            )
            record = outcome.to_record()
            record["cache_comparison"] = {
                "accepted": accepted,
                "uncached_run_id": uncached["run_id"],
                "uncached_p95_e2e_ms": uncached_p95,
                "uncached_delivered_fps": uncached_fps,
            }
            outcome = RunOutcome(
                run_id=record["run_id"],
                config=record["config"],
                timings=record["timings"],
                client=record["client"],
                prometheus=record["prometheus"],
                guardrails=record["guardrails"],
                ranking=record["ranking"],
                notes=record["notes"],
            )
            record_outcome = record
        else:
            record_outcome = outcome.to_record()
    else:
        record_outcome = outcome.to_record()

    for index, run in enumerate(results["runs"]):
        if run["run_id"] == outcome.run_id:
            results["runs"][index] = record_outcome
            break
    else:
        results["runs"].append(record_outcome)

    build_baseline_map(results)
    save_results(Path(args.results_path), results)
    generate_summary(results, Path(args.summary_path))
    return record_outcome


def collect_stage2_survivors(results: dict[str, Any], args: argparse.Namespace) -> set[tuple[int, int]]:
    survivors: set[tuple[int, int]] = set()
    for run in results["runs"]:
        if run["config"]["stage"] not in {"baseline", "stage2"}:
            continue
        if run["guardrails"].get("hard_pass"):
            workers = int(run["config"]["workers"])
            fps = int(run["config"]["fps"])
            if matches_seed_filters(workers=workers, fps=fps, args=args):
                survivors.add((workers, fps))
    return survivors


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    results_path = Path(args.results_path)
    summary_path = Path(args.summary_path)
    results = load_results(results_path, args.resume)

    try:
        baseline = build_baseline_map(results)

        for fps in OFFERED_FPS_VALUES:
            if not matches_selected(fps, args.fps):
                continue
            config = baseline_config_for_fps(fps)
            run_config_and_store(config, args, results, baseline)
            baseline = build_baseline_map(results)
        if run_stage_number(args.through_stage) <= run_stage_number("baseline"):
            return 0

        for config in build_stage2_configs(args):
            run_config_and_store(config, args, results, baseline)
        if run_stage_number(args.through_stage) <= run_stage_number("stage2"):
            return 0

        baseline = build_baseline_map(results)
        survivors = collect_stage2_survivors(results, args)
        for config in build_stage3_configs(survivors, args):
            run_config_and_store(config, args, results, baseline)
        if run_stage_number(args.through_stage) <= run_stage_number("stage3"):
            return 0

        baseline = build_baseline_map(results)
        for config in build_stage4_configs(results, baseline, args):
            run_config_and_store(config, args, results, baseline)

        return 0
    except Exception as exc:  # pragma: no cover - orchestration errors are surfaced to the operator
        logger.error("Benchmark run failed: %s", exc)
        return 1
    finally:
        save_results(results_path, results)
        generate_summary(results, summary_path)


if __name__ == "__main__":
    sys.exit(main())
