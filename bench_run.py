"""Benchmark runner: sweep NUM_WORKERS / instance_count / max_queue_delay configs.

Per run: applies config, restarts model-loader + triton + inference, waits for
readiness, runs the load generator for one full pass of the 120s test video,
captures stdout stats, queries Triton metrics from Prometheus over the run
window, and writes everything to bench_results.json.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent
CONFIG = REPO / "server" / "internal" / "models" / "yolov8s" / "config.gpu.pbtxt"
COMPOSE = REPO / "docker-compose.yml"
VIDEO = "data/Traffic_test_hd_1920_1080_30fps.mp4"
PROM = "http://localhost:9090"
RESULTS = REPO / "bench_results.json"

MATRIX = [
    {"name": "1_baseline_w24_1inst_d25", "workers": 24, "instances": 1, "delay_us": 25000},
    {"name": "2_w24_1inst_d50",          "workers": 24, "instances": 1, "delay_us": 50000},
    {"name": "3_w32_1inst_d50",          "workers": 32, "instances": 1, "delay_us": 50000},
    {"name": "4_w40_1inst_d50",          "workers": 40, "instances": 1, "delay_us": 50000},
    {"name": "5_w24_2inst_d50",          "workers": 24, "instances": 2, "delay_us": 50000},
    {"name": "6_w32_1inst_d100",         "workers": 32, "instances": 1, "delay_us": 100000},
]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def edit_config(workers: int, instances: int, delay_us: int) -> None:
    text = CONFIG.read_text()
    text = re.sub(r"max_queue_delay_microseconds:\s*\d+", f"max_queue_delay_microseconds: {delay_us}", text)
    text = re.sub(r"count:\s*\d+(\s*\n\s*kind:\s*KIND_GPU)", f"count: {instances}\\1", text)
    CONFIG.write_text(text)

    text = COMPOSE.read_text()
    text = re.sub(r'(NUM_WORKERS:\s*)"\d+"', f'\\1"{workers}"', text)
    COMPOSE.write_text(text)


def run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=REPO, check=True, **kw)


def reset_kafka() -> None:
    log("resetting kafka topics (clearing backlog)")
    cmd = (
        "kafka-topics --bootstrap-server kafka:9092 --delete --topic frames; "
        "kafka-topics --bootstrap-server kafka:9092 --delete --topic detections; "
        "for i in {1..15}; do "
        "  kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic frames --partitions 3 --replication-factor 1 && "
        "  kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic detections --partitions 3 --replication-factor 1 && break; "
        "  sleep 1; "
        "done"
    )
    subprocess.run(["docker", "exec", "owl-kafka", "bash", "-c", cmd], check=True, capture_output=True)


def restart_stack() -> None:
    log("re-running model-loader to refresh config")
    run(["docker", "compose", "up", "-d", "--force-recreate", "model-loader"])
    subprocess.run(["docker", "wait", "owl-model-loader"], check=True, capture_output=True)
    log("restarting triton")
    run(["docker", "compose", "restart", "triton"])
    reset_kafka()
    log("recreating gateway + inference")
    run(["docker", "compose", "up", "-d", "--force-recreate", "gateway", "inference"])

    log("waiting for yolov8s model ready")
    for i in range(180):
        try:
            with urllib.request.urlopen("http://localhost:8000/v2/models/yolov8s/ready", timeout=2) as r:
                if r.status == 200:
                    log(f"model ready after {i}s")
                    break
        except Exception:
            pass
        time.sleep(1)
    else:
        raise RuntimeError("yolov8s model failed to become ready in 180s")

    log("warmup 25s")
    time.sleep(25)


def parse_stats(stdout: str) -> dict:
    stats = {}
    patterns = {
        "frames_sent":     r"Frames sent:\s+(\d+)",
        "frames_dropped":  r"Frames dropped:\s+(\d+)",
        "results_received": r"Results received:\s+(\d+)",
        "avg_fps":         r"Avg FPS:\s+([\d.]+)",
        "avg_inference_ms": r"Avg inference:\s+([\d.]+)ms",
        "avg_e2e_ms":      r"Avg E2E:\s+([\d.]+)ms",
        "cache_hit_rate":  r"Cache hit rate:\s+([\d.]+)%",
    }
    for key, pat in patterns.items():
        m = re.search(pat, stdout)
        if m:
            v = m.group(1)
            stats[key] = float(v) if "." in v else int(v)
    return stats


def run_load() -> tuple[float, float, str]:
    start = time.time()
    log("starting load generator")
    proc = subprocess.run(
        [sys.executable, "cli.py", "video", "../" + VIDEO, "--fps", "30", "--headless"],
        cwd=REPO / "yolo_client",
        capture_output=True,
        text=True,
        timeout=300,
    )
    end = time.time()
    log(f"load gen finished in {end - start:.1f}s exit={proc.returncode}")
    return start, end, proc.stdout + "\n" + proc.stderr


def prom_query(query: str, when: float) -> float | None:
    url = f"{PROM}/api/v1/query?query={urllib.parse.quote(query)}&time={int(when)}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
        if data["data"]["result"]:
            return float(data["data"]["result"][0]["value"][1])
    except Exception as e:
        log(f"prom query failed: {e}")
    return None


def collect_metrics(start: float, end: float) -> dict:
    # query at end-of-window; rate uses the test duration so we average over the run
    win = max(int(end - start), 30)
    queries = {
        "compute_ms_per_req":  f"rate(nv_inference_compute_infer_duration_us[{win}s]) / rate(nv_inference_request_success[{win}s]) / 1000",
        "queue_ms_per_req":    f"rate(nv_inference_queue_duration_us[{win}s]) / rate(nv_inference_request_success[{win}s]) / 1000",
        "request_ms_per_req":  f"rate(nv_inference_request_duration_us[{win}s]) / rate(nv_inference_request_success[{win}s]) / 1000",
        "input_ms_per_req":    f"rate(nv_inference_compute_input_duration_us[{win}s]) / rate(nv_inference_request_success[{win}s]) / 1000",
        "output_ms_per_req":   f"rate(nv_inference_compute_output_duration_us[{win}s]) / rate(nv_inference_request_success[{win}s]) / 1000",
        "batch_size":          f"rate(nv_inference_request_success[{win}s]) / rate(nv_inference_exec_count[{win}s])",
        "triton_req_per_s":    f"rate(nv_inference_request_success[{win}s])",
        "gpu_util_avg_pct":    f"avg_over_time(nv_gpu_utilization[{win}s]) * 100",
        "gpu_util_max_pct":    f"max_over_time(nv_gpu_utilization[{win}s]) * 100",
        "gpu_mem_used_gib":    f"avg_over_time(nv_gpu_memory_used_bytes[{win}s]) / 1073741824",
        "pending_avg":         f"avg_over_time(nv_inference_pending_request_count[{win}s])",
    }
    return {k: prom_query(q, end) for k, q in queries.items()}


def main() -> None:
    all_results = []
    for cfg in MATRIX:
        log(f"==== {cfg['name']} ====")
        log(f"workers={cfg['workers']} instances={cfg['instances']} delay_us={cfg['delay_us']}")
        edit_config(cfg["workers"], cfg["instances"], cfg["delay_us"])
        restart_stack()
        start, end, stdout = run_load()
        stats = parse_stats(stdout)
        log(f"client stats: {stats}")
        time.sleep(5)
        metrics = collect_metrics(start, end)
        log(f"triton metrics: {metrics}")
        all_results.append({
            "cfg": cfg,
            "client_stats": stats,
            "triton_metrics": metrics,
            "duration_s": end - start,
            "stdout_tail": stdout[-2000:],
        })
        # write incrementally so we don't lose data on a crash
        RESULTS.write_text(json.dumps(all_results, indent=2))

    log("done")


if __name__ == "__main__":
    main()
