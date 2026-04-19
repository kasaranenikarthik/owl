"""Generate PNG charts for the experiments report from bench_results.json
and the warm-system data points captured manually.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

OUT = Path(__file__).resolve().parent / "charts"
OUT.mkdir(exist_ok=True)

# Bench results captured from the 6-config sweep run by bench_run.py.
# Embedded inline so this chart script is self-contained even after the JSON
# is cleaned up.
BENCH = [
    {"cfg": {"name": "1_baseline_w24_1inst_d25",  "workers": 24, "instances": 1, "delay_us": 25000},
     "client_stats": {"avg_fps": 2.8, "avg_e2e_ms": 9728.3, "frames_sent": 1483, "frames_dropped": 3135, "results_received": 434},
     "triton_metrics": {"compute_ms_per_req": 2837.4, "queue_ms_per_req": 2579.8, "input_ms_per_req": 28.3, "output_ms_per_req": 87.5, "batch_size": 16.4, "gpu_util_avg_pct": 45.9}},
    {"cfg": {"name": "2_w24_1inst_d50",           "workers": 24, "instances": 1, "delay_us": 50000},
     "client_stats": {"avg_fps": 2.3, "avg_e2e_ms": 10657.1, "frames_sent": 1157, "frames_dropped": 3245, "results_received": 347},
     "triton_metrics": {"compute_ms_per_req": 3382.7, "queue_ms_per_req": 2966.9, "input_ms_per_req": 21.8, "output_ms_per_req": 36.0, "batch_size": 14.6, "gpu_util_avg_pct": 42.4}},
    {"cfg": {"name": "3_w32_1inst_d50",           "workers": 32, "instances": 1, "delay_us": 50000},
     "client_stats": {"avg_fps": 2.7, "avg_e2e_ms": 11797.4, "frames_sent": 1361, "frames_dropped": 3143, "results_received": 429},
     "triton_metrics": {"compute_ms_per_req": 3693.5, "queue_ms_per_req": 2843.0, "input_ms_per_req": 36.9, "output_ms_per_req": 46.1, "batch_size": 16.8, "gpu_util_avg_pct": 42.5}},
    {"cfg": {"name": "4_w40_1inst_d50",           "workers": 40, "instances": 1, "delay_us": 50000},
     "client_stats": {"avg_fps": 2.2, "avg_e2e_ms": 16805.4, "frames_sent": 1005, "frames_dropped": 2719, "results_received": 325},
     "triton_metrics": {"compute_ms_per_req": 6511.9, "queue_ms_per_req": 5330.0, "input_ms_per_req": 38.6, "output_ms_per_req": 77.9, "batch_size": 25.4, "gpu_util_avg_pct": 50.9}},
    {"cfg": {"name": "5_w24_2inst_d50",           "workers": 24, "instances": 2, "delay_us": 50000},
     "client_stats": {"avg_fps": 1.0, "avg_e2e_ms": 11943.5, "frames_sent": 1297, "frames_dropped": 3305, "results_received": 141},
     "triton_metrics": {"compute_ms_per_req": 7987.5, "queue_ms_per_req": 2831.3, "input_ms_per_req": 26.5, "output_ms_per_req": 26.1, "batch_size": 6.0, "gpu_util_avg_pct": 72.6}},
    {"cfg": {"name": "6_w32_1inst_d100",          "workers": 32, "instances": 1, "delay_us": 100000},
     "client_stats": {"avg_fps": 1.8, "avg_e2e_ms": 16378.7, "frames_sent": 864, "frames_dropped": 3296, "results_received": 273},
     "triton_metrics": {"compute_ms_per_req": 7201.7, "queue_ms_per_req": 4714.1, "input_ms_per_req": 28.7, "output_ms_per_req": 38.2, "batch_size": 17.1, "gpu_util_avg_pct": 60.4}},
]


def by_name(name):
    for r in BENCH:
        if r["cfg"]["name"].startswith(name):
            return r
    raise KeyError(name)


# ---------- Manual (warm) data captured during interactive testing ----------
# Each entry: (workers, instances, delay_us, fps, e2e_s, drop_pct, compute_ms,
#              batch, gpu_avg_pct, pending)
MANUAL = [
    # original 64-worker run with [8,16,32,64] preferred
    {"workers": 64, "inst": 1, "delay": 10000,  "fps": 6.7, "e2e": 4.8, "drop": 36, "compute": 14200, "batch": 50.3, "gpu_avg": 29.9, "pending": 2},
    # 32 workers same preferred
    {"workers": 32, "inst": 1, "delay": 10000,  "fps": 7.1, "e2e": 6.4, "drop": 44, "compute": 360,   "batch": 28.1, "gpu_avg": 22.0, "pending": 7},
    # 24w 2 instances
    {"workers": 24, "inst": 2, "delay": 10000,  "fps": 1.6, "e2e": 7.8, "drop": 72, "compute": 4600,  "batch": 7.7,  "gpu_avg": 69.0, "pending": 1},
    # 24w 50us (essentially zero) delay
    {"workers": 24, "inst": 1, "delay": 50,     "fps": 6.3, "e2e": 5.6, "drop": 48, "compute": 1000,  "batch": 21.4, "gpu_avg":  2.0, "pending": 1},
    # 24w 50ms delay (best run)
    {"workers": 24, "inst": 1, "delay": 50000,  "fps": 8.4, "e2e": 4.8, "drop": 34, "compute": 477,   "batch": 19.9, "gpu_avg":  1.0, "pending": 22},
    # 24w 25ms delay
    {"workers": 24, "inst": 1, "delay": 25000,  "fps": 7.4, "e2e": 5.1, "drop": 41, "compute": 252,   "batch": 20.7, "gpu_avg": 41.0, "pending": 1},
]


def chart_workers():
    """Experiment 1: FPS and per-batch compute vs NUM_WORKERS."""
    bench_workers = [24, 32, 40]
    bench_fps = [by_name(f"{i}_")["client_stats"]["avg_fps"] for i in [2, 3, 4]]
    bench_compute = [by_name(f"{i}_")["triton_metrics"]["compute_ms_per_req"] for i in [2, 3, 4]]

    # Warm data at compatible delay (10ms-25ms range, 1 instance)
    warm = [(m["workers"], m["fps"], m["compute"]) for m in MANUAL if m["inst"] == 1 and 10000 <= m["delay"] <= 25000]
    warm.sort()
    warm_workers = [w[0] for w in warm]
    warm_fps = [w[1] for w in warm]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4))

    ax1.plot(warm_workers, warm_fps, "o-", color="#2a7", linewidth=2, markersize=10, label="Warm system (manual)")
    ax1.plot(bench_workers, bench_fps, "s-", color="#e63", linewidth=2, markersize=10, label="Cold start (bench)")
    ax1.set_xlabel("NUM_WORKERS")
    ax1.set_ylabel("Achieved FPS (client-side)")
    ax1.set_title("Throughput vs worker pool size")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    ax2.bar([str(w) for w in bench_workers], bench_compute, color=["#2a7", "#2a7", "#e63"])
    ax2.set_xlabel("NUM_WORKERS (cold-start bench, 50ms delay)")
    ax2.set_ylabel("Triton per-request compute (ms)")
    ax2.set_title("Contention pathology emerges past 32 workers")
    ax2.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    plt.savefig(OUT / "exp1_workers.png", dpi=140, bbox_inches="tight")
    plt.close()


def chart_delay():
    """Experiment 2: FPS vs max_queue_delay (warm vs cold)."""
    warm = [(m["delay"], m["fps"]) for m in MANUAL if m["inst"] == 1 and m["workers"] == 24]
    warm.sort()
    warm_delay_ms = [d / 1000 for d, _ in warm]
    warm_fps = [f for _, f in warm]

    cold_delay_ms = [25, 50]
    cold_fps = [by_name("1_")["client_stats"]["avg_fps"], by_name("2_")["client_stats"]["avg_fps"]]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(warm_delay_ms, warm_fps, "o-", color="#2a7", linewidth=2, markersize=10, label="Warm system, 24w/1inst (manual)")
    ax.plot(cold_delay_ms, cold_fps, "s-", color="#e63", linewidth=2, markersize=10, label="Cold start, 24w/1inst (bench)")
    ax.set_xscale("log")
    ax.set_xlabel("max_queue_delay (ms, log scale)")
    ax.set_ylabel("Achieved FPS")
    ax.set_title("Throughput vs Triton dynamic batching delay")
    ax.legend()
    ax.grid(True, alpha=0.3, which="both")
    ax.set_xticks([0.05, 0.1, 1, 10, 25, 50, 100])
    ax.set_xticklabels(["0.05", "0.1", "1", "10", "25", "50", "100"])

    plt.tight_layout()
    plt.savefig(OUT / "exp2_delay.png", dpi=140, bbox_inches="tight")
    plt.close()


def chart_instances():
    """Experiment 3: 4-panel comparison of 1 vs 2 instances."""
    one = by_name("2_")  # 24w/1inst/50ms
    two = by_name("5_")  # 24w/2inst/50ms

    metrics = [
        ("FPS",            "client_stats", "avg_fps",            "higher better"),
        ("Compute ms",     "triton_metrics", "compute_ms_per_req", "lower better"),
        ("Avg batch size", "triton_metrics", "batch_size",        "higher better"),
        ("GPU avg %",      "triton_metrics", "gpu_util_avg_pct",  "saturation, not throughput"),
    ]

    fig, axes = plt.subplots(1, 4, figsize=(13, 3.5))
    for ax, (label, group, key, note) in zip(axes, metrics):
        v1 = one[group][key]
        v2 = two[group][key]
        bars = ax.bar(["1 inst", "2 inst"], [v1, v2], color=["#2a7", "#e63"])
        for b, v in zip(bars, [v1, v2]):
            ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.1f}", ha="center", va="bottom", fontsize=10)
        ax.set_title(f"{label}\n({note})", fontsize=10)
        ax.grid(True, alpha=0.3, axis="y")

    fig.suptitle("Two model instances on a single GPU collapse throughput", fontsize=12)
    plt.tight_layout()
    plt.savefig(OUT / "exp3_instances.png", dpi=140, bbox_inches="tight")
    plt.close()


def chart_breakdown():
    """Latency breakdown across all 6 bench configs."""
    names, queue, compute, ovh = [], [], [], []
    for r in BENCH:
        cfg = r["cfg"]
        names.append(f"w{cfg['workers']}/i{cfg['instances']}/{cfg['delay_us']//1000}ms")
        m = r["triton_metrics"]
        queue.append(m["queue_ms_per_req"])
        compute.append(m["compute_ms_per_req"])
        ovh.append(m["input_ms_per_req"] + m["output_ms_per_req"])

    fig, ax = plt.subplots(figsize=(10, 4.5))
    x = range(len(names))
    ax.bar(x, queue,   label="Queue (waiting in Triton)",     color="#888")
    ax.bar(x, compute, bottom=queue, label="Compute (GPU)",   color="#2a7")
    ax.bar(x, ovh,     bottom=[q + c for q, c in zip(queue, compute)], label="Input + Output (H2D/D2H)", color="#e63")
    ax.set_xticks(x)
    ax.set_xticklabels(names, rotation=20, ha="right")
    ax.set_ylabel("Time per request (ms)")
    ax.set_title("Triton-side latency breakdown across configurations (cold-start bench)")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    plt.savefig(OUT / "latency_breakdown.png", dpi=140, bbox_inches="tight")
    plt.close()


def main():
    chart_workers()
    chart_delay()
    chart_instances()
    chart_breakdown()
    print(f"Wrote charts to {OUT}")


if __name__ == "__main__":
    main()
