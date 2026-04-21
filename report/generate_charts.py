from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

REPORT_DIR = Path(__file__).resolve().parent
RESULTS_PATH = REPORT_DIR / "bench_results.json"
OUT = REPORT_DIR / "charts"
OUT.mkdir(exist_ok=True)

REDUCED_STAGE3_WORKERS = 2
REDUCED_STAGE3_FPS = (5, 30)
REDUCED_STAGE3_DELAYS = (0, 5, 10, 25, 50)
REDUCED_STAGE3_BATCHES = (1, 4, 8, 16)


def load_results() -> dict:
    if not RESULTS_PATH.exists():
        raise FileNotFoundError(f"Missing benchmark results: {RESULTS_PATH}")
    with RESULTS_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def chart_baseline_capacity(results: dict) -> None:
    baseline = results.get("baseline_by_fps", {})
    if not baseline:
        return

    fps_values = sorted(int(fps) for fps in baseline)
    delivered = [baseline[str(fps)]["delivered_fps"] for fps in fps_values]
    p95 = [baseline[str(fps)]["gateway_p95_e2e_ms"] for fps in fps_values]
    drop = [baseline[str(fps)]["client_drop_rate"] for fps in fps_values]

    fig, axes = plt.subplots(1, 3, figsize=(13, 4))
    plots = (
        ("Delivered FPS", delivered, "#2a7", "Delivered FPS"),
        ("Gateway p95 E2E", p95, "#d64", "Latency (ms)"),
        ("Client Drop Rate", drop, "#36c", "Drop rate (%)"),
    )

    for ax, (title, values, color, ylabel) in zip(axes, plots):
        ax.plot(fps_values, values, marker="o", linewidth=2, color=color)
        ax.set_title(title)
        ax.set_xlabel("Offered FPS")
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.25)

    fig.suptitle("Experiment 1: Baseline Capacity Sweep", fontsize=14, y=1.03)
    fig.tight_layout()
    fig.savefig(OUT / "exp1_baseline_capacity.png", dpi=160, bbox_inches="tight")
    plt.close(fig)


def chart_stage2_worker_sweep(results: dict) -> None:
    stage2 = [run for run in results.get("runs", []) if run.get("config", {}).get("stage") == "stage2"]
    if not stage2:
        return

    worker_values = sorted({int(run["config"]["workers"]) for run in stage2})
    fps_values = sorted({int(run["config"]["fps"]) for run in stage2})
    colors = plt.cm.tab10(range(len(fps_values)))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    for color, fps in zip(colors, fps_values):
        rows = sorted(
            (run for run in stage2 if int(run["config"]["fps"]) == fps),
            key=lambda run: int(run["config"]["workers"]),
        )
        workers = [int(run["config"]["workers"]) for run in rows]
        delivered = [run["client"]["aggregate"]["delivered_fps"] for run in rows]
        p95 = [run["prometheus"]["gateway_p95_e2e_ms"] for run in rows]
        failures = [not run["guardrails"].get("hard_pass") for run in rows]

        ax1.plot(workers, delivered, marker="o", linewidth=2, color=color, label=f"{fps} FPS")
        ax2.plot(workers, p95, marker="o", linewidth=2, color=color, label=f"{fps} FPS")

        failed_workers = [worker for worker, failed in zip(workers, failures) if failed]
        failed_delivered = [value for value, failed in zip(delivered, failures) if failed]
        failed_p95 = [value for value, failed in zip(p95, failures) if failed]
        if failed_workers:
            ax1.scatter(failed_workers, failed_delivered, marker="x", s=70, linewidths=2, color="crimson")
            ax2.scatter(failed_workers, failed_p95, marker="x", s=70, linewidths=2, color="crimson")

    ax1.set_title("Delivered FPS by Worker Count")
    ax1.set_xlabel("Workers")
    ax1.set_ylabel("Delivered FPS")
    ax1.set_xticks(worker_values)
    ax1.grid(True, alpha=0.25)

    ax2.set_title("Gateway p95 E2E by Worker Count")
    ax2.set_xlabel("Workers")
    ax2.set_ylabel("Latency (ms)")
    ax2.set_xticks(worker_values)
    ax2.grid(True, alpha=0.25)

    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=6, frameon=False, bbox_to_anchor=(0.5, 1.04))
    fig.suptitle("Experiment 2: Worker Sweep", fontsize=14, y=1.10)
    fig.tight_layout()
    fig.savefig(OUT / "exp2_worker_sweep.png", dpi=160, bbox_inches="tight")
    plt.close(fig)


def reduced_stage3_rows(results: dict) -> list[dict]:
    rows: list[dict] = []
    for run in results.get("runs", []):
        cfg = run.get("config", {})
        if cfg.get("stage") != "stage3":
            continue
        if int(cfg["workers"]) != REDUCED_STAGE3_WORKERS:
            continue
        fps = int(cfg["fps"])
        delay = int(round(float(cfg["queue_delay_ms"])))
        batch = int(cfg["max_batch_size"])
        if fps not in REDUCED_STAGE3_FPS:
            continue
        if delay not in REDUCED_STAGE3_DELAYS or batch not in REDUCED_STAGE3_BATCHES:
            continue
        rows.append(run)
    return rows


def matrix_for(rows: list[dict], fps: int, value_getter) -> list[list[float]]:
    index = {
        (
            int(run["config"]["fps"]),
            int(round(float(run["config"]["queue_delay_ms"]))),
            int(run["config"]["max_batch_size"]),
        ): run
        for run in rows
    }

    matrix: list[list[float]] = []
    for delay in REDUCED_STAGE3_DELAYS:
        row: list[float] = []
        for batch in REDUCED_STAGE3_BATCHES:
            run = index[(fps, delay, batch)]
            row.append(value_getter(run))
        matrix.append(row)
    return matrix


def pass_matrix(rows: list[dict], fps: int) -> list[list[bool]]:
    index = {
        (
            int(run["config"]["fps"]),
            int(round(float(run["config"]["queue_delay_ms"]))),
            int(run["config"]["max_batch_size"]),
        ): run
        for run in rows
    }

    matrix: list[list[bool]] = []
    for delay in REDUCED_STAGE3_DELAYS:
        row: list[bool] = []
        for batch in REDUCED_STAGE3_BATCHES:
            run = index[(fps, delay, batch)]
            row.append(bool(run["guardrails"].get("hard_pass")))
        matrix.append(row)
    return matrix


def annotate_heatmap(ax, values: list[list[float]], passes: list[list[bool]], fmt: str) -> None:
    for y, row in enumerate(values):
        for x, value in enumerate(row):
            ax.text(
                x,
                y,
                format(value, fmt),
                ha="center",
                va="center",
                color="white" if value > (max(max(r) for r in values) + min(min(r) for r in values)) / 2 else "black",
                fontsize=9,
            )
            if not passes[y][x]:
                ax.add_patch(
                    Rectangle(
                        (x - 0.5, y - 0.5),
                        1,
                        1,
                        fill=False,
                        edgecolor="crimson",
                        linewidth=2,
                    )
                )


def chart_stage3_reduced(results: dict) -> None:
    rows = reduced_stage3_rows(results)
    if not rows:
        return

    fig, axes = plt.subplots(2, 2, figsize=(11, 8))

    for col, fps in enumerate(REDUCED_STAGE3_FPS):
        delivered = matrix_for(rows, fps, lambda run: run["client"]["aggregate"]["delivered_fps"])
        p95 = matrix_for(rows, fps, lambda run: run["prometheus"]["gateway_p95_e2e_ms"])
        passes = pass_matrix(rows, fps)

        im_del = axes[0][col].imshow(delivered, aspect="auto", cmap="viridis")
        annotate_heatmap(axes[0][col], delivered, passes, ".2f")
        axes[0][col].set_title(f"Delivered FPS at {fps} Offered FPS")

        im_p95 = axes[1][col].imshow(p95, aspect="auto", cmap="magma_r")
        annotate_heatmap(axes[1][col], p95, passes, ".0f")
        axes[1][col].set_title(f"Gateway p95 E2E at {fps} Offered FPS")

        for row_axes in (axes[0][col], axes[1][col]):
            row_axes.set_xticks(range(len(REDUCED_STAGE3_BATCHES)))
            row_axes.set_xticklabels(REDUCED_STAGE3_BATCHES)
            row_axes.set_yticks(range(len(REDUCED_STAGE3_DELAYS)))
            row_axes.set_yticklabels(REDUCED_STAGE3_DELAYS)
            row_axes.set_xlabel("Batch size")
            row_axes.set_ylabel("Queue delay (ms)")

        fig.colorbar(im_del, ax=axes[0][col], fraction=0.046, pad=0.04)
        fig.colorbar(im_p95, ax=axes[1][col], fraction=0.046, pad=0.04)

    fig.suptitle(
        "Experiment 3: Reduced Queue-Delay and Batch-Size Sweep (Workers = 2)\n"
        "Red cell borders mark guardrail failures",
        fontsize=14,
        y=1.02,
    )
    fig.tight_layout()
    fig.savefig(OUT / "exp3_reduced_batching.png", dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    results = load_results()
    chart_baseline_capacity(results)
    chart_stage2_worker_sweep(results)
    chart_stage3_reduced(results)
    print(f"Wrote charts to {OUT}")


if __name__ == "__main__":
    main()
