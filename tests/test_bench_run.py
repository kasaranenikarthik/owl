from __future__ import annotations

import http.client
import unittest
from types import SimpleNamespace
from unittest import mock

from bench_run import (
    RunConfig,
    build_stage3_configs,
    collect_stage2_survivors,
    evaluate_guardrails,
    execute_run,
    render_triton_gpu_config,
    wait_for_http,
)


class BenchRunTests(unittest.TestCase):
    def test_render_triton_gpu_config_uses_run_parameters(self) -> None:
        config = RunConfig(
            stage="stage3",
            workers=6,
            fps=15,
            queue_delay_us=25000,
            max_batch_size=16,
            preferred_batch_size=(16,),
            similarity_threshold=-1,
            instance_count=2,
            client_count=1,
        )

        rendered = render_triton_gpu_config(config)

        self.assertIn("max_batch_size: 16", rendered)
        self.assertIn("preferred_batch_size: [16]", rendered)
        self.assertIn("max_queue_delay_microseconds: 25000", rendered)
        self.assertIn("count: 2", rendered)

    def test_guardrails_pass_for_moderate_regression(self) -> None:
        baseline = {
            15: {
                "delivered_fps": 10.0,
                "client_drop_rate": 20.0,
                "queue_full_rate_pct": 5.0,
                "stale_rate_pct": 2.0,
                "gateway_p95_e2e_ms": 1000.0,
            }
        }
        config = RunConfig(
            stage="stage2",
            workers=5,
            fps=15,
            queue_delay_us=2000,
            max_batch_size=8,
            preferred_batch_size=(8,),
            similarity_threshold=-1,
        )
        client = {
            "aggregate": {
                "results_received": 100,
                "delivered_fps": 7.0,
                "client_drop_rate": 40.0,
            }
        }
        prometheus = {
            "queue_full_rate_pct": 20.0,
            "stale_rate_pct": 15.0,
            "gateway_p95_e2e_ms": 2500.0,
        }

        guardrails = evaluate_guardrails(config, client, prometheus, baseline)

        self.assertTrue(guardrails["hard_pass"])
        self.assertTrue(all(check["pass"] for check in guardrails["checks"].values()))

    def test_guardrails_fail_for_catastrophic_regression(self) -> None:
        baseline = {
            30: {
                "delivered_fps": 8.0,
                "client_drop_rate": 35.0,
                "queue_full_rate_pct": 10.0,
                "stale_rate_pct": 5.0,
                "gateway_p95_e2e_ms": 1500.0,
            }
        }
        config = RunConfig(
            stage="stage2",
            workers=1,
            fps=30,
            queue_delay_us=2000,
            max_batch_size=8,
            preferred_batch_size=(8,),
            similarity_threshold=-1,
        )
        client = {
            "aggregate": {
                "results_received": 10,
                "delivered_fps": 2.0,
                "client_drop_rate": 70.0,
            }
        }
        prometheus = {
            "queue_full_rate_pct": 40.0,
            "stale_rate_pct": 30.0,
            "gateway_p95_e2e_ms": 5000.0,
        }

        guardrails = evaluate_guardrails(config, client, prometheus, baseline)

        self.assertFalse(guardrails["hard_pass"])
        self.assertFalse(guardrails["checks"]["delivered_fps"]["pass"])
        self.assertFalse(guardrails["checks"]["gateway_p95_e2e_ms"]["pass"])

    def test_wait_for_http_retries_remote_disconnect(self) -> None:
        response = mock.MagicMock()
        response.__enter__.return_value.status = 200

        with (
            mock.patch(
                "bench_run.urllib.request.urlopen",
                side_effect=[http.client.RemoteDisconnected("closed"), response],
            ),
            mock.patch("bench_run.time.sleep"),
        ):
            wait_for_http("http://localhost:8000/health", timeout_seconds=1.0)

    def test_collect_stage2_survivors_respects_worker_and_fps_filters(self) -> None:
        args = SimpleNamespace(workers=(2,), fps=(5, 30), queue_delay_ms=None, batch_sizes=None)
        results = {
            "runs": [
                {
                    "config": {"stage": "baseline", "workers": 4, "fps": 5},
                    "guardrails": {"hard_pass": True},
                },
                {
                    "config": {"stage": "stage2", "workers": 2, "fps": 5},
                    "guardrails": {"hard_pass": True},
                },
                {
                    "config": {"stage": "stage2", "workers": 2, "fps": 30},
                    "guardrails": {"hard_pass": True},
                },
                {
                    "config": {"stage": "stage2", "workers": 3, "fps": 5},
                    "guardrails": {"hard_pass": True},
                },
            ]
        }

        survivors = collect_stage2_survivors(results, args)

        self.assertEqual(survivors, {(2, 5), (2, 30)})

    def test_build_stage3_configs_respects_delay_and_batch_filters(self) -> None:
        args = SimpleNamespace(workers=(2,), fps=(5, 30), queue_delay_ms=(0, 50), batch_sizes=(1, 16))

        configs = build_stage3_configs({(1, 5), (2, 5), (2, 30)}, args)

        observed = {
            (config.workers, config.fps, config.queue_delay_us, config.max_batch_size)
            for config in configs
        }
        expected = {
            (2, 5, 0, 1),
            (2, 5, 0, 16),
            (2, 5, 50000, 1),
            (2, 5, 50000, 16),
            (2, 30, 0, 1),
            (2, 30, 0, 16),
            (2, 30, 50000, 1),
            (2, 30, 50000, 16),
        }

        self.assertEqual(observed, expected)

    def test_execute_run_stops_services_before_reset_after_warmup(self) -> None:
        args = SimpleNamespace(
            server_url="ws://localhost:8080/ws",
            video_path="video.mp4",
            warmup_seconds=5.0,
            measure_seconds=30.0,
            settle_seconds=2.0,
            jpeg_quality=80,
            send_queue_size=10,
            prometheus_url="http://localhost:9090",
        )
        config = RunConfig(
            stage="stage3",
            workers=2,
            fps=30,
            queue_delay_us=50000,
            max_batch_size=8,
            preferred_batch_size=(8,),
            similarity_threshold=-1,
        )
        call_order: list[str] = []

        def record(name: str):
            def _inner(*_args, **_kwargs):
                call_order.append(name)
                if name == "write_temp_triton_config":
                    return "temp.pbtxt"
                if name == "build_compose_env":
                    return {"NUM_WORKERS": "2"}
                if name == "asyncio_run_benchmark":
                    duration = _kwargs["duration_seconds"]
                    return {
                        "aggregate": {
                            "results_received": 1,
                            "delivered_fps": 1.0 if duration == args.warmup_seconds else 2.0,
                            "client_drop_rate": 0.0,
                        },
                        "per_client": [],
                    }
                if name == "collect_prometheus_metrics":
                    return {
                        "gateway_p95_e2e_ms": 100.0,
                        "worker_queue_p95_ms": 10.0,
                        "frames_consumed": 1.0,
                        "queue_full_count": 0.0,
                        "stale_count": 0.0,
                        "superseded_count": 0.0,
                        "cache_hit_rate_pct": 0.0,
                        "triton_request_ms": 10.0,
                        "triton_queue_ms": 1.0,
                        "triton_compute_ms": 5.0,
                        "triton_avg_batch_size": 1.0,
                        "gpu_util_avg_pct": 0.0,
                        "queue_full_rate_pct": 0.0,
                        "stale_rate_pct": 0.0,
                        "superseded_rate_pct": 0.0,
                    }
                return None

            return _inner

        with (
            mock.patch("bench_run.write_temp_triton_config", side_effect=record("write_temp_triton_config")),
            mock.patch("bench_run.build_compose_env", side_effect=record("build_compose_env")),
            mock.patch("bench_run.ensure_static_stack", side_effect=record("ensure_static_stack")),
            mock.patch("bench_run.stop_run_services", side_effect=record("stop_run_services")),
            mock.patch("bench_run.reset_runtime_state", side_effect=record("reset_runtime_state")),
            mock.patch("bench_run.start_run_services", side_effect=record("start_run_services")),
            mock.patch("bench_run.asyncio_run_benchmark", side_effect=record("asyncio_run_benchmark")),
            mock.patch("bench_run.collect_prometheus_metrics", side_effect=record("collect_prometheus_metrics")),
            mock.patch("bench_run.time.time", side_effect=[1000.0, 1030.0]),
        ):
            execute_run(config, args)

        self.assertEqual(
            call_order,
            [
                "write_temp_triton_config",
                "build_compose_env",
                "ensure_static_stack",
                "stop_run_services",
                "reset_runtime_state",
                "start_run_services",
                "asyncio_run_benchmark",
                "stop_run_services",
                "reset_runtime_state",
                "start_run_services",
                "asyncio_run_benchmark",
                "collect_prometheus_metrics",
            ],
        )


if __name__ == "__main__":
    unittest.main()
