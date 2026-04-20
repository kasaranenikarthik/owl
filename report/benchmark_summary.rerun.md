# OWL Benchmark Summary

## Baseline by FPS

| Offered FPS | Delivered FPS | Client drop % | Queue full % | Stale % | Gateway p95 E2E ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 30 | 4.95 | 77.43 | 0.00 | 0.00 | 625.49 |

## Top Passing Configurations

| Run | Stage | Workers | FPS | Delay ms | Batch | Threshold | p95 E2E ms | Delivered FPS | Queue+Stale % |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| stage2_w2_fps30_d2000_mb8_pb8_thr-1_i1_c1 | stage2 | 2 | 30 | 2.00 | [8] | -1 | 625.01 | 4.91 | 0.00 |
| baseline_w4_fps30_d2000_mb8_pb8_thr-1_i1_c1 | baseline | 4 | 30 | 2.00 | [8] | -1 | 625.49 | 4.95 | 0.00 |
