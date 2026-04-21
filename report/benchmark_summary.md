# OWL Benchmark Summary

## Baseline by FPS

| Offered FPS | Delivered FPS | Client drop % | Queue full % | Stale % | Gateway p95 E2E ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 5 | 4.39 | 10.68 | 0.00 | 0.00 | 626.64 |
| 7 | 4.98 | 22.85 | 0.00 | 0.00 | 1253.35 |
| 10 | 4.64 | 49.68 | 0.00 | 0.00 | 1258.36 |
| 15 | 4.65 | 64.23 | 0.00 | 0.00 | 1256.87 |
| 20 | 4.98 | 69.54 | 0.00 | 0.00 | 1253.13 |
| 30 | 4.52 | 79.31 | 0.00 | 0.00 | 1257.52 |

## Top Passing Configurations

| Run | Stage | Workers | FPS | Delay ms | Batch | Threshold | p95 E2E ms | Delivered FPS | Queue+Stale % |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| stage2_w2_fps5_d2000_mb8_pb8_thr-1_i1_c1 | stage2 | 2 | 5 | 2.00 | [8] | -1 | 499.73 | 4.72 | 0.00 |
| stage2_w3_fps5_d2000_mb8_pb8_thr-1_i1_c1 | stage2 | 3 | 5 | 2.00 | [8] | -1 | 562.33 | 4.55 | 0.00 |
| stage3_w2_fps5_d0_mb4_pb4_thr-1_i1_c1 | stage3 | 2 | 5 | 0.00 | [4] | -1 | 568.41 | 4.20 | 0.00 |
| stage2_w1_fps20_d2000_mb8_pb8_thr-1_i1_c1 | stage2 | 1 | 20 | 2.00 | [8] | -1 | 586.79 | 3.83 | 0.00 |
| stage2_w1_fps30_d2000_mb8_pb8_thr-1_i1_c1 | stage2 | 1 | 30 | 2.00 | [8] | -1 | 589.12 | 3.76 | 0.00 |
| stage3_w2_fps5_d0_mb8_pb8_thr-1_i1_c1 | stage3 | 2 | 5 | 0.00 | [8] | -1 | 596.13 | 4.20 | 0.00 |
| stage3_w2_fps5_d0_mb1_pb1_thr-1_i1_c1 | stage3 | 2 | 5 | 0.00 | [1] | -1 | 602.64 | 4.20 | 0.00 |
| stage2_w5_fps5_d2000_mb8_pb8_thr-1_i1_c1 | stage2 | 5 | 5 | 2.00 | [8] | -1 | 603.49 | 4.20 | 0.00 |
| stage3_w2_fps5_d25000_mb1_pb1_thr-1_i1_c1 | stage3 | 2 | 5 | 25.00 | [1] | -1 | 604.72 | 4.20 | 0.00 |
| stage3_w2_fps5_d0_mb16_pb16_thr-1_i1_c1 | stage3 | 2 | 5 | 0.00 | [16] | -1 | 605.60 | 4.20 | 0.00 |
