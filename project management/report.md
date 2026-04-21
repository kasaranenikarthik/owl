## Project management

### Team and ownership

Three contributors with explicit ownership domains established at project start:

| Contributor              | Domain                                                                                                  |
|--------------------------|---------------------------------------------------------------------------------------------------------|
| **Karthik Kasaraneni**   | Go gateway + inference bridge, Kafka/Redis/Triton wiring, GPU configuration, observability dashboards   |
| **Jatin Sainani**        | Python client (`yolo_client`), Python distributed prototype, GPU inference integration, benchmarks       |
| **Santrupti Patil**      | Design documentation, experiments, deployment architecture, README, observability dashboards             |

---
Task board: [Tasks board.pdf](Tasks%20board.pdf)
---

### From initial design to final state

The project moved through four deliberate phases. Branch structure mirrored the phases — each contributor worked on their own branch and integrated through reviewed merges to `main`.

#### Problem decomposition used at project start

Before choosing a single architecture, we decomposed the system into four independently testable concerns so each prototype could answer a specific risk:

| Concern | Key question | Owner in Phase 1 | Exit criterion |
|---|---|---|---|
| **Serving path** | Can we maintain stable API behavior while changing the backend inference engine? | Karthik | Gateway + inference bridge can serve detections end-to-end with clear interface boundaries |
| **Pipeline scalability** | Can ingestion and compute be decoupled under burst load? | Jatin | Queue-based prototype demonstrates worker fan-out and non-blocking delivery |
| **Deployment viability** | Can the architecture be moved to cloud GPU infrastructure without redesign? | Santrupti | Terraform skeleton provisions network + compute primitives compatible with GPU workloads |
| **Observability correctness** | Can we measure bottlenecks instead of guessing? | Karthik + Jatin | Initial Prometheus/Grafana metrics track queueing, latency, and throughput per stage |

This decomposition made the convergence decision evidence-based: each prototype was judged on one primary risk first, then on full-system fit.

**Phase 1 — Three parallel architectures.** Rather than commit early to one design, each team member prototyped a different distributed shape:

- *Jatin* — Python ingest → workers → aggregator with FastAPI / WebSocket delivery and micro-batching
- *Karthik* — Go gateway + inference bridge wrapping NVIDIA Triton, Kafka decoupling, Redis similarity cache
- *Santrupti* — Terraform + ECS deployment skeleton with GPU-aware infrastructure

The cost of three quick prototypes was judged smaller than the cost of re-architecting late in the term. Result: by end of Phase 1, three working starting points existed (commits **fef1f07**, **9c8d7db**, **000500a**), each demonstrating a different tradeoff (rapid iteration vs. production-style serving vs. cloud-deployable).

##### Phase 1 comparison snapshot (used in convergence discussion)

| Prototype | Strength discovered | Limitation discovered | Kept for final state |
|---|---|---|---|
| Python ingest/workers/aggregator | Fastest to iterate, easy to run local experiments | More custom serving code to maintain; less clear separation from model runtime | **Client + benchmark tooling** |
| Go + Triton + Kafka + Redis | Strong separation of concerns, clearer scaling boundaries, close to production serving practices | Higher setup complexity and stricter config contracts | **Core runtime architecture** |
| Terraform + ECS skeleton | Strong deployability story and reproducible infra | Not sufficient alone without stable app/runtime interfaces | **Deployment target and documentation baseline** |

**Phase 2 — Convergence.** The team reviewed the three prototypes against fixed criteria — separation of model serving from application logic, scaling story, fit with the course's distributed-systems learning goals — and selected Karthik's Triton-based architecture as canonical. Jatin's `yolo_client` was preserved as the user-facing layer (and became the load generator used in the experiments above); Santrupti's Terraform modules were kept as the deployment target. Triton was integrated in commit **cbec7ef** (Apr 13); the integrated system was tagged **owl v1.0** in commit **489901e** (Apr 14). Decision rationale was documented in `PROJECT_REPORT.md` so it would survive personnel turnover.

Convergence implementation was done as controlled interface swaps rather than a full rewrite:

1. Keep the external request/response shape stable at the gateway boundary.
2. Replace direct model invocation with Triton-backed inference calls.
3. Preserve Jatin's client contracts so benchmark scripts required minimal rewrites.
4. Validate that Terraform assumptions (ports, services, runtime dependencies) still matched the integrated stack.

This reduced migration risk because each step had a rollback point and a narrow verification surface.

**Phase 3 — GPU enablement.** Santrupti added GPU-aware Docker Compose with CPU fallback (commit **5895826**), Triton model configurations for both execution targets, and a model-loader service that picks the correct config at startup. Jatin wired GPU inference end-to-end and shipped the first observability dashboard (commit **8c0dffb**, Apr 16).

**Phase 4 — Performance work and writing (Apr 17 – 19).** Comprehensive Grafana dashboards with annotated panels (commits **066e626**, **625c490**, **7adf9f1**), the automated benchmark harness used for the experiments above (commit **5cd3922**), and the report itself (commit **348b9ff**).

#### Final-state architecture mapping (what survived from each stream)

- **Runtime spine (Go/Triton/Kafka/Redis):** from Karthik's prototype, became the production-style data path.
- **Client and load generation (`yolo_client`):** from Jatin's prototype, became both user interface layer and benchmark driver.
- **Infra-as-code modules (`server/terraform`):** from Santrupti's stream, became the deployment and reproducibility backbone.
- **Dashboards and metrics:** jointly extended in late phases to make performance bottlenecks measurable during benchmarking.

In other words, the final state was not a winner-take-all replacement; it was a selective merge where each initial prototype contributed a durable subsystem.

### Problems encountered

| Problem | Impact | Resolution |
|---|---|---|
| **FP16 model rollback (Apr 18).** Karthik attempted FP16 to speed up inference (commits **b68e40a**, **2cb936e**); the change broke end-to-end because the input dtype declared in `config.gpu.pbtxt` (`TYPE_FP32`) no longer matched the export, and downstream postprocessing assumed FP32 outputs. | ~½ day lost | Reverted in commits **06af236**, **71bf8a4**, **4d7db89**, **ffd090c**, **95d4adc**. Diagnostic process surfaced implicit type assumptions across model loader → Triton config → bridge postprocessor; learning preserved in code comments. |
| **Stash recovery during rollback.** FP16 revert created an in-flight stash (`WIP on karthikfeature`) that needed careful unwinding to avoid losing other in-progress changes (commits **41db7e8**, **c2c0143**, **cb65a2d**). | Risk of data loss | Recovered without loss; pair-debugging session adopted as standard practice for non-trivial reverts. |
| **Configuration drift between docker-compose env and Triton.** Sweeping Triton config required restarting the *model-loader* service (which copies the config into the Triton volume) in addition to Triton itself — not obvious from `docker-compose.yml`. | Several initial benchmark runs invalidated by stale config | Documented and encoded in `bench_run.py:restart_stack()` so future operators don't re-discover. |
| **Cold-start vs warm benchmarking divergence.** Discovered during the experiments above: the automated bench produces ~3 × lower throughput than interactive runs because 150 s isn't long enough to JIT-warm CUDA kernels. | Bench numbers don't match production | Documented as a methodology limitation; future-work item is a pre-warmup pass before timed runs. |
| **Silent frame drops.** Experiments revealed `server/cmd/inference/main.go:192-198` drops frames silently when the job channel overflows; only the 2-second staleness path increments the visible counter. | Real overload under-reported in metrics | Identified as a metric-correctness bug; fix queued for the next sprint, not blocking the report. |

### How the work was divided and coordinated

- **Branch ownership.** `karthikfeature` (server runtime), Jatin's branches for client + benchmark, Santrupti's branches for deployment. Merges to `main` only after cross-review.
- **Commit-message conventions.** `Revert "X"` commits clearly mark rolled-back work; `WIP on <branch>` tags identify in-flight stash recoveries; feature commits kept small (median ≈ 7 files / 100 lines per commit).
- **Async coordination via `PROJECT_REPORT.md`.** Updated weekly during Phases 1-2 to keep all three architectural directions visible to the team and to record decision rationale.
- **Async observation via Grafana.** Once dashboards were live (Phase 4), all three team members could independently watch the same metrics during experiments without coordinating runs in real time.

#### Integration handoffs that reduced merge risk

| Handoff | Producer | Consumer | Contract used |
|---|---|---|---|
| Client request format and benchmark flags | Jatin | Karthik | Stable gateway API shape + CLI arguments |
| Inference response schema | Karthik | Jatin | JSON detection payload contract used by renderer/CLI |
| Service and infra assumptions | Karthik | Santrupti | Container names, ports, env vars, and GPU runtime requirements |
| Dashboards and metric semantics | Karthik + Jatin | Whole team | Shared Prometheus metric names and panel annotations |

This explicit producer/consumer framing helped keep responsibilities clear during rapid changes in Phases 3-4.

### Lessons learned

1. **Three parallel prototypes was the right call.** Cost: ~1 week of "duplicated" effort. Benefit: Phase 2 convergence on Karthik's design was uncontested because the alternatives had been built and could be compared concretely. We would do this again.
2. **Type-system invariants need to be enforced in code, not in human memory.** The FP16 incident traced to an FP32 assumption that lived in three files and a model export script, none of which referenced each other. Future Triton configurations should be validated programmatically against the exported model's actual dtype.
3. **Bench automation pays for itself after one run.** The 6-config sweep that took ~25 minutes via `bench_run.py` would have taken half a day to run manually with consistent methodology. The script is now reusable for future tuning experiments (e.g., post-TensorRT enablement).

---