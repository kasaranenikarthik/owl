# OWL Project Report

## Problem, Team, and Overview of Experiments

Our project, OWL, targets real-time object detection over live video streams in a distributed setting. The practical problem is that raw video is easy to capture but expensive to process at low latency once multiple streams, high-resolution frames, and bursty workloads are involved. Future stakeholders include traffic-monitoring teams, campus safety operators, warehouse managers, and any engineering team that needs near-real-time object detection without tying the user-facing application directly to GPU inference latency.

Our team contributions are reflected in the branch structure and component ownership. Jatin's work focuses on the user-facing client pipeline and the earlier Python-based distributed prototype, including ingestion, aggregation, WebSocket delivery, and local observability assets. Santrupti's work focuses on deployment architecture, especially Terraform, VPC/ECR/ECS provisioning, logging, and the cloud runtime needed to move the system from local development to managed infrastructure. Karthik's work focuses on the server-side runtime that we selected as the main architecture for the project: a Go gateway and inference bridge around Kafka, Redis, and NVIDIA Triton Inference Server. We chose Karthik's architecture because Triton gives us a stronger production story for GPU model serving than directly embedding inference inside application workers. Triton standardizes model serving, improves scaling options, and aligns better with the course goal of building a distributed, observable system rather than only a local demo.

The experiments for OWL evaluate system-level tradeoffs rather than only model accuracy. The main outcomes we care about are end-to-end latency, inference latency, throughput in frames per second, stale-frame drop behavior, cache hit rate, worker queue depth, and the system's behavior under overload or partial failure. AI is central to the product itself because YOLOv8 performs the object detection, and Triton is the serving layer for that AI model in the selected architecture. We are also using AI carefully during development for code scaffolding, documentation drafting, and refactoring support, but the core engineering decisions and validation still depend on manual review and runtime checks. Observability is built into the design through Prometheus metrics, Grafana dashboards, health endpoints, Kafka lag monitoring, and planned GPU metrics collection so that performance and failure behavior are measurable rather than anecdotal.

## Project Plan and Recent Progress

Recent progress happened along three parallel tracks. First, the client pipeline in `yolo_client` is working today: it can capture frames from a webcam, video file, or RTSP/RTMP source, JPEG-encode them, send them over WebSocket, and render returned detections with session statistics. Second, Karthik's branch established the architecture we intend to keep for the server side: Go services for the gateway and inference bridge, Redis-based similarity caching, Kafka-based decoupling between intake and inference, and Triton-backed YOLOv8 serving. Third, Santrupti's branch created the infrastructure baseline for cloud deployment with Terraform modules for networking, logging, ECR, and ECS services.

The most important current status point is that only the client side is working as a verified integrated path right now. We validated the client locally using the included dummy WebSocket server and confirmed that two frames were sent and two detection responses were received successfully. The server-side code is not yet the active shared end-to-end path for the team submission; that code will be integrated from the other branch as part of the next merge phase. In other words, our chosen architecture is Karthik's Triton-based design, but our currently working demonstration is still centered on the client path.

Our current plan is:

| Time Window | Planned Work | Primary Owner(s) |
| --- | --- | --- |
| March 29-April 2, 2026 | Merge the selected server code path from Karthik's branch into the shared branch and stabilize interfaces between `yolo_client` and the Go gateway | Karthik, Jatin |
| April 2-April 6, 2026 | Wire Kafka, Redis, and Triton together for an end-to-end server run and validate stale-frame and cache behavior | Karthik |
| April 6-April 10, 2026 | Connect deployment assets from Santrupti's branch to the selected runtime and clean up environment/config handling | Santrupti |
| April 10-April 14, 2026 | Run performance experiments, collect observability data, and finalize report/results | Entire team |

AI changes the cost-benefit profile of the project in two ways. On the benefit side, the system becomes useful because automated object detection replaces manual video inspection, and development effort is reduced by using AI tools for boilerplate and iteration support. On the cost side, AI introduces GPU serving cost, Triton operational complexity, model lifecycle management, and the need to monitor reliability, latency, and false positives. That is why observability is not optional in our plan; it is the mechanism we will use to keep AI performance, reliability, and cost understandable for stakeholders.

## Objectives

Our short-term objective is to ship an end-to-end distributed video inference pipeline that demonstrates the following path: source video capture, client transmission, asynchronous job handoff, GPU-backed object detection, and structured result delivery back to the user. For the course timeline, success means that we can show measurable latency and throughput, explain how the system behaves under load, and demonstrate the role of observability in debugging and tuning the system.

Our long-term objective goes beyond the course project. We want OWL to become a reusable distributed inference template for edge-to-cloud video analytics. In that version, the system would support multiple concurrent streams, safer deployment controls, autoscaling, richer model management, and more formal failure-handling policies around dropped frames, backpressure, and reordering. We also want the platform to be extensible to other models beyond YOLOv8.

For stakeholders deploying an AI-enabled version of this system, we will control performance, reliability, and cost through bounded queues, stale-frame dropping, similarity-based caching, Triton-backed model serving, and metrics-driven capacity planning. These design choices are intended to prevent the common failure mode where a system appears to work at low load but collapses once inference becomes the bottleneck.

## Related Work

OWL sits at the intersection of distributed streaming systems, GPU model serving, and computer vision inference. From the computer vision side, YOLOv8 is relevant because it provides strong real-time object detection performance and a straightforward path to deployable inference. From the model serving side, NVIDIA Triton is relevant because it externalizes inference behind a dedicated serving layer instead of embedding the model directly into the application server. From the distributed systems side, Kafka is relevant because it provides durable buffering, consumer groups, partition-based scaling, and clean separation between request intake and inference execution. Redis is relevant in our design as a lightweight similarity cache that reduces repeated expensive inference on near-duplicate frames.

Within our own team repository, we already explored two related architectural directions before selecting the final one. Jatin's earlier Python architecture uses an ingest service, inference workers, and an aggregator with REST/WebSocket delivery and micro-batching. Santrupti's architecture extends that work into deployable cloud infrastructure with Terraform and ECS. Those branches were valuable because they established the distributed shape of the problem, but we selected Karthik's architecture as the final direction because Triton gives us a more credible production inference layer and a cleaner separation between application logic and model serving.

The instruction also asks for three related Piazza projects. Those specific Piazza references are not present in the repository snapshot used for this draft, so they should be inserted before final submission rather than invented here. The correct comparison section should identify three projects that overlap with our work in distributed streaming, AI inference, or observability, and then explain where they share architectural goals with OWL and where our Triton-centered design differs.

## Methodology

The selected methodology is a staged distributed pipeline with explicit service boundaries. At the client and edge layer, video comes from a webcam, file, or RTSP/RTMP stream and is handled by the Python `yolo_client`, which captures frames, encodes JPEG payloads, sends them over WebSocket, and renders detections plus latency statistics. At the server edge, the Go gateway accepts WebSocket traffic, assigns per-client frame IDs, and performs similarity checks using Redis. Cache hits let the system return the last suitable result immediately; cache misses are published to Kafka.

The next stage is asynchronous inference processing. Kafka decouples intake from inference so that user-facing latency is not directly bound to the GPU-serving path. The Go inference bridge consumes frames from Kafka, drops stale work, performs preprocessing with GoCV, and calls Triton over HTTP for YOLOv8 inference. Triton returns raw tensor output, the bridge post-processes detections, and results are published back to a Kafka detections topic. The gateway consumes those detections, updates the similarity cache, drops stale out-of-order results, and returns the final JSON response to the client.

The complete architecture we are documenting includes all three team contributions:

1. Jatin architecture: a Python-first distributed prototype with ingest, inference, and aggregator services, FastAPI/WebSocket delivery, micro-batching, and local Docker observability.
2. Santrupti architecture: Terraform modules for VPC, ECR, ECS, logging, and GPU-aware deployment on AWS infrastructure.
3. Selected Karthik architecture: Python client plus Go gateway, Redis similarity cache, Kafka topics for `frames` and `detections`, Go inference bridge, Triton-backed YOLOv8s serving, and Prometheus/Grafana-style observability.

We chose the third architecture as the canonical design because Triton improves feasibility for production-style AI serving. It lets us separate model serving from API and queue handling, makes GPU serving a first-class concern, and fits better with planned scaling experiments.

The experiments we will run are designed around explicit tradeoffs:

| Experiment | Independent Variables | Dependent Variables |
| --- | --- | --- |
| Throughput versus latency | worker count, queue depth, Kafka partitions | p50/p95 latency, frames per second, lag |
| Cache effectiveness | similarity threshold, cache TTL | cache hit rate, avoided inferences, latency improvement |
| Overload behavior | input frame rate above capacity | stale-frame drops, queue growth, responsiveness |
| Deployment/runtime comparison | Python worker path versus Triton-backed path | operational complexity, inference stability, observability quality |
| Reliability under faults | service restarts, slow workers | recovery time, lost frames, backlog growth |

Observability is part of the methodology, not an afterthought. The Go services already expose Prometheus metrics for connected clients, cache hits and misses, end-to-end latency, inference duration, detections per frame, queue depth, and worker pool capacity. The Python prototype also includes Prometheus/Grafana assets. In the full integrated system we will use these metrics, along with Kafka lag and GPU telemetry, to evaluate both performance and failure behavior.

## Preliminary Results

Our preliminary results are strongest on the client side and architectural validation. The working `yolo_client` already supports webcam, file, and stream inputs; JPEG encoding; asynchronous WebSocket send/receive loops; and rendering of detections and runtime statistics. In a local validation run with the included dummy WebSocket server, the client successfully connected, sent two frames, and received two responses. That confirms the client path and the user-facing interaction model are already functional.

On the server side, we have a complete selected architecture and codebase shape, but not a fully merged, verified end-to-end run in the shared submission path yet. The Go gateway and inference bridge, Redis similarity cache, Triton client, Kafka producer/consumer layers, and metrics package all exist in Karthik's branch architecture. However, the server code still needs to be integrated from the other branch into the final shared path and then exercised end to end with Kafka, Redis, and Triton available together. That integration step is the main remaining systems task.

The most important remaining data to collect are end-to-end latency under realistic video load, inference latency under Triton, cache hit behavior on repeated or visually similar frames, queue depth under overload, and recovery behavior when workers lag or restart. Our expected pathological worst case is a high-frame-rate, high-resolution input stream that changes every frame and therefore defeats similarity caching while also exceeding the throughput capacity of the inference workers. In that case, queue growth and stale-frame dropping become the dominant behaviors. Our expected base case is a moderate-FPS stream with some visual redundancy, where caching and asynchronous buffering should reduce perceived latency while preserving useful detections.

## Impact

The anticipated impact of OWL is that it turns a common AI demo problem into a real distributed systems problem with measurable engineering tradeoffs. Many object detection demos stop at "the model works." Our project is more useful because it asks what happens when that model is embedded inside a system with queues, clients, cache behavior, GPU serving, deployment concerns, and observability requirements. That makes the results relevant both to the course and to anyone building production-style AI pipelines.

Other students could use our project in two ways. First, they could treat OWL as a reference architecture for distributed AI inference, especially if they need an example of Kafka-based decoupling, WebSocket result delivery, and observability instrumentation. Second, they could help test the system by acting as external clients, generating streams, or comparing alternative inference-serving strategies. The reason people should care about the results is not just that we detect objects; it is that we are measuring how to do AI inference in a way that is scalable, observable, and operationally credible.
