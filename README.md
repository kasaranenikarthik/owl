# OWL: YOLO Inference Pipeline

OWL is a distributed object detection pipeline built around YOLOv8 and Triton Inference Server.

## Project Description

OWL is designed to run object detection at scale with clean separation between ingestion, inference, and post-processing.
The system targets production-style workloads where requests may come in bursts and where observability and resilience are important.

Core goals:

- Provide a reliable API path for image/object-detection requests.
- Decouple request intake from heavy inference work.
- Integrate Triton + YOLOv8 for fast model serving.
- Support similarity-aware behavior and caching to avoid repeated expensive work.
- Expose metrics and health signals for operations.

# Architecture
![alt text](Architecture.png)


## Data Flow

The high-level data path is:

1. A client request is sent from `yolo_client` (or another producer) to the gateway using web sockets.
2. The gateway validates/parses request payloads and publishes jobs/events via Kafka.
3. The inference service consumes work items from Kafka.
4. The inference service prepares tensors/inputs and calls Triton (`internal/triton/client.go`).
5. Triton runs the YOLO model and returns detections.
6. Post-processing is applied (for example, filtering, formatting, and optional similarity/cache checks in `internal/similarity`).
7. The processed result is emitted back through the service boundary (response/event), and metrics are recorded.

Supporting flow behavior:

- Metrics (`internal/metrics`) track service health, throughput, and failures.
- Kafka producers/consumers (`internal/kafka`) provide backpressure-friendly buffering between components.

## Design Rationale

This project uses a modular service design with shared internal packages.

Why this design works:

- Separation of concerns: gateway and inference paths evolve independently.
- Scalability: Kafka allows horizontal scaling of consumers without tightly coupling API latency to inference latency.
- Maintainability: config, messaging, model access, similarity logic, and shutdown mechanics are isolated by package.
- Reliability: graceful shutdown and asynchronous messaging reduce failure amplification during deploys or spikes.
- Portability: Kubernetes/Terraform assets make it easier to move from local development to managed environments.

Tradeoffs:

- Added operational complexity (Kafka + Triton + multiple services).
- More moving parts means stronger need for tracing, metrics, and configuration discipline.

In short, OWL optimizes for robust, production-oriented inference orchestration rather than a single-process demo pipeline.

## Repository Layout

```text
owl/
	server/          # Go services (gateway + inference)
	yolo_client/     # Python client and CLI
	k8s/             # Kubernetes manifests
	terraform/       # Terraform definitions
```

## Prerequisites

- Go
- Python 3.10+
- Docker
- Kafka and Triton Inference Server for full end-to-end flow

## Go Server

From the `server/` directory:

```bash
go mod tidy
go run ./cmd/gateway
```

In a separate terminal, run the inference service if needed:

```bash
go run ./cmd/inference
```

## Python Client

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install websockets
pip install opencv-python
```

Run the CLI (example):

```bash
python -m cli.py --help
```

## Infrastructure

- Kubernetes manifests are in `k8s/`.
- Terraform definitions are in `terraform/`.
