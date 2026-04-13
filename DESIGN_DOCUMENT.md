# Distributed Real-Time Video Inference Pipeline
## Design Document - First Iteration

---

## 1. System Overview

This system processes live video streams in real-time using horizontally scalable GPU inference workers. It extracts frames from video sources, routes them through Kafka to an inference bridge, performs YOLOv8 object detection through NVIDIA Triton Inference Server, and delivers ordered detection results through REST and WebSocket APIs.

**Baseline before Triton rollout:** Processing 1920x1080 @ 30fps traffic video, detecting cars, motorcycles, and people with ~14ms inference latency per frame on a single GPU worker.

---

## 2. Architecture

```
                        DISTRIBUTED VIDEO INFERENCE PIPELINE
                        ====================================

  Video Source(s)                 Kafka Cluster                 Bridge + Serving
  ===============          =========================         =====================

  +-------------+          +----------------------+
  | RTSP Stream |          |   video.frames       |
  | MP4 File    |---+--->  |   (6 partitions)     |---+---> +-------------------+
  | Webcam      |   |      |   key = stream_id    |         | Inference Bridge   |
  +-------------+   |      +----------------------+         | (Kafka -> Triton)  |
                    |                                       +---------+---------+
  +-------------+   |                                                 |
  | Ingest Svc  |---+                                                 | HTTP
  | (OpenCV +   |                                                     v
  |  Kafka Pub) |                                         +---------------------+
  +-------------+                                         | Triton Server       |
       |                                                  | (YOLOv8 ONNX model) |
       | Prometheus                                       +----------+----------+
       | :8001                                                       |
       |                                                             | publish
       |                                                             v
                                                        +----------------------+
                                                        |  video.detections    |
                                                        |  (6 partitions)      |
                                                        |  key = stream_id     |
                                                        +----------+-----------+
                                                                   |
                                                                   v
                                                        +----------+-----------+
                                                        |   AGGREGATOR SVC     |
                                                        |   (FastAPI + async)  |
                                                        |                      |
                                                        |  +-Ordering Buffer-+ |
                                                        |  | Per-stream      | |
                                                        |  | min-heap        | |
                                                        |  | reorder logic   | |
                                                        |  +-----------------+ |
                                                        |                      |
                                                        |  REST  :8080         |
                                                        |  WS    :8080/ws/     |
                                                        |  Prom  :8003         |
                                                        +----------------------+
                                                                   |
                                              +--------------------+--------------------+
                                              v                    v                    v
                                     GET /api/streams    GET /detections     WS /ws/{stream_id}
                                                         ?since_frame_id=N   (real-time push)


  ===================  OBSERVABILITY LAYER  ===================

  +------------------+       +------------------+
  |   Prometheus     |<------| Scrape targets:  |
  |   :9090          |       |  ingest:8001     |
  |                  |       |  inference:8002  |
  |                  |       |  triton:8002     |
  +--------+---------+       |  aggregator:8003 |
           |                 |  aggregator:8080 |
           v                 +------------------+
  +--------+---------+
  |   Grafana        |   Dashboard: "Video Inference Pipeline"
  |   :3000          |   - Frames ingested/sec
  |   admin/admin    |   - Inference throughput/sec per worker
  |                  |   - Inference latency p50/p95/p99
  +------------------+   - Batch size distribution
                         - Kafka consumer lag
                         - Reorder buffer size
                         - Detections/sec
                         - Active WebSocket connections
```

---

## 3. Data Flow

```
Frame Lifecycle:

  OpenCV capture
       |
       v
  JPEG encode (cv2.imencode)
       |
       v
  Base64 encode --> FrameMessage (Pydantic JSON)
       |
       v
  Kafka produce (key=stream_id, topic=video.frames)
       |
       v
  Kafka consume (consumer group: inference-workers)
       |
       v
  Base64 decode --> JPEG decode --> numpy array
       |
       v
  Collect up to N frames for the bridge dispatch window
       |
       v
  Per-frame HTTP inference requests to Triton
       |
       v
  Triton dynamic batching (preferred batch sizes 4 or 8, max queue delay 50ms)
       |
       v
  Unpack results --> DetectionMessage per frame
       |
       v
  Kafka produce (key=stream_id, topic=video.detections)
       |
       v
  Kafka consume (consumer group: aggregator)
       |
       v
  Ordering buffer (per-stream min-heap reorder)
       |
       v
  REST API / WebSocket broadcast
```

---

## 4. Component Details

### 4.1 Ingest Service

| Property       | Value                              |
|----------------|------------------------------------|
| Base image     | `python:3.12-slim` + ffmpeg        |
| Metrics port   | 8001                               |
| Kafka key      | `stream_id` (partition affinity)   |
| Encoding       | JPEG -> Base64 -> JSON             |
| Rate control   | Sleeps to match source FPS         |
| Frame skip     | Configurable (`FRAME_SKIP=1`)      |
| Loop behavior  | Loops video on EOF                 |

**Key file:** `ingest/frame_publisher.py` - OpenCV capture loop with Kafka producer.

### 4.2 Inference Service

| Property       | Value                                          |
|----------------|------------------------------------------------|
| Base image     | `pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime`|
| Metrics port   | 8002                                           |
| Default backend| Triton HTTP bridge                             |
| Fallback       | Local YOLOv8 inference via Ultralytics         |
| Model serving  | Triton + ONNX model repository                 |
| Consumer group | `inference-workers`                            |
| Dispatch window| Up to `TRITON_MAX_INFLIGHT` frames OR `TRITON_COLLECT_TIMEOUT_MS` |
| Triton batching| Dynamic batching in Triton (`[4,8]`, 50ms queue delay) |
| Commit         | Manual, after successful publish               |

**Key files:**
- `inference/consumer.py` - Polling windows from Kafka
- `inference/batcher.py` - Frame decode + detection unpacking
- `inference/model.py` - Backend selection, local YOLO, and Triton bridge client
- `inference/export_triton_repo.py` - ONNX export + Triton model repository bootstrap

### 4.3 Aggregator Service

| Property       | Value                              |
|----------------|------------------------------------|
| Base image     | `python:3.12-slim`                 |
| API port       | 8080 (REST + WebSocket)            |
| Metrics port   | 8003 (native) + 8080/metrics (FastAPI) |
| Consumer group | `aggregator`                       |
| Framework      | FastAPI + uvicorn                  |

**Endpoints:**

| Method | Path                                    | Description                    |
|--------|-----------------------------------------|--------------------------------|
| GET    | `/health`                               | Liveness check                 |
| GET    | `/api/streams`                          | List active stream IDs         |
| GET    | `/api/streams/{id}/detections?since_frame_id=N&limit=100` | Ordered detections |
| WS     | `/ws/{stream_id}`                       | Real-time detection push       |

**Ordering buffer algorithm:**
1. First detection for a stream initializes `next_expected = frame_id`
2. Each detection is pushed onto a per-stream min-heap
3. Consecutive frames starting at `next_expected` are drained and emitted
4. Duplicates (`frame_id < next_expected`) are dropped
5. Recent 500 ordered detections are kept in a bounded deque per stream

### 4.4 Kafka Configuration

| Topic             | Partitions | Retention | Max Msg Size | Purpose         |
|-------------------|------------|-----------|--------------|-----------------|
| `video.frames`    | 6          | 5 min     | 10 MB        | JPEG frames     |
| `video.detections`| 6          | 10 min    | default      | Detection JSON  |

**Broker:** Kafka 4.0 in KRaft mode (no Zookeeper).

### 4.5 Observability

**Prometheus metrics:**

| Metric                        | Type      | Labels      | Service    |
|-------------------------------|-----------|-------------|------------|
| `frames_published_total`      | Counter   | stream_id   | ingest     |
| `frames_consumed_total`       | Counter   | worker_id   | inference  |
| `inference_duration_seconds`  | Histogram | worker_id   | inference  |
| `batch_size_actual`           | Histogram | -           | inference  |
| `detections_published_total`  | Counter   | -           | inference  |
| `triton_requests_total`       | Counter   | worker_id   | inference  |
| `triton_request_failures_total` | Counter | worker_id, reason | inference |
| `triton_inflight_requests`    | Gauge     | worker_id   | inference  |
| `detections_consumed_total`   | Counter   | -           | aggregator |
| `reorder_buffer_size`         | Gauge     | stream_id   | aggregator |
| `websocket_connections`       | Gauge     | -           | aggregator |
| `kafka_consumer_lag`          | Gauge     | topic, partition | shared |

**Grafana dashboard** auto-provisions 8 panels covering throughput, latency percentiles, batch distribution, Kafka lag, buffer size, and connection count. Refreshes every 5 seconds.

---

## 5. Message Schemas

### FrameMessage

```json
{
  "stream_id": "stream-0",
  "frame_id": 1234,
  "timestamp": 1774738860.59,
  "width": 1920,
  "height": 1080,
  "encoding": "jpeg",
  "data": "<base64-encoded JPEG bytes>"
}
```

### DetectionMessage

```json
{
  "stream_id": "stream-0",
  "frame_id": 1234,
  "timestamp": 1774738860.59,
  "detections": [
    {
      "x1": 253.3, "y1": 632.8,
      "x2": 611.2, "y2": 816.3,
      "confidence": 0.84,
      "class_id": 2,
      "class_name": "car"
    }
  ],
  "inference_time_ms": 14.19
}
```

---

## 6. Key Design Decisions and Tradeoffs

### 6.1 Kafka as the Streaming Backbone

| Aspect        | Decision                          | Tradeoff                                                                 |
|---------------|-----------------------------------|--------------------------------------------------------------------------|
| **Buffering** | Kafka decouples producers/consumers | Adds latency (serialize, network, deserialize) vs direct function calls |
| **Ordering**  | `key=stream_id` for partition affinity | Single stream bound to one partition; limits per-stream parallelism     |
| **Durability** | Short retention (5-10 min)       | Frames are ephemeral; can't replay old data, but saves disk             |
| **Protocol**  | JSON over Kafka                   | Human-readable, debuggable; ~33% size overhead from Base64 encoding     |

**Why Kafka over Redis Streams / RabbitMQ:** Kafka provides durable log semantics, consumer groups for horizontal scaling, and partition-based ordering guarantees. Consumer group rebalancing automatically redistributes work when workers join/leave.

**What we give up:** Higher latency than in-process queues. Base64+JSON encoding adds ~33% overhead vs raw bytes. For a production system, Avro/Protobuf with a schema registry would reduce this.

### 6.2 Triton Dispatch + Dynamic Batching Strategy

| Aspect             | Decision                                      | Tradeoff                                               |
|--------------------|-----------------------------------------------|--------------------------------------------------------|
| **Dispatch window**| Up to `TRITON_MAX_INFLIGHT` frames            | Controls bridge concurrency and request fan-out        |
| **Collect timeout**| `TRITON_COLLECT_TIMEOUT_MS`                   | Bounds bridge wait time under low input volume         |
| **Triton batching**| Preferred batch sizes `[4, 8]`                | Better GPU efficiency without Python-side model batches |
| **Queue delay**    | `TRITON_DYNAMIC_BATCH_DELAY_US=50000`         | Lets Triton coalesce requests, but adds queueing delay |

**Why this matters for experiments:** Experiment 2 now tunes both bridge concurrency and Triton dynamic batching to find the throughput/latency sweet spot.

### 6.3 Ordering Guarantees

| Aspect           | Decision                                | Tradeoff                                               |
|------------------|-----------------------------------------|--------------------------------------------------------|
| **Per-stream**   | Min-heap reorder buffer in aggregator   | Extra memory + latency for out-of-order frames         |
| **Initialization** | `next_expected` set to first frame seen | Tolerates late-joining consumers; may miss early frames |
| **Buffer bound** | 500 recent detections per stream        | Prevents OOM; old detections are evicted               |
| **Cross-stream** | No global ordering                      | Each stream is independent; simpler, but can't correlate across streams |

**Key tradeoff:** If a frame is lost (worker crash mid-batch), the ordering buffer blocks indefinitely waiting for that frame_id. Experiment 4 (fault injection) will expose this and motivate adding a gap-detection timeout.

### 6.4 At-Least-Once Semantics

| Aspect         | Decision                               | Tradeoff                                               |
|----------------|----------------------------------------|--------------------------------------------------------|
| **Commit**     | Manual commit after successful publish | No data loss; may produce duplicate detections on crash |
| **Dedup**      | Ordering buffer drops `frame_id < next_expected` | Handles most duplicates; not a full exactly-once guarantee |
| **Alternative** | Kafka transactions (exactly-once)     | Higher complexity and latency; overkill for this use case |

**Why not exactly-once:** The ordering buffer's duplicate-drop logic handles the common case. A duplicate detection is far less harmful than a missed one in a real-time video system.

### 6.5 KRaft Mode Kafka (No Zookeeper)

| Aspect       | Decision                     | Tradeoff                                                  |
|--------------|------------------------------|-----------------------------------------------------------|
| **Simplicity** | Single-node KRaft           | One fewer container, less memory, simpler ops              |
| **HA**       | No replication               | Single point of failure; acceptable for dev/experimentation |
| **Scaling**  | Can't add brokers easily     | Would need to reconfigure for multi-broker in production   |

### 6.6 JSON + Base64 Message Encoding

| Aspect          | Decision                    | Tradeoff                                                   |
|-----------------|-----------------------------|------------------------------------------------------------|
| **Debuggability** | JSON is human-readable    | Can `kafka-console-consumer` and inspect messages directly |
| **Size**        | Base64 adds ~33% overhead   | A 100KB JPEG becomes ~133KB on the wire                    |
| **Schema**      | Pydantic validation         | Runtime type checking; no compile-time schema registry     |
| **Alternative** | Avro/Protobuf + Schema Registry | ~50% smaller messages, stricter contracts, more infra   |

**Why JSON for now:** First iteration prioritizes debuggability and fast development. The overhead is acceptable for experiments. Schema evolution is easy with Pydantic's optional fields.

### 6.7 YOLOv8 Nano Model

| Aspect         | Decision                    | Tradeoff                                                  |
|----------------|-----------------------------|------------------------------------------------------------|
| **Speed**      | YOLOv8n: ~14ms/frame on GPU | Fastest YOLO variant; good for real-time                   |
| **Accuracy**   | Lower mAP than YOLOv8s/m/l  | Acceptable for detecting large objects (cars, people)      |
| **Memory**     | ~6.2MB model, ~1GB GPU RAM  | Leaves headroom for batching on consumer GPUs              |
| **Alternative** | YOLOv8s/m with TensorRT    | Better accuracy, but 2-5x slower without TensorRT optimization |

### 6.8 Single Aggregator Instance

| Aspect         | Decision                        | Tradeoff                                               |
|----------------|---------------------------------|--------------------------------------------------------|
| **Simplicity** | One aggregator, one reorder buffer | No distributed state coordination needed              |
| **Bottleneck** | All detections flow through one node | Can become a bottleneck at high stream counts        |
| **WebSocket**  | All WS connections on one server | Limits to ~10K concurrent connections                  |
| **Alternative** | Sharded aggregators by stream_id | Horizontal scaling, but needs service discovery + routing |

---

## 7. Known Limitations

1. **Ordering buffer stall:** If a frame is permanently lost, the buffer blocks waiting for that `frame_id`. Needs a configurable gap timeout to skip missing frames.

2. **No backpressure signaling:** When inference workers fall behind, Kafka lag grows unboundedly. The ingest service has no feedback mechanism to slow down. Experiment 3 will evaluate policies for this.

3. **Single partition per stream:** All frames from one stream go to one partition, processed by one worker. A high-FPS stream can't be parallelized across workers (without breaking ordering at the Kafka level).

4. **Cold-start model export:** The Triton repository is persisted in a named volume, but the first boot still has to export the YOLO model to ONNX before Triton can load it.

5. **No authentication:** All API endpoints and WebSocket connections are unauthenticated. Acceptable for local experimentation only.

6. **Base64 overhead:** ~33% message size increase. At 30fps 1080p, each frame message is ~150-200KB. At scale, this adds significant network and Kafka storage pressure.

---

## 8. Infrastructure Summary

```
CONTAINER        IMAGE                                      PORTS              GPU
---------        -----                                      -----              ---
kafka            bitnamilegacy/kafka:4.0.0-debian-12-r10    9092               -
kafka-init       bitnamilegacy/kafka:4.0.0-debian-12-r10    (exits after init) -
ingest           python:3.12-slim + ffmpeg + OpenCV          8001               -
triton-model-init pytorch:2.2.0-cuda12.1 + ultralytics      (exits after init) -
triton           nvcr.io/nvidia/tritonserver:26.02-py3      8004(host), 8002   1x NVIDIA
inference        pytorch:2.2.0-cuda12.1 + ultralytics       8002               optional
aggregator       python:3.12-slim + FastAPI                  8080, 8003         -
prometheus       prom/prometheus:v2.51.0                     9090               -
grafana          grafana/grafana:10.4.0                      3000               -
```

**Volumes:** `kafka-data`, `triton-model-repo`, `prometheus-data`, `grafana-data` (named Docker volumes)
**Network:** `pipeline` (bridge)

---

## 9. How to Run

```bash
# Start everything
make up

# Check service status
docker compose ps

# View logs
make logs

# Access endpoints
#   API:        http://localhost:8080/api/streams
#   WebSocket:  ws://localhost:8080/ws/stream-0
#   Prometheus: http://localhost:9090
#   Grafana:    http://localhost:3000  (admin/admin)

# Run smoke test
make test

# Tear down
make down
```

---

## 10. Planned Experiments

| #  | Experiment                        | What Varies                              | What We Measure                                    |
|----|-----------------------------------|------------------------------------------|----------------------------------------------------|
| 1  | Horizontal scaling of GPU workers | Worker count (1,2,4), partition count    | Throughput, latency p50/p95/p99, Kafka lag, GPU util |
| 2  | Batch size vs latency tradeoff    | Batch size (1-16), timeout (5-50ms)      | Throughput, p95/p99 latency, GPU utilization        |
| 3  | Overload & backpressure policy    | Input rate beyond capacity + drop policy | Tail latency, lag growth, drop rate, staleness      |
| 4  | Fault injection under load        | Kill/restart workers, simulate slow worker | Recovery time, lag spike, p99 during recovery     |
| 5  | Ordering under distributed processing | Artificial jitter, heterogeneous workers | Reorder rate, buffer size, ordering latency      |

Each experiment will be run against the metrics exposed through Prometheus/Grafana to capture quantitative results.
