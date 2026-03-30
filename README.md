# Distributed Real-Time Video Inference Pipeline

## Cloud Infrastructure + Core Pipeline

This branch contains the **cloud deployment infrastructure** and **core distributed pipeline** for the OWL project. It provides the foundational services (ingest, inference worker, aggregator) deployed on AWS via Terraform-managed ECS with GPU support.

### What's here

```
├── src/
│   ├── ingest-service/          # Reads video → publishes frames to Kafka
│   ├── inference-worker/        # Consumes frames → YOLOv8 on GPU → publishes detections
│   └── aggregator/              # Consumes detections → reorder buffer → REST API
├── terraform/                   # Full AWS infrastructure (VPC, ECR, ECS, CloudWatch)
├── config/                      # Prometheus + Grafana configs (observability)
├── scripts/                     # Build, push, and deployment helpers
├── docker-compose.local.yml     # Run everything locally (no AWS, CPU-only)
└── .gitignore
```

### Architecture

```
Video → Ingest → Kafka [raw-frames] → GPU Worker (YOLOv8) → Kafka [detections] → Aggregator (REST API)
```

All services run as ECS tasks on a single `g4dn.xlarge` EC2 instance (NVIDIA T4 GPU), orchestrated by Terraform. Services communicate via host networking through Kafka.

### Quick start — local (no AWS)

```bash
docker compose -f docker-compose.local.yml up --build
# Wait ~2 minutes, then:
curl http://localhost:8080/health
curl http://localhost:8080/stats | python3 -m json.tool
curl http://localhost:8080/streams/stream-0 | python3 -m json.tool
```

Runs on CPU (~5-15 fps). No GPU required. Good for development and testing.

### Quick start — AWS (GPU)

```bash
# 1. Configure
cd terraform/
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set your key_pair_name

# 2. Deploy
terraform init && terraform apply

# 3. SSH in and build images
ssh -i ~/.ssh/<key>.pem ec2-user@<IP>
# Copy src/ and scripts/ to the instance, then:
./scripts/build-and-push.sh <ACCOUNT_ID> us-east-1

# 4. Force ECS to pull new images
./scripts/force-redeploy.sh us-east-1

# 5. Test
curl http://<IP>:8080/stats | python3 -m json.tool
```

### API endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Health check |
| `GET /stats` | Pipeline stats: throughput, uptime, per-stream metrics |
| `GET /streams` | List all active video streams |
| `GET /streams/{id}` | Latest detections for a specific stream |

### How it connects to the team

- **Jatin's branch**: Client pipeline, Python prototype, observability assets
- **Karthik's branch**: Go gateway, Triton inference server, Redis caching, Kafka wiring
- **This branch (Santrupti)**: Cloud infrastructure (Terraform/ECS/ECR), core pipeline services, GPU deployment

The selected final architecture uses Karthik's Triton-based server. This branch provides the deployment foundation and the initial distributed pipeline that informed the final design.

### AWS resources created by Terraform

- VPC with 2 public subnets + internet gateway
- 3 ECR repositories (ingest, worker, aggregator)
- ECS cluster with EC2 launch type (GPU-optimized AMI)
- Auto Scaling Group (1x g4dn.xlarge)
- 4 ECS task definitions + services (Kafka, ingest, worker, aggregator)
- CloudWatch log group

### Cost

- `g4dn.xlarge`: ~$0.53/hr — **always stop when not using**
- Pause: `./scripts/pause-instance.sh`
- Resume: `./scripts/resume-instance.sh`
- Destroy: `cd terraform && terraform destroy`
