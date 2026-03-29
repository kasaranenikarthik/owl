# ═══════════════════════════════════════════════════════════
# ECS Cluster + EC2 GPU Instance + Task Definitions + Services
# ═══════════════════════════════════════════════════════════
#
# This module deploys all 5 containers (zookeeper, kafka, ingest,
# inference-worker, aggregator) on a single g4dn.xlarge instance
# using ECS with EC2 launch type.
#
# All tasks use "host" network mode — they share the EC2 host's
# network namespace. This means services communicate via localhost
# (e.g., Kafka at localhost:9092). Simple and perfect for Phase 1.
# Week 2 can migrate to awsvpc + service discovery for scaling.

# ── AMI: ECS-Optimized GPU (Amazon Linux 2) ──
# This AMI comes with: Docker, ECS Agent, NVIDIA drivers,
# NVIDIA Container Runtime — all pre-configured.
# The SSM parameter always points to the latest version.
data "aws_ssm_parameter" "ecs_ami" {
  name = var.use_gpu ? "/aws/service/ecs/optimized-ami/amazon-linux-2/gpu/recommended/image_id" : "/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id"
}

# ── ECS Cluster ──
resource "aws_ecs_cluster" "this" {
  name = "${var.name}-cluster"
}

# ── Launch Template ──
# Defines HOW the EC2 instance is created (AMI, instance type, etc.)
resource "aws_launch_template" "ecs" {
  name_prefix   = "${var.name}-ecs-"
  image_id      = data.aws_ssm_parameter.ecs_ami.value
  instance_type = var.instance_type
  key_name      = var.key_pair_name

  iam_instance_profile {
    name = var.iam_instance_profile_name
  }

  network_interfaces {
    associate_public_ip_address = true
    security_groups             = [var.security_group_id]
  }

  # user_data tells the ECS agent which cluster to join.
  # This is the magic that connects your EC2 instance to ECS.
  user_data = base64encode(<<-USERDATA
    #!/bin/bash
    echo "ECS_CLUSTER=${aws_ecs_cluster.this.name}" >> /etc/ecs/ecs.config
    echo "ECS_ENABLE_GPU_SUPPORT=true" >> /etc/ecs/ecs.config
  USERDATA
  )

  # 100 GB root volume — DLAMI + Docker images + video files need space
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = 100
      volume_type           = "gp3"
      delete_on_termination = true
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags = { Name = "${var.name}-ecs-gpu" }
  }
}

# ── Auto Scaling Group ──
# Phase 1: min=max=1 (single instance).
# Week 2: increase max for horizontal scaling experiments.
resource "aws_autoscaling_group" "ecs" {
  name                = "${var.name}-ecs-asg"
  min_size            = 1
  max_size            = 1
  desired_capacity    = 1
  vpc_zone_identifier = [var.subnet_id]

  launch_template {
    id      = aws_launch_template.ecs.id
    version = "$Latest"
  }

  # Let ECS manage instance lifecycle
  protect_from_scale_in = true

  tag {
    key                 = "AmazonECSManaged"
    value               = true
    propagate_at_launch = true
  }
}

# ── Capacity Provider (connects ASG to ECS) ──
resource "aws_ecs_capacity_provider" "gpu" {
  name = "${var.name}-gpu-cp"

  auto_scaling_group_provider {
    auto_scaling_group_arn         = aws_autoscaling_group.ecs.arn
    managed_termination_protection = "ENABLED"

    managed_scaling {
      status          = "ENABLED"
      target_capacity = 100
    }
  }
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = [aws_ecs_capacity_provider.gpu.name]

  default_capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.gpu.name
    weight            = 1
  }
}


# ════════════════════════════════════════════
# TASK DEFINITIONS
# ════════════════════════════════════════════
# All tasks use networkMode = "host" so they share the EC2
# host's network. Kafka is at localhost:9092, Zookeeper at
# localhost:2181, Aggregator at localhost:8080.

# ── Kafka + Zookeeper (combined into one task, two containers) ──
resource "aws_ecs_task_definition" "kafka" {
  family                = "${var.name}-kafka"
  network_mode          = "host"
  execution_role_arn    = var.execution_role_arn
  task_role_arn         = var.task_role_arn

  # Zookeeper + Kafka need ~3 GB RAM together
  cpu    = "1024"
  memory = "3072"

  container_definitions = jsonencode([
    {
      name      = "zookeeper"
      image     = "confluentinc/cp-zookeeper:7.7.1"
      essential = true
      cpu       = 256
      memory    = 512
      environment = [
        { name = "ZOOKEEPER_CLIENT_PORT", value = "2181" },
        { name = "ZOOKEEPER_TICK_TIME",   value = "2000" },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = var.log_group_name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "zookeeper"
        }
      }
    },
    {
      name      = "kafka"
      image     = "confluentinc/cp-kafka:7.7.1"
      essential = true
      cpu       = 768
      memory    = 2560

      # Wait for zookeeper container to start first
      dependsOn = [{ containerName = "zookeeper", condition = "START" }]

      environment = [
        { name = "KAFKA_BROKER_ID",                             value = "1" },
        { name = "KAFKA_ZOOKEEPER_CONNECT",                     value = "localhost:2181" },
        { name = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",        value = "PLAINTEXT:PLAINTEXT" },
        { name = "KAFKA_ADVERTISED_LISTENERS",                  value = "PLAINTEXT://localhost:9092" },
        { name = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR",      value = "1" },
        { name = "KAFKA_AUTO_CREATE_TOPICS_ENABLE",             value = "true" },
        { name = "KAFKA_MESSAGE_MAX_BYTES",                     value = "2000000" },
        { name = "KAFKA_REPLICA_FETCH_MAX_BYTES",               value = "2000000" },
        { name = "KAFKA_NUM_PARTITIONS",                        value = "1" },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = var.log_group_name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "kafka"
        }
      }
    }
  ])
}

# ── Ingest Service ──
resource "aws_ecs_task_definition" "ingest" {
  family             = "${var.name}-ingest"
  network_mode       = "host"
  execution_role_arn = var.execution_role_arn
  task_role_arn      = var.task_role_arn
  cpu                = "512"
  memory             = "1024"

  container_definitions = jsonencode([{
    name      = "ingest"
    image     = var.ingest_image
    essential = true
    cpu       = 512
    memory    = 1024
    environment = [
      { name = "KAFKA_BOOTSTRAP",  value = "localhost:9092" },
      { name = "TOPIC_RAW_FRAMES", value = "raw-frames" },
      { name = "VIDEO_SOURCE",     value = "/videos/test.mp4" },
      { name = "STREAM_ID",        value = "stream-0" },
      { name = "TARGET_FPS",       value = "5" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = var.log_group_name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "ingest"
      }
    }
  }])
}

# ── Inference Worker (with GPU) ──
resource "aws_ecs_task_definition" "worker" {
  family             = "${var.name}-worker"
  network_mode       = "host"
  execution_role_arn = var.execution_role_arn
  task_role_arn      = var.task_role_arn
  cpu                = "1024"
  memory             = "4096"

  container_definitions = jsonencode([{
    name      = "inference-worker"
    image     = var.worker_image
    essential = true
    cpu       = 1024
    memory    = 4096

    # ── GPU resource requirement ──
    # This tells ECS to assign 1 GPU to this container.
    # ECS uses the NVIDIA container runtime automatically
    # when a GPU resource is requested.
    resourceRequirements = var.use_gpu ? [
      { type = "GPU", value = "1" }
    ] : []

    environment = [
      { name = "KAFKA_BOOTSTRAP",       value = "localhost:9092" },
      { name = "TOPIC_RAW_FRAMES",      value = "raw-frames" },
      { name = "TOPIC_DETECTIONS",      value = "detections" },
      { name = "CONSUMER_GROUP",        value = "inference-workers" },
      { name = "WORKER_ID",            value = "worker-0" },
      { name = "MODEL_SIZE",           value = "yolov8n" },
      { name = "CONFIDENCE_THRESHOLD", value = "0.5" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = var.log_group_name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "worker"
      }
    }
  }])
}

# ── Aggregator ──
resource "aws_ecs_task_definition" "aggregator" {
  family             = "${var.name}-aggregator"
  network_mode       = "host"
  execution_role_arn = var.execution_role_arn
  task_role_arn      = var.task_role_arn
  cpu                = "512"
  memory             = "1024"

  container_definitions = jsonencode([{
    name      = "aggregator"
    image     = var.aggregator_image
    essential = true
    cpu       = 512
    memory    = 1024
    environment = [
      { name = "KAFKA_BOOTSTRAP",  value = "localhost:9092" },
      { name = "TOPIC_DETECTIONS", value = "detections" },
      { name = "CONSUMER_GROUP",   value = "aggregator" },
      { name = "REORDER_WINDOW",   value = "30" },
      { name = "API_PORT",         value = "8080" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = var.log_group_name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "aggregator"
      }
    }
  }])
}


# ════════════════════════════════════════════
# ECS SERVICES
# ════════════════════════════════════════════
# Services keep tasks running and restart them if they crash.
# On first `terraform apply`, the custom image services (ingest,
# worker, aggregator) will fail because images aren't in ECR yet.
# That's expected — once you push images, ECS retries automatically.

resource "aws_ecs_service" "kafka" {
  name            = "${var.name}-kafka"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.kafka.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.gpu.name
    weight            = 1
  }

  # Don't wait for steady state — Kafka takes time to initialize
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
}

resource "aws_ecs_service" "ingest" {
  name            = "${var.name}-ingest"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.ingest.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.gpu.name
    weight            = 1
  }

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  depends_on = [aws_ecs_service.kafka]
}

resource "aws_ecs_service" "worker" {
  name            = "${var.name}-worker"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.gpu.name
    weight            = 1
  }

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  depends_on = [aws_ecs_service.kafka]
}

resource "aws_ecs_service" "aggregator" {
  name            = "${var.name}-aggregator"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.aggregator.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.gpu.name
    weight            = 1
  }

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  depends_on = [aws_ecs_service.kafka]
}


# ════════════════════════════════════════════
# LOOK UP THE INSTANCE IP (for SSH + API access)
# ════════════════════════════════════════════
# The ASG creates the instance, but we need its public IP
# for the terraform output. This data source finds it.

data "aws_instances" "ecs" {
  filter {
    name   = "tag:Name"
    values = ["${var.name}-ecs-gpu"]
  }
  filter {
    name   = "instance-state-name"
    values = ["running"]
  }

  depends_on = [aws_autoscaling_group.ecs]
}
