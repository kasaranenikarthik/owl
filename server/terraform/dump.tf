###############################################################################
# Directory structure:
#
# terraform/
# ├── main.tf
# ├── variables.tf
# ├── outputs.tf
# ├── providers.tf
# └── modules/
#     ├── vpc/
#     │   ├── main.tf, variables.tf, outputs.tf
#     ├── eks/
#     │   ├── main.tf, variables.tf, outputs.tf
#     ├── redis/
#     │   ├── main.tf, variables.tf, outputs.tf
#     ├── kafka/
#     │   ├── main.tf, variables.tf, outputs.tf
#     └── monitoring/
#         ├── main.tf, variables.tf, outputs.tf
###############################################################################


###############################################################################
# terraform/variables.tf
###############################################################################

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "yolo-realtime"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "eks_cluster_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.30"
}

variable "cpu_instance_types" {
  type    = list(string)
  default = ["m5.xlarge"]
}

variable "gpu_instance_types" {
  type    = list(string)
  default = ["g4dn.xlarge"]
}

variable "gpu_min_size" {
  type    = number
  default = 1
}

variable "gpu_max_size" {
  type    = number
  default = 4
}

variable "grafana_admin_password" {
  type      = string
  sensitive = true
  default   = "changeme"
}


###############################################################################
# terraform/providers.tf
###############################################################################

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.0" }
    helm   = { source = "hashicorp/helm", version = "~> 2.12" }
    kubectl = { source = "gavinbunney/kubectl", version = "~> 1.14" }
  }
}

provider "aws" { region = var.region }

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_ca_data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
    }
  }
}

provider "kubectl" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_ca_data)
  load_config_file       = false
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
  }
}


###############################################################################
# terraform/main.tf
###############################################################################

module "vpc" {
  source       = "./modules/vpc"
  project_name = var.project_name
  vpc_cidr     = var.vpc_cidr
  region       = var.region
}

module "eks" {
  source             = "./modules/eks"
  project_name       = var.project_name
  cluster_version    = var.eks_cluster_version
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  cpu_instance_types = var.cpu_instance_types
  gpu_instance_types = var.gpu_instance_types
  gpu_min_size       = var.gpu_min_size
  gpu_max_size       = var.gpu_max_size
}

module "redis" {
  source                = "./modules/redis"
  project_name          = var.project_name
  vpc_id                = module.vpc.vpc_id
  private_subnet_ids    = module.vpc.private_subnet_ids
  eks_security_group_id = module.eks.cluster_security_group_id
}

module "kafka" {
  source     = "./modules/kafka"
  depends_on = [module.eks]
}

module "monitoring" {
  source                 = "./modules/monitoring"
  grafana_admin_password = var.grafana_admin_password
  depends_on             = [module.eks]
}


###############################################################################
# terraform/outputs.tf
###############################################################################

output "cluster_name"    { value = module.eks.cluster_name }
output "cluster_endpoint" { value = module.eks.cluster_endpoint }
output "redis_endpoint"   { value = module.redis.endpoint }
output "kubeconfig_cmd" {
  value = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}
output "grafana_cmd" {
  value = "kubectl port-forward svc/monitoring-grafana -n monitoring 3000:80"
}


###############################################################################
# modules/vpc/variables.tf
###############################################################################

variable "project_name" { type = string }
variable "vpc_cidr"     { type = string }
variable "region"       { type = string }


###############################################################################
# modules/vpc/main.tf
###############################################################################

data "aws_availability_zones" "available" {
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.project_name}-vpc"
  cidr = var.vpc_cidr
  azs  = slice(data.aws_availability_zones.available.names, 0, 3)

  private_subnets = [
    cidrsubnet(var.vpc_cidr, 8, 1),
    cidrsubnet(var.vpc_cidr, 8, 2),
    cidrsubnet(var.vpc_cidr, 8, 3),
  ]
  public_subnets = [
    cidrsubnet(var.vpc_cidr, 8, 101),
    cidrsubnet(var.vpc_cidr, 8, 102),
    cidrsubnet(var.vpc_cidr, 8, 103),
  ]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true

  public_subnet_tags  = { "kubernetes.io/role/elb" = 1 }
  private_subnet_tags = { "kubernetes.io/role/internal-elb" = 1 }
  tags                = { Project = var.project_name }
}


###############################################################################
# modules/vpc/outputs.tf
###############################################################################

output "vpc_id"             { value = module.vpc.vpc_id }
output "private_subnet_ids" { value = module.vpc.private_subnets }
output "public_subnet_ids"  { value = module.vpc.public_subnets }


###############################################################################
# modules/eks/variables.tf
###############################################################################

variable "project_name"       { type = string }
variable "cluster_version"    { type = string }
variable "vpc_id"             { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "cpu_instance_types" { type = list(string) }
variable "gpu_instance_types" { type = list(string) }
variable "gpu_min_size"       { type = number }
variable "gpu_max_size"       { type = number }
variable "cpu_min_size"       { type = number; default = 2 }
variable "cpu_max_size"       { type = number; default = 6 }
variable "cpu_desired_size"   { type = number; default = 3 }


###############################################################################
# modules/eks/main.tf
###############################################################################

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.project_name
  cluster_version = var.cluster_version
  vpc_id          = var.vpc_id
  subnet_ids      = var.private_subnet_ids

  cluster_endpoint_public_access = true
  enable_irsa                    = true

  cluster_addons = {
    coredns    = { most_recent = true }
    kube-proxy = { most_recent = true }
    vpc-cni    = { most_recent = true }
  }

  eks_managed_node_groups = {
    cpu_nodes = {
      instance_types = var.cpu_instance_types
      min_size       = var.cpu_min_size
      max_size       = var.cpu_max_size
      desired_size   = var.cpu_desired_size
      labels         = { role = "cpu-workload" }
    }

    gpu_nodes = {
      ami_type       = "AL2_x86_64_GPU"
      instance_types = var.gpu_instance_types
      capacity_type  = "SPOT"
      min_size       = var.gpu_min_size
      max_size       = var.gpu_max_size
      desired_size   = var.gpu_min_size

      labels = {
        role                     = "gpu-inference"
        "nvidia.com/gpu.present" = "true"
      }
      taints = [{
        key = "nvidia.com/gpu", value = "true", effect = "NO_SCHEDULE"
      }]
    }
  }

  tags = { Project = var.project_name }
}

resource "helm_release" "nvidia_device_plugin" {
  name       = "nvidia-device-plugin"
  repository = "https://nvidia.github.io/k8s-device-plugin"
  chart      = "nvidia-device-plugin"
  namespace  = "kube-system"
  version    = "0.14.5"

  set { name = "tolerations[0].key";      value = "nvidia.com/gpu" }
  set { name = "tolerations[0].operator"; value = "Exists" }
  set { name = "tolerations[0].effect";   value = "NoSchedule" }
}


###############################################################################
# modules/eks/outputs.tf
###############################################################################

output "cluster_endpoint"        { value = module.eks.cluster_endpoint }
output "cluster_name"            { value = module.eks.cluster_name }
output "cluster_ca_data"         { value = module.eks.cluster_certificate_authority_data }
output "oidc_provider_arn"       { value = module.eks.oidc_provider_arn }
output "cluster_security_group_id" { value = module.eks.cluster_security_group_id }


###############################################################################
# modules/redis/variables.tf
###############################################################################

variable "project_name"          { type = string }
variable "vpc_id"                { type = string }
variable "private_subnet_ids"    { type = list(string) }
variable "eks_security_group_id" { type = string }
variable "node_type"             { type = string; default = "cache.t3.micro" }


###############################################################################
# modules/redis/main.tf
###############################################################################

resource "aws_elasticache_subnet_group" "this" {
  name       = "${var.project_name}-redis"
  subnet_ids = var.private_subnet_ids
}

resource "aws_security_group" "redis" {
  name   = "${var.project_name}-redis-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [var.eks_security_group_id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Project = var.project_name }
}

resource "aws_elasticache_cluster" "this" {
  cluster_id           = "${var.project_name}-redis"
  engine               = "redis"
  engine_version       = "7.1"
  node_type            = var.node_type
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  port                 = 6379
  subnet_group_name    = aws_elasticache_subnet_group.this.name
  security_group_ids   = [aws_security_group.redis.id]
  tags                 = { Project = var.project_name }
}


###############################################################################
# modules/redis/outputs.tf
###############################################################################

output "endpoint" {
  value = "${aws_elasticache_cluster.this.cache_nodes[0].address}:6379"
}


###############################################################################
# modules/kafka/variables.tf
###############################################################################

variable "strimzi_version" { type = string; default = "0.40.0" }
variable "namespace"       { type = string; default = "kafka" }


###############################################################################
# modules/kafka/main.tf
###############################################################################

resource "helm_release" "strimzi" {
  name             = "strimzi"
  repository       = "https://strimzi.io/charts"
  chart            = "strimzi-kafka-operator"
  namespace        = var.namespace
  create_namespace = true
  version          = var.strimzi_version
  wait             = true
}

resource "kubectl_manifest" "kafka_metrics_cm" {
  yaml_body = <<-YAML
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: kafka-metrics
      namespace: ${var.namespace}
    data:
      kafka-metrics-config.yml: |
        lowercaseOutputName: true
        rules:
          - pattern: kafka.server<type=(.+), name=(.+)><>Value
            name: kafka_server_$1_$2
            type: GAUGE
          - pattern: kafka.server<type=(.+), name=(.+)><>Count
            name: kafka_server_$1_$2_total
            type: COUNTER
  YAML
  depends_on = [helm_release.strimzi]
}

resource "kubectl_manifest" "kafka_cluster" {
  yaml_body = <<-YAML
    apiVersion: kafka.strimzi.io/v1beta2
    kind: Kafka
    metadata:
      name: yolo-kafka
      namespace: ${var.namespace}
    spec:
      kafka:
        version: 3.7.0
        replicas: 3
        listeners:
          - name: plain
            port: 9092
            type: internal
            tls: false
        config:
          offsets.topic.replication.factor: 3
          transaction.state.log.replication.factor: 3
          min.insync.replicas: 2
          log.retention.hours: 1
        storage:
          type: jbod
          volumes:
            - id: 0
              type: persistent-claim
              size: 20Gi
              deleteClaim: false
        metricsConfig:
          type: jmxPrometheusExporter
          valueFrom:
            configMapKeyRef:
              name: kafka-metrics
              key: kafka-metrics-config.yml
        template:
          pod:
            affinity:
              nodeAffinity:
                requiredDuringSchedulingIgnoredDuringExecution:
                  nodeSelectorTerms:
                    - matchExpressions:
                        - key: role
                          operator: In
                          values: ["cpu-workload"]
      zookeeper:
        replicas: 3
        storage:
          type: persistent-claim
          size: 10Gi
          deleteClaim: false
      entityOperator:
        topicOperator: {}
  YAML
  depends_on = [helm_release.strimzi]
}

resource "kubectl_manifest" "topic_frames" {
  yaml_body = <<-YAML
    apiVersion: kafka.strimzi.io/v1beta2
    kind: KafkaTopic
    metadata:
      name: frames
      namespace: ${var.namespace}
      labels:
        strimzi.io/cluster: yolo-kafka
    spec:
      partitions: 12
      replicas: 3
      config:
        retention.ms: "60000"
        max.message.bytes: "2097152"
        cleanup.policy: delete
  YAML
  depends_on = [kubectl_manifest.kafka_cluster]
}

resource "kubectl_manifest" "topic_detections" {
  yaml_body = <<-YAML
    apiVersion: kafka.strimzi.io/v1beta2
    kind: KafkaTopic
    metadata:
      name: detections
      namespace: ${var.namespace}
      labels:
        strimzi.io/cluster: yolo-kafka
    spec:
      partitions: 12
      replicas: 3
      config:
        retention.ms: "60000"
        cleanup.policy: delete
  YAML
  depends_on = [kubectl_manifest.kafka_cluster]
}


###############################################################################
# modules/kafka/outputs.tf
###############################################################################

output "bootstrap_servers" {
  value = "yolo-kafka-kafka-bootstrap.${var.namespace}.svc.cluster.local:9092"
}


###############################################################################
# modules/monitoring/variables.tf
###############################################################################

variable "grafana_admin_password" { type = string; sensitive = true }
variable "namespace"              { type = string; default = "monitoring" }


###############################################################################
# modules/monitoring/main.tf
###############################################################################

resource "helm_release" "kube_prometheus" {
  name             = "monitoring"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  namespace        = var.namespace
  create_namespace = true
  version          = "58.0.0"

  values = [<<-YAML
    grafana:
      enabled: true
      adminPassword: "${var.grafana_admin_password}"
      service:
        type: ClusterIP
      sidecar:
        dashboards:
          enabled: true
          searchNamespace: ALL
          label: grafana_dashboard
    prometheus:
      prometheusSpec:
        serviceMonitorSelectorNilUsesHelmValues: false
        podMonitorSelectorNilUsesHelmValues: false
        retention: 15d
        storageSpec:
          volumeClaimTemplate:
            spec:
              accessModes: ["ReadWriteOnce"]
              resources:
                requests:
                  storage: 50Gi
    alertmanager:
      enabled: true
  YAML
  ]
}

resource "helm_release" "dcgm_exporter" {
  name       = "dcgm-exporter"
  repository = "https://nvidia.github.io/dcgm-exporter/helm-charts"
  chart      = "dcgm-exporter"
  namespace  = var.namespace
  version    = "3.3.5"

  set { name = "tolerations[0].key";      value = "nvidia.com/gpu" }
  set { name = "tolerations[0].operator"; value = "Exists" }
  set { name = "tolerations[0].effect";   value = "NoSchedule" }
  set { name = "serviceMonitor.enabled";  value = "true" }
  set { name = "serviceMonitor.interval"; value = "10s" }

  # Collect all GPU metrics including power, PCIe, temperature
  set { name = "arguments[0]"; value = "-f" }
  set { name = "arguments[1]"; value = "/etc/dcgm-exporter/dcp-metrics-included.csv" }

  depends_on = [helm_release.kube_prometheus]
}


###############################################################################
# modules/monitoring/outputs.tf
###############################################################################

output "grafana_service"    { value = "monitoring-grafana" }
output "prometheus_service" { value = "monitoring-kube-prometheus-prometheus" }