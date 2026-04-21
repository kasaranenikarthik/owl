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
