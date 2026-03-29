variable "name" { type = string }
variable "region" { type = string }

# Networking
variable "subnet_id" { type = string }
variable "security_group_id" { type = string }

# Instance config
variable "key_pair_name" { type = string }
variable "instance_type" { type = string }
variable "iam_instance_profile_name" { type = string }
variable "use_gpu" { type = bool }

# IAM
variable "execution_role_arn" { type = string }
variable "task_role_arn" { type = string }

# ECR images
variable "ingest_image" { type = string }
variable "worker_image" { type = string }
variable "aggregator_image" { type = string }

# Logging
variable "log_group_name" { type = string }
