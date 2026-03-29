# ── IAM (LabRole — same pattern as your HW7) ──
data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

# ── Networking ──
module "vpc" {
  source = "./modules/vpc"
  name   = var.project_name
}

# ── Logging ──
module "logging" {
  source = "./modules/logging"
  name   = var.project_name
}

# ── Container Registry ──
module "ecr" {
  source = "./modules/ecr"
  name   = var.project_name
}

# ── ECS Cluster + GPU Instance + Task Definitions + Services ──
module "ecs" {
  source = "./modules/ecs"
  name   = var.project_name
  region = var.aws_region

  # Networking
  subnet_id         = module.vpc.public_subnet_ids[0]
  security_group_id = module.vpc.ecs_security_group_id

  # Instance config
  key_pair_name             = var.key_pair_name
  instance_type             = var.instance_type
  iam_instance_profile_name = var.iam_instance_profile_name
  use_gpu                   = var.use_gpu

  # IAM (LabRole)
  execution_role_arn = data.aws_iam_role.lab_role.arn
  task_role_arn      = data.aws_iam_role.lab_role.arn

  # ECR image URIs
  ingest_image     = "${module.ecr.ingest_repository_url}:latest"
  worker_image     = "${module.ecr.worker_repository_url}:latest"
  aggregator_image = "${module.ecr.aggregator_repository_url}:latest"

  # Logging
  log_group_name = module.logging.log_group_name
}
