output "ec2_instance_public_ip" {
  description = "SSH into this IP to build and push Docker images"
  value       = module.ecs.instance_public_ip
}

output "ecs_cluster_name" {
  value = module.ecs.cluster_name
}

output "ecr_ingest_url" {
  description = "ECR repo URL for ingest service"
  value       = module.ecr.ingest_repository_url
}

output "ecr_worker_url" {
  description = "ECR repo URL for inference worker"
  value       = module.ecr.worker_repository_url
}

output "ecr_aggregator_url" {
  description = "ECR repo URL for aggregator"
  value       = module.ecr.aggregator_repository_url
}

output "aggregator_url" {
  description = "Aggregator REST API (available after services start)"
  value       = "http://${module.ecs.instance_public_ip}:8080"
}

output "ssh_command" {
  description = "Run this to SSH into the EC2 instance"
  value       = "ssh -i ~/.ssh/${var.key_pair_name}.pem ec2-user@${module.ecs.instance_public_ip}"
}

output "ecr_login_command" {
  description = "Run this ON the EC2 instance to authenticate Docker to ECR"
  value       = "aws ecr get-login-password --region ${var.aws_region} | docker login --username AWS --password-stdin ${module.ecr.registry_url}"
}
