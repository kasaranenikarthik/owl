output "ingest_repository_url" {
  value = aws_ecr_repository.ingest.repository_url
}

output "worker_repository_url" {
  value = aws_ecr_repository.worker.repository_url
}

output "aggregator_repository_url" {
  value = aws_ecr_repository.aggregator.repository_url
}

# Registry URL (without repo name) — used for docker login
output "registry_url" {
  value = split("/", aws_ecr_repository.ingest.repository_url)[0]
}
