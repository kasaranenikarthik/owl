resource "aws_ecr_repository" "ingest" {
  name         = "${var.name}-ingest"
  force_delete = true
}

resource "aws_ecr_repository" "worker" {
  name         = "${var.name}-worker"
  force_delete = true
}

resource "aws_ecr_repository" "aggregator" {
  name         = "${var.name}-aggregator"
  force_delete = true
}
