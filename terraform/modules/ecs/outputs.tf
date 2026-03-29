output "cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "instance_public_ip" {
  value = length(data.aws_instances.ecs.public_ips) > 0 ? data.aws_instances.ecs.public_ips[0] : "PENDING — run 'terraform refresh' after instance is running"
}
