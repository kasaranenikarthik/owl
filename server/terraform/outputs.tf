output "cluster_name"    { value = module.eks.cluster_name }
output "cluster_endpoint" { value = module.eks.cluster_endpoint }
output "redis_endpoint"   { value = module.redis.endpoint }
output "kubeconfig_cmd" {
  value = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}
output "grafana_cmd" {
  value = "kubectl port-forward svc/monitoring-grafana -n monitoring 3000:80"
}
