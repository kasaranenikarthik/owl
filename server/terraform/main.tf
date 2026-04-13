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
  cpu_min_size       = 2
  cpu_max_size       = 6
  cpu_desired_size   = 3
}

module "redis" {
  source                = "./modules/redis"
  project_name          = var.project_name
  vpc_id                = module.vpc.vpc_id
  private_subnet_ids    = module.vpc.private_subnet_ids
  eks_security_group_id = module.eks.cluster_security_group_id
  node_type             = "cache.t3.micro"
}

module "kafka" {
  source           = "./modules/kafka"
  strimzi_version  = "0.40.0"
  namespace        = "kafka"
  depends_on       = [module.eks]
}

module "monitoring" {
  source                 = "./modules/monitoring"
  grafana_admin_password = var.grafana_admin_password
  namespace              = "monitoring"
  depends_on             = [module.eks]
}
