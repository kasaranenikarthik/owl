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

  set {
    name  = "tolerations[0].key"
    value = "nvidia.com/gpu"
  }
  set {
    name  = "tolerations[0].operator"
    value = "Exists"
  }
  set {
    name  = "tolerations[0].effect"
    value = "NoSchedule"
  }
}
