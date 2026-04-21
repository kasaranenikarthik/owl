variable "project_name" {
  type = string
}

variable "cluster_version" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "cpu_instance_types" {
  type = list(string)
}

variable "gpu_instance_types" {
  type = list(string)
}

variable "gpu_min_size" {
  type = number
}

variable "gpu_max_size" {
  type = number
}

variable "cpu_min_size" {
  type    = number
  default = 2
}

variable "cpu_max_size" {
  type    = number
  default = 6
}

variable "cpu_desired_size" {
  type    = number
  default = 3
}
