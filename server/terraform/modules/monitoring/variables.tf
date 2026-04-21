variable "grafana_admin_password" {
  type      = string
  sensitive = true
}

variable "namespace" {
  type    = string
  default = "monitoring"
}
