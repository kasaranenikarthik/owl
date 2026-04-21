resource "helm_release" "kube_prometheus" {
  name             = "monitoring"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  namespace        = var.namespace
  create_namespace = true
  version          = "58.0.0"

  values = [<<-YAML
    grafana:
      enabled: true
      adminPassword: "${var.grafana_admin_password}"
      service:
        type: ClusterIP
      sidecar:
        dashboards:
          enabled: true
          searchNamespace: ALL
          label: grafana_dashboard
    prometheus:
      prometheusSpec:
        serviceMonitorSelectorNilUsesHelmValues: false
        podMonitorSelectorNilUsesHelmValues: false
        retention: 15d
        storageSpec:
          volumeClaimTemplate:
            spec:
              accessModes: ["ReadWriteOnce"]
              resources:
                requests:
                  storage: 50Gi
    alertmanager:
      enabled: true
  YAML
  ]
}

resource "helm_release" "dcgm_exporter" {
  name       = "dcgm-exporter"
  repository = "https://nvidia.github.io/dcgm-exporter/helm-charts"
  chart      = "dcgm-exporter"
  namespace  = var.namespace
  version    = "3.3.5"

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
  set {
    name  = "serviceMonitor.enabled"
    value = "true"
  }
  set {
    name  = "serviceMonitor.interval"
    value = "10s"
  }

  # Collect all GPU metrics including power, PCIe, temperature
  set {
    name  = "arguments[0]"
    value = "-f"
  }
  set {
    name  = "arguments[1]"
    value = "/etc/dcgm-exporter/dcp-metrics-included.csv"
  }

  depends_on = [helm_release.kube_prometheus]
}
