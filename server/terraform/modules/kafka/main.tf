resource "helm_release" "strimzi" {
  name             = "strimzi"
  repository       = "https://strimzi.io/charts"
  chart            = "strimzi-kafka-operator"
  namespace        = var.namespace
  create_namespace = true
  version          = var.strimzi_version
  wait             = true
}

resource "kubectl_manifest" "kafka_metrics_cm" {
  yaml_body = <<-YAML
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: kafka-metrics
      namespace: ${var.namespace}
    data:
      kafka-metrics-config.yml: |
        lowercaseOutputName: true
        rules:
          - pattern: kafka.server<type=(.+), name=(.+)><>Value
            name: kafka_server_$1_$2
            type: GAUGE
          - pattern: kafka.server<type=(.+), name=(.+)><>Count
            name: kafka_server_$1_$2_total
            type: COUNTER
  YAML
  depends_on = [helm_release.strimzi]
}

resource "kubectl_manifest" "kafka_cluster" {
  yaml_body = <<-YAML
    apiVersion: kafka.strimzi.io/v1beta2
    kind: Kafka
    metadata:
      name: yolo-kafka
      namespace: ${var.namespace}
    spec:
      kafka:
        version: 3.7.0
        replicas: 3
        listeners:
          - name: plain
            port: 9092
            type: internal
            tls: false
        config:
          offsets.topic.replication.factor: 3
          transaction.state.log.replication.factor: 3
          min.insync.replicas: 2
          log.retention.hours: 1
        storage:
          type: jbod
          volumes:
            - id: 0
              type: persistent-claim
              size: 20Gi
              deleteClaim: false
        metricsConfig:
          type: jmxPrometheusExporter
          valueFrom:
            configMapKeyRef:
              name: kafka-metrics
              key: kafka-metrics-config.yml
        template:
          pod:
            affinity:
              nodeAffinity:
                requiredDuringSchedulingIgnoredDuringExecution:
                  nodeSelectorTerms:
                    - matchExpressions:
                        - key: role
                          operator: In
                          values: ["cpu-workload"]
      zookeeper:
        replicas: 3
        storage:
          type: persistent-claim
          size: 10Gi
          deleteClaim: false
      entityOperator:
        topicOperator: {}
  YAML
  depends_on = [helm_release.strimzi]
}

resource "kubectl_manifest" "topic_frames" {
  yaml_body = <<-YAML
    apiVersion: kafka.strimzi.io/v1beta2
    kind: KafkaTopic
    metadata:
      name: frames
      namespace: ${var.namespace}
      labels:
        strimzi.io/cluster: yolo-kafka
    spec:
      partitions: 12
      replicas: 3
      config:
        retention.ms: "60000"
        max.message.bytes: "2097152"
        cleanup.policy: delete
  YAML
  depends_on = [kubectl_manifest.kafka_cluster]
}

resource "kubectl_manifest" "topic_detections" {
  yaml_body = <<-YAML
    apiVersion: kafka.strimzi.io/v1beta2
    kind: KafkaTopic
    metadata:
      name: detections
      namespace: ${var.namespace}
      labels:
        strimzi.io/cluster: yolo-kafka
    spec:
      partitions: 12
      replicas: 3
      config:
        retention.ms: "60000"
        cleanup.policy: delete
  YAML
  depends_on = [kubectl_manifest.kafka_cluster]
}
