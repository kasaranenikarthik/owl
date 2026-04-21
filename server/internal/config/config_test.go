package config

import (
	"reflect"
	"testing"
)

func TestLoadUsesCurrentEnvNames(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "kafka:9092,kafka:9093")
	t.Setenv("KAFKA_BROKER", "localhost:9092")
	t.Setenv("KAFKA_FRAMES_TOPIC", "frames-new")
	t.Setenv("FRAMES_TOPIC", "frames-old")
	t.Setenv("KAFKA_DETECTIONS_TOPIC", "detections-new")
	t.Setenv("DETECTIONS_TOPIC", "detections-old")
	t.Setenv("METRICS_PORT", "19090")
	t.Setenv("PROMETHEUS_PORT", "9090")
	t.Setenv("MODEL_PATH", "/models/current.onnx")
	t.Setenv("YOLO_MODEL_PATH", "/models/legacy.onnx")

	cfg := Load()

	if got, want := cfg.KafkaBrokers, []string{"kafka:9092", "kafka:9093"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("KafkaBrokers = %v, want %v", got, want)
	}
	if got, want := cfg.FramesTopic, "frames-new"; got != want {
		t.Fatalf("FramesTopic = %q, want %q", got, want)
	}
	if got, want := cfg.DetectionsTopic, "detections-new"; got != want {
		t.Fatalf("DetectionsTopic = %q, want %q", got, want)
	}
	if got, want := cfg.MetricsPort, "19090"; got != want {
		t.Fatalf("MetricsPort = %q, want %q", got, want)
	}
	if got, want := cfg.ModelPath, "/models/current.onnx"; got != want {
		t.Fatalf("ModelPath = %q, want %q", got, want)
	}
}

func TestLoadUsesLegacyEnvNamesAsFallback(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_BROKER", "kafka:9092")
	t.Setenv("KAFKA_FRAMES_TOPIC", "")
	t.Setenv("FRAMES_TOPIC", "video.frames")
	t.Setenv("KAFKA_DETECTIONS_TOPIC", "")
	t.Setenv("DETECTIONS_TOPIC", "video.detections")
	t.Setenv("METRICS_PORT", "")
	t.Setenv("PROMETHEUS_PORT", "9090")
	t.Setenv("MODEL_PATH", "")
	t.Setenv("YOLO_MODEL_PATH", "/models/yolov8n.pt")

	cfg := Load()

	if got, want := cfg.KafkaBrokers, []string{"kafka:9092"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("KafkaBrokers = %v, want %v", got, want)
	}
	if got, want := cfg.FramesTopic, "video.frames"; got != want {
		t.Fatalf("FramesTopic = %q, want %q", got, want)
	}
	if got, want := cfg.DetectionsTopic, "video.detections"; got != want {
		t.Fatalf("DetectionsTopic = %q, want %q", got, want)
	}
	if got, want := cfg.MetricsPort, "9090"; got != want {
		t.Fatalf("MetricsPort = %q, want %q", got, want)
	}
	if got, want := cfg.ModelPath, "/models/yolov8n.pt"; got != want {
		t.Fatalf("ModelPath = %q, want %q", got, want)
	}
}
