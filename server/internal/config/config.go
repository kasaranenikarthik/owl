// =============================================================================
// internal/config/config.go
// =============================================================================

package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers    []string
	FramesTopic     string
	DetectionsTopic string
	ConsumerGroup   string

	HTTPPort    string
	MetricsPort string

	RedisAddr           string
	SimilarityThreshold int
	CacheTTLSeconds     int

	ModelPath           string
	ConfidenceThreshold float32
}

func Load() Config {
	return Config{
		KafkaBrokers:        strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		FramesTopic:         env("KAFKA_FRAMES_TOPIC", "frames"),
		DetectionsTopic:     env("KAFKA_DETECTIONS_TOPIC", "detections"),
		ConsumerGroup:       env("KAFKA_CONSUMER_GROUP", "default-group"),
		HTTPPort:            env("HTTP_PORT", "8080"),
		MetricsPort:         env("METRICS_PORT", "9090"),
		RedisAddr:           env("REDIS_ADDR", "localhost:6379"),
		SimilarityThreshold: envInt("SIMILARITY_THRESHOLD", 5),
		CacheTTLSeconds:     envInt("CACHE_TTL_SECONDS", 30),
		ModelPath:           env("MODEL_PATH", "/models/yolov8s-seg.onnx"),
		ConfidenceThreshold: float32(envFloat("CONFIDENCE_THRESHOLD", 0.5)),
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	v, err := strconv.Atoi(os.Getenv(k))
	if err != nil {
		return d
	}
	return v
}

func envFloat(k string, d float64) float64 {
	v, err := strconv.ParseFloat(os.Getenv(k), 64)
	if err != nil {
		return d
	}
	return v
}
