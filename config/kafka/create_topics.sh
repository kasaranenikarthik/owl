#!/bin/bash
set -e

BROKER="kafka:29092"

echo "Waiting for Kafka to be ready..."
until kafka-topics.sh --bootstrap-server "$BROKER" --list > /dev/null 2>&1; do
  sleep 2
done
echo "Kafka is ready"

echo "Creating topics..."
kafka-topics.sh --bootstrap-server "$BROKER" --create --if-not-exists \
  --topic video.frames \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=300000 \
  --config max.message.bytes=10485760

kafka-topics.sh --bootstrap-server "$BROKER" --create --if-not-exists \
  --topic video.detections \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=600000

echo "Topics created:"
kafka-topics.sh --bootstrap-server "$BROKER" --list
