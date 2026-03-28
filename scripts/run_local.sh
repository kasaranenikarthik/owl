#!/bin/bash
set -e

echo "Building and starting all services..."
docker compose up --build -d

echo "Waiting for services to be healthy..."
sleep 10

echo "Service status:"
docker compose ps

echo ""
echo "Endpoints:"
echo "  Aggregator API:  http://localhost:8080"
echo "  Aggregator WS:   ws://localhost:8080/ws/{stream_id}"
echo "  Prometheus:       http://localhost:9090"
echo "  Grafana:          http://localhost:3000 (admin/admin)"
echo ""
echo "Tailing logs (Ctrl+C to stop)..."
docker compose logs -f
