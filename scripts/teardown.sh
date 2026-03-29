#!/bin/bash
set -e

echo "Tearing down all services and volumes..."
docker compose down -v --remove-orphans
echo "Done."
