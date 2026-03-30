#!/bin/bash
set -euo pipefail
REGION=${1:-us-east-1}
echo "Scaling ASG to 0 (stopping instance)..."
aws autoscaling update-auto-scaling-group \
    --auto-scaling-group-name video-pipeline-ecs-asg \
    --min-size 0 --max-size 0 --desired-capacity 0 \
    --region ${REGION}
echo "Instance stopped. No compute charges."
