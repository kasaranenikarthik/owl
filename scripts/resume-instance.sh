#!/bin/bash
set -euo pipefail
REGION=${1:-us-east-1}
echo "Scaling ASG to 1 (starting instance)..."
aws autoscaling update-auto-scaling-group \
    --auto-scaling-group-name video-pipeline-ecs-asg \
    --min-size 1 --max-size 1 --desired-capacity 1 \
    --region ${REGION}
echo "Wait 2-3 min, then: cd terraform && terraform refresh && terraform output"
