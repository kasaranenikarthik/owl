#!/bin/bash
set -euo pipefail
REGION=${1:-us-east-1}
CLUSTER="video-pipeline-cluster"
for SVC in ingest worker aggregator; do
    echo "Redeploying video-pipeline-${SVC}..."
    aws ecs update-service --cluster ${CLUSTER} --service video-pipeline-${SVC} \
        --force-new-deployment --region ${REGION} --no-cli-pager
done
echo "All services redeployed. Wait 1-2 min for tasks to start."
