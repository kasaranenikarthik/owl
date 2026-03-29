#!/bin/bash
# ═══════════════════════════════════════════════════════
# build-and-push.sh — Run this ON the EC2 instance
#
# Builds all 3 Docker images and pushes them to ECR.
# After pushing, ECS will automatically pull the new images
# on the next task restart.
#
# Usage:
#   ./scripts/build-and-push.sh <AWS_ACCOUNT_ID> <REGION>
#
# Example:
#   ./scripts/build-and-push.sh 123456789012 us-east-1
# ═══════════════════════════════════════════════════════
set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <AWS_ACCOUNT_ID> <REGION>"
    echo "  Find your account ID: aws sts get-caller-identity --query Account --output text"
    exit 1
fi

ACCOUNT_ID=$1
REGION=$2
PROJECT="video-pipeline"
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

echo "═══════════════════════════════════════"
echo " Registry: ${REGISTRY}"
echo " Region:   ${REGION}"
echo "═══════════════════════════════════════"

# Step 1: Authenticate Docker to ECR
echo ""
echo "▶ Logging into ECR..."
aws ecr get-login-password --region ${REGION} | \
    docker login --username AWS --password-stdin ${REGISTRY}

# Step 2: Build and push each service
for SERVICE in ingest worker aggregator; do
    IMAGE="${REGISTRY}/${PROJECT}-${SERVICE}:latest"
    CONTEXT="src/"

    case ${SERVICE} in
        ingest)     CONTEXT="src/ingest-service" ;;
        worker)     CONTEXT="src/inference-worker" ;;
        aggregator) CONTEXT="src/aggregator" ;;
    esac

    echo ""
    echo "▶ Building ${SERVICE}..."
    docker build -t ${IMAGE} ${CONTEXT}

    echo "▶ Pushing ${SERVICE} to ECR..."
    docker push ${IMAGE}

    echo "✓ ${SERVICE} pushed: ${IMAGE}"
done

echo ""
echo "═══════════════════════════════════════"
echo " All images pushed to ECR!"
echo ""
echo " Next: ECS will pull these automatically."
echo " Force redeploy with:"
echo "   aws ecs update-service --cluster ${PROJECT}-cluster --service ${PROJECT}-ingest --force-new-deployment --region ${REGION}"
echo "   aws ecs update-service --cluster ${PROJECT}-cluster --service ${PROJECT}-worker --force-new-deployment --region ${REGION}"
echo "   aws ecs update-service --cluster ${PROJECT}-cluster --service ${PROJECT}-aggregator --force-new-deployment --region ${REGION}"
echo "═══════════════════════════════════════"
