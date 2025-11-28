#!/usr/bin/env bash
set -euo pipefail

REGION="${AWS_REGION:-us-west-1}"
PROFILE="${AWS_PROFILE:-}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

if [[ -n "$PROFILE" ]]; then
  AWS_ARGS=(--profile "$PROFILE")
else
  AWS_ARGS=()
fi

REPO_URL="${ECR_REPO:-}"
if [[ -z "$REPO_URL" ]]; then
  if command -v terraform >/dev/null 2>&1; then
    REPO_URL="$(terraform -chdir="$(dirname "$0")/../terraform" output -raw ecr_repository_url 2>/dev/null || true)"
  fi
fi

if [[ -z "$REPO_URL" ]]; then
  echo "ECR repository URL not set. Export ECR_REPO or run terraform output." >&2
  exit 1
fi

echo "Logging in to ECR ($REGION) ..."
aws "${AWS_ARGS[@]}" ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REPO_URL"

echo "Building consumer image: ${REPO_URL}:${IMAGE_TAG}"
docker build -t "${REPO_URL}:${IMAGE_TAG}" "$(dirname "$0")/../consumer"

echo "Pushing image ..."
docker push "${REPO_URL}:${IMAGE_TAG}"

echo "Done"
