#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

REGION="${AWS_REGION:-us-west-1}"
PROFILE="${AWS_PROFILE:-}"
PLATFORM="${PLATFORM:-linux/amd64}"

IMAGE_TAG="${IMAGE_TAG:-}"
# Default to git SHA (with -dirty timestamp suffix if needed) when no tag provided.
if [[ -z "$IMAGE_TAG" ]] && command -v git >/dev/null 2>&1 && git -C "$REPO_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  GIT_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
  if git -C "$REPO_ROOT" status --porcelain | grep -q .; then
    IMAGE_TAG="${GIT_SHA}-dirty-$(date +%Y%m%d%H%M%S)"
  else
    IMAGE_TAG="$GIT_SHA"
  fi
fi
# Final fallback if git is unavailable.
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
if [[ ${#AWS_ARGS[@]} -gt 0 ]]; then
  aws "${AWS_ARGS[@]}" ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REPO_URL"
else
  aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REPO_URL"
fi

echo "Building consumer image: ${REPO_URL}:${IMAGE_TAG} (platform: ${PLATFORM})"
docker build --platform "${PLATFORM}" -t "${REPO_URL}:${IMAGE_TAG}" "${REPO_ROOT}/consumer"

echo "Pushing image ..."
docker push "${REPO_URL}:${IMAGE_TAG}"

echo "Done"
