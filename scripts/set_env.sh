#!/usr/bin/env bash
# Usage: source ./scripts/set_env.sh

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "Please source this script so it can export environment variables: source $0" >&2
  exit 1
fi

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

export AWS_REGION="${AWS_REGION:-us-west-1}"
export TF_VAR_aws_region="$AWS_REGION"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

if [[ -n "${IMAGE_TAG:-}" ]]; then
  export TF_VAR_container_image_tag="$IMAGE_TAG"
fi

if [[ -n "${AWS_PROFILE:-}" ]]; then
  export AWS_PROFILE
fi

echo "Environment set:"
echo "  AWS_REGION=$AWS_REGION"
[[ -n "${AWS_PROFILE:-}" ]] && echo "  AWS_PROFILE=$AWS_PROFILE"
[[ -n "${TF_VAR_container_image_tag:-}" ]] && echo "  TF_VAR_container_image_tag=$TF_VAR_container_image_tag"
[[ -f "$ENV_FILE" ]] && echo "  Loaded outputs from $ENV_FILE"
