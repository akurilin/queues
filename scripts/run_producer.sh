#!/usr/bin/env bash
# Convenience wrapper to run the producer with sane defaults from Terraform/.env.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
PRODUCER_DIR="${REPO_ROOT}/producer"
VENV_DIR="${PRODUCER_DIR}/.venv"
PYTHON_BIN="${PYTHON_BIN:-python3}"
ENV_FILE="${REPO_ROOT}/.env"

# Load outputs from .env if present (QUEUE_URL, AWS_REGION, AWS_PROFILE).
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

QUEUE_URL="${QUEUE_URL:-}"
AWS_REGION="${AWS_REGION:-us-west-1}"
AWS_PROFILE="${AWS_PROFILE:-}"
# Normalize empty profile to unset so boto uses default chain.
AWS_PROFILE="$(printf "%s" "$AWS_PROFILE" | xargs)"
if [[ -z "$AWS_PROFILE" ]]; then
  unset AWS_PROFILE
fi

# If queue URL still empty, try Terraform output.
if [[ -z "$QUEUE_URL" ]] && command -v terraform >/dev/null 2>&1; then
  QUEUE_URL="$(terraform -chdir="${REPO_ROOT}/terraform" output -raw queue_url 2>/dev/null || true)"
fi

if [[ -z "$QUEUE_URL" ]]; then
  echo "QUEUE_URL is not set. Populate .env via Terraform or export QUEUE_URL." >&2
  exit 1
fi

export QUEUE_URL AWS_REGION
[[ -n "${AWS_PROFILE:-}" ]] && export AWS_PROFILE

# Ensure virtual env exists with producer deps.
if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  echo "Creating virtualenv for producer in ${VENV_DIR} ..."
  "$PYTHON_BIN" -m venv "${VENV_DIR}"
fi

echo "Installing producer requirements ..."
"${VENV_DIR}/bin/pip" install -q -r "${PRODUCER_DIR}/requirements.txt"

cd "${PRODUCER_DIR}"

"${VENV_DIR}/bin/python" "${PRODUCER_DIR}/produce.py" \
  --queue-url "$QUEUE_URL" \
  --region "$AWS_REGION" \
  ${AWS_PROFILE:+--profile "$AWS_PROFILE"} \
  "$@"
