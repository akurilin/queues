#!/usr/bin/env bash
# One-shot runner for the SQS E2E pytest. Creates/uses .venv locally.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements-dev.txt

pytest test_e2e.py -s "$@"
