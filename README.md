# SQS Demo

Minimal-but-complete SQS workflow. Entirely vibe-coded. Terraform creates:
- VPC (public subnets only, no NAT gateway)
- SQS standard queue + dead-letter queue (DLQ)
- ECR repo for the consumer image
- ECS Fargate cluster/service/task for the consumer
- CloudWatch log group
- Two DynamoDB tables (`message-status`, `message-completed`) for idempotency and per-message state tracking
- IAM roles (execution role for ECR pull + CloudWatch, task role for SQS + DynamoDB)

## Repo layout

| Directory | Purpose |
|-----------|---------|
| `consumer/` | Python SQS consumer (Dockerized, Python 3.14-slim) with chaos/failure knobs |
| `producer/` | Local Python script to enqueue synthetic messages with batching and rate limiting |
| `scripts/` | Build/push helper (`build_and_push.sh`), producer wrapper (`run_producer.sh`), env loader (`set_env.sh`) |
| `terraform/` | Infrastructure definitions with local state |
| `scenarios/` | Self-contained runnable scenarios: happy path, crash recovery, duplicate processing |
| `plans/` | Planning/design documents |

## Prerequisites
- AWS CLI configured with a profile that can create VPC/ECR/ECS/SQS/DynamoDB resources
- Terraform >= 1.5
- Docker (can build for amd64; script defaults to `--platform linux/amd64`)
- Python 3.11+ for the producer and scenario scripts

## How to use this repo end-to-end

1) **(Optional) load env helpers**
   `source ./scripts/set_env.sh`

2) **Provision infrastructure** (local Terraform state in `terraform/`)
   ```bash
   terraform -chdir=terraform init
   terraform -chdir=terraform apply -auto-approve
   ```
   This writes a `.env` at repo root with `QUEUE_URL`, `DLQ_ARN`, `ECR_REPO`, `AWS_REGION`, `MESSAGE_STATUS_TABLE`, and `MESSAGE_COMPLETED_TABLE`.

3) **Build and push the consumer image**
   ```bash
   ./scripts/build_and_push.sh
   ```
   - Defaults `IMAGE_TAG` to the current git SHA (appends `-dirty-<timestamp>` if the tree is dirty). Override by exporting `IMAGE_TAG=...` if you want a custom tag.
   - Uses `ECR_REPO` from `.env` or Terraform output.
   - Builds for amd64 by default (`PLATFORM=linux/amd64`), so Fargate can run it.

4) **Point ECS at the pushed tag (if not `latest`)**
   ```bash
   terraform -chdir=terraform apply -auto-approve -var container_image_tag=${IMAGE_TAG:-$(git rev-parse --short HEAD)}
   ```

5) **Run the producer locally to send messages**
   ```bash
   ./scripts/run_producer.sh --n 25 --rate 5
   ```
   The wrapper auto-loads `.env`/Terraform outputs for `QUEUE_URL`/`AWS_REGION`/`AWS_PROFILE`, creates a venv in `producer/.venv`, installs deps, and runs `produce.py`.

   Or manually:
   ```bash
   cd producer
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   QUEUE_URL=$(terraform -chdir=../terraform output -raw queue_url)
   python produce.py --n 25 --rate 5 --queue-url "$QUEUE_URL"
   ```

6) **Watch the consumer**
   - Logs: `aws logs tail /aws/ecs/sqs-demo-consumer --follow`
   - ECS service: `aws ecs describe-services --cluster sqs-demo-cluster --services sqs-demo-service`
   - Scale: `terraform -chdir=terraform apply -auto-approve -var desired_count=3`

7) **Tear down**
   ```bash
   terraform -chdir=terraform destroy -auto-approve
   ```

## Architecture

```
Producer (local)  ──▶  SQS Queue  ──▶  Consumer (ECS Fargate)  ──▶  DynamoDB (status + completed)
                          │
                          ▼
                     Dead-Letter Queue (after 5 failed receives)
```

- **Idempotency**: DynamoDB tables track message IDs. The `message-status` table records processing state (STARTED/COMPLETED) with attempt counts. The `message-completed` table provides a separate completion record for cross-consumer deduplication. The consumer also maintains an in-memory LRU cache for fast local duplicate detection.
- **Long-polling**: Consumer uses configurable wait time (default 10s) to reduce API calls.
- **Batching**: Producer batches up to 10 messages per SQS API call.
- **Scalability**: ECS service can scale horizontally (configurable `desired_count`, default 1).

## What the consumer does
- Long-polls SQS, parses JSON payloads, tracks state in DynamoDB, deletes messages after processing.
- Extracts a custom `id` field from the message payload if present, otherwise falls back to the SQS `MessageId`.
- Handles stale receipt handles gracefully (logs a warning instead of crashing).

## Consumer env vars

| Variable | Default | Description |
|----------|---------|-------------|
| `QUEUE_URL` | *(required)* | SQS queue URL to poll |
| `AWS_REGION` | SDK default | Region override |
| `WAIT_TIME_SECONDS` | `10` | Long-poll wait (seconds) |
| `MAX_MESSAGES` | `1` | Messages per receive call (capped at 10) |
| `SLEEP_MIN_SECONDS` | `0.1` | Minimum simulated work duration |
| `SLEEP_MAX_SECONDS` | `1.0` | Maximum simulated work duration |
| `LONG_SLEEP_SECONDS` | `0.0` | Long sleep duration for visibility overrun simulation (0 = disabled) |
| `LONG_SLEEP_EVERY` | `0` | Apply long sleep every Nth message (0 = disabled) |
| `CRASH_RATE` | `0.0` | Probability (0–1) of random crash per message |
| `CRASH_AFTER_RECEIVE` | `0` | If non-zero, crash deterministically after processing but before deleting |
| `IDEMPOTENCY_CACHE_SIZE` | `1000` | In-memory LRU cache size for duplicate detection (0 = disabled) |
| `MESSAGE_LIMIT` | `0` | Stop after processing N messages (0 = unlimited) |
| `IDLE_TIMEOUT_SECONDS` | `0` | Exit if no messages arrive for this many seconds (0 = disabled) |
| `MESSAGE_STATUS_TABLE` | *(optional)* | DynamoDB table name for status tracking |
| `MESSAGE_COMPLETED_TABLE` | *(optional)* | DynamoDB table name for completion idempotency |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

## Producer usage
```bash
./scripts/run_producer.sh --n 100 --rate 20
```
- `--n` — number of messages to send (default 10)
- `--rate` — messages/sec limit (`0` = as fast as possible)
- `--batch-size` — messages per SQS batch call (max 10, default 10)
- `--queue-url` — override queue URL (defaults to `QUEUE_URL` env var)
- `--region` — AWS region (defaults to `AWS_REGION` env var)
- `--profile` — AWS profile (defaults to `AWS_PROFILE` env var)

Each message contains a JSON payload with a UUID `id` and a random `work` value (1–1000).

## Failure-mode experiments
- **Visibility overrun**: `LONG_SLEEP_SECONDS=10 LONG_SLEEP_EVERY=2` — expect duplicates as messages become visible again before processing completes
- **Crashy worker**: `CRASH_RATE=0.2` — container exits intermittently; ECS restarts the task, messages redeliver
- **Deterministic crash**: `CRASH_AFTER_RECEIVE=1` — crashes after processing but before delete, useful for testing redelivery
- **Scale out**: `terraform -chdir=terraform apply -auto-approve -var desired_count=3` — watch backlog drain across multiple consumers
- **Burst load**: `./scripts/run_producer.sh --n 1000 --rate 0` — observe SQS metrics and processing throughput
- **Kill task mid-run**: terminate the ECS task from the console while processing — message should reappear (not yet deleted)

## Environment config
- Terraform writes a `.env` at repo root with `QUEUE_URL`, `DLQ_ARN`, `ECR_REPO`, `AWS_REGION`, `MESSAGE_STATUS_TABLE`, and `MESSAGE_COMPLETED_TABLE`. Optional: `AWS_PROFILE`, `IMAGE_TAG`.
- If you prefer manual setup, copy `.env.example` to `.env` and fill in your account-specific values; scripts/loaders will pick it up.

## Terraform variables

| Variable | Default | Description |
|----------|---------|-------------|
| `aws_region` | `us-west-1` | AWS region for all resources |
| `project_name` | `sqs-demo` | Base name for created resources |
| `vpc_cidr` | `10.0.0.0/20` | CIDR block for the demo VPC |
| `desired_count` | `1` | Number of consumer ECS tasks |
| `container_cpu` | `256` | CPU units for the consumer task |
| `container_memory` | `512` | Memory (MB) for the consumer task |
| `container_image_tag` | `latest` | Image tag to deploy |
| `queue_visibility_timeout` | `10` | SQS visibility timeout (seconds) |
| `queue_receive_wait` | `10` | SQS long-poll wait time (seconds) |
| `log_retention_days` | `14` | CloudWatch log retention (days) |

## Scenarios
`scenarios/` contains self-contained setups to exercise typical queue behaviors. Each scenario provisions its own isolated infrastructure (SQS queue + DLQ, DynamoDB tables, ECS cluster + task definition, task IAM role) via `scenarios/terraform/`, runs the test, and tears everything down afterwards. Shared resources (VPC, execution role, CloudWatch, ECR) come from the main terraform.

- **Happy path**: messages flow through cleanly, all processed and deleted.
- **Crashed consumer**: observe redelivery and recovery after worker crashes.
- **Duplicate message processing**: see how DynamoDB-backed idempotency handles redelivered messages.

### Running scenarios
- **Prereqs**: main infra provisioned (`terraform -chdir=terraform apply`), consumer image pushed to ECR, AWS CLI + Terraform on PATH.
- **One-time setup**:
  ```bash
  python -m venv scenarios/.venv && source scenarios/.venv/bin/activate
  pip install -r scenarios/requirements.txt
  ```
- **Run from repo root** with the venv active:
  ```bash
  python scenarios/run.py happy --count 5 --batch-size 5
  python scenarios/run.py crash --visibility-wait 15
  python scenarios/run.py duplicates --slow-seconds 30 --second-start-delay 0
  ```
- Use `--image-tag <tag>` to specify a container image tag (default: `latest`).
- Each run creates a Terraform workspace, provisions per-scenario resources, validates them, runs the scenario, then destroys everything.

### Validating infrastructure
You can validate infrastructure independently:
```bash
# Validate shared (main) infrastructure
python scenarios/validate_infra.py --main

# Validate a specific scenario workspace
python scenarios/validate_infra.py --workspace happy-a1b2c3d4
```

## Tear down
```bash
terraform -chdir=terraform destroy -auto-approve
```

## WIP
- Moving testing scenarios to a local environment would be a real time saver, although would lose true e2e test value
- A few more scenarios to implement
