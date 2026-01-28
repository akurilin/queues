# SQS Demo

Minimal-but-complete SQS workflow. Entirely vibe-coded. Terraform creates per-scenario resources:
- SQS standard queue + dead-letter queue (DLQ) for each of 5 scenarios
- Two DynamoDB tables per scenario (`message-status`, `message-completed`) for idempotency and per-message state tracking

Consumers and producers both run locally (no ECS/Fargate/Docker needed).

## Repo layout

| Directory | Purpose |
|-----------|---------|
| `consumer/` | Python SQS consumer with chaos/failure knobs |
| `producer/` | Local Python script to enqueue synthetic messages with batching and rate limiting |
| `scripts/` | Build/push helper (`build_and_push.sh`), producer wrapper (`run_producer.sh`), env loader (`set_env.sh`) |
| `terraform/` | Infrastructure definitions with local state |
| `scenarios/` | 5 self-contained runnable scenarios exercising queue behaviors |
| `plans/` | Planning/design documents |

## Prerequisites
- AWS CLI configured with a profile that can create SQS/DynamoDB resources
- Terraform >= 1.5
- Python 3.11+

## How to use this repo end-to-end

1) **Provision infrastructure** (local Terraform state in `terraform/`)
   ```bash
   make infra-up
   ```
   This writes a `.env` at repo root with `QUEUE_URL`, `DLQ_ARN`, `AWS_REGION`, `MESSAGE_STATUS_TABLE`, and `MESSAGE_COMPLETED_TABLE` (pointing at the happy-path resources for ad-hoc testing).

2) **Run the producer locally to send messages**
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

3) **Run a consumer locally**
   ```bash
   source .env
   cd consumer
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   python consume.py
   ```

4) **Run scenarios** (see [Scenarios](#scenarios) below)
   ```bash
   make scenario-happy
   ```

5) **Tear down**
   ```bash
   make infra-down
   ```

## Architecture

```
Producer (local)  ──▶  SQS Queue  ──▶  Consumer (local)  ──▶  DynamoDB (status + completed)
                          │
                          ▼
                     Dead-Letter Queue (after 2 failed receives)
```

- **Idempotency**: DynamoDB tables track message IDs. The `message-status` table records processing state (STARTED/COMPLETED) with attempt counts. The `message-completed` table provides a separate completion record for cross-consumer deduplication. The consumer also maintains an in-memory LRU cache for fast local duplicate detection.
- **Long-polling**: Consumer uses configurable wait time (default 10s) to reduce API calls.
- **Batching**: Producer batches up to 10 messages per SQS API call.

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
| `REJECT_PAYLOAD_MARKER` | *(empty)* | Reject messages whose payload contains this marker (for poison message testing) |
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
- `--poison-count` — number of poison messages to inject (default 0)
- `--queue-url` — override queue URL (defaults to `QUEUE_URL` env var)
- `--region` — AWS region (defaults to `AWS_REGION` env var)
- `--profile` — AWS profile (defaults to `AWS_PROFILE` env var)

Each message contains a JSON payload with a UUID `id` and a random `work` value (1–1000). Poison messages additionally carry `"poison": true`.

## Failure-mode experiments
- **Visibility overrun**: `LONG_SLEEP_SECONDS=10 LONG_SLEEP_EVERY=2` — expect duplicates as messages become visible again before processing completes
- **Crashy worker**: `CRASH_RATE=0.2` — consumer exits intermittently; messages redeliver from the queue
- **Deterministic crash**: `CRASH_AFTER_RECEIVE=1` — crashes after processing but before delete, useful for testing redelivery
- **Burst load**: `./scripts/run_producer.sh --n 1000 --rate 0` — observe SQS metrics and processing throughput

## Environment config
- Terraform writes a `.env` at repo root with `QUEUE_URL`, `DLQ_ARN`, `AWS_REGION`, `MESSAGE_STATUS_TABLE`, and `MESSAGE_COMPLETED_TABLE` (using the happy-path scenario resources).
- If you prefer manual setup, copy `.env.example` to `.env` and fill in your account-specific values; scripts/loaders will pick it up.

## Terraform variables

| Variable | Default | Description |
|----------|---------|-------------|
| `aws_region` | `us-west-1` | AWS region for all resources |
| `project_name` | `sqs-demo` | Base name for created resources |
| `queue_visibility_timeout` | `10` | SQS visibility timeout (seconds) |
| `queue_receive_wait` | `10` | SQS long-poll wait time (seconds) |

## Scenarios
`scenarios/` contains self-contained setups to exercise typical queue behaviors. Each scenario uses its own isolated SQS queue + DLQ and DynamoDB tables, all provisioned by the shared `terraform/` configuration. Consumers and producers run as local Python subprocesses.

**Fast** (finish in seconds):
- **Happy path** (`make scenario-happy`): messages flow through cleanly, all processed and deleted.
- **Crash recovery** (`make scenario-crash`): consumer crashes after receiving; observe redelivery and recovery.
- **Duplicate delivery** (`make scenario-duplicates`): slow processing triggers visibility timeout, causing redelivery; DynamoDB-backed idempotency prevents duplicate side effects.
- **Poison messages** (`make scenario-poison`): messages with a poison marker are rejected and redrive to the DLQ after max retries.

**Slow** (5–10 minutes):
- **Backpressure / auto-scaling** (`make scenario-backpressure`): a continuous producer floods the queue; the runner monitors queue depth and spawns additional consumers until equilibrium.

### Running scenarios
- **Prereqs**: infrastructure provisioned (`make infra-up`), AWS CLI + Terraform on PATH.
- **Quick start** (venv is created automatically):
  ```bash
  make scenarios-fast        # run the 4 fast scenarios
  make scenarios-slow        # run slow scenarios (backpressure)
  make scenarios             # run everything (fast then slow)
  ```
- **Individual scenarios**:
  ```bash
  make scenario-happy
  make scenario-crash
  ```
- Pass extra arguments via `ARGS`:
  ```bash
  make scenario-happy ARGS="--count 10 --batch-size 5"
  ```

### Validating infrastructure
```bash
make validate
```

## Makefile targets

Run `make help` to see all targets. Key ones:

| Target | Purpose |
|--------|---------|
| `make preflight` | Check local environment for required tools |
| `make infra-up` | Provision infrastructure |
| `make infra-down` | Destroy infrastructure |
| `make venv` | Create/update the project venv |
| `make validate` | Validate scenario infrastructure is healthy |
| `make scenario-*` | Run individual scenarios |
| `make scenarios-fast` | Run the 4 fast scenarios |
| `make scenarios-slow` | Run slow scenarios (backpressure) |
| `make scenarios` | Run all scenarios (fast then slow) |

## Tear down
```bash
make infra-down
```
