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
| `USE_TRANSACTIONAL_WRITES` | `0` | Use DynamoDB transactions for atomic side effect + idempotency writes |
| `CRASH_AFTER_SIDE_EFFECT` | `0` | Crash after DynamoDB transaction but before SQS delete (for testing) |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

## Producer usage
```bash
./scripts/run_producer.sh --n 100 --rate 20
```
- `--n` — number of messages to send (default 10)
- `--rate` — messages/sec limit (`0` = as fast as possible)
- `--batch-size` — messages per SQS batch call (max 10, default 10)
- `--poison-count` — number of poison messages to inject at random indices (default 0)
- `--poison-every N` — make every Nth message poison (e.g., 3 means indices 2, 5, 8...); overrides `--poison-count`
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
- **Partial batch failure** (`make scenario-partial-batch`): consumer receives batches of 10 messages containing both good and poison messages; good messages are processed and deleted individually while poison messages are retried and eventually land in the DLQ.
- **Transactional side effects** (`make scenario-side-effects`): consumer uses DynamoDB transactions to atomically write the side effect and idempotency record; proves no duplicate side effects occur even when crashing after the transaction but before SQS delete.

**Slow** (5–10 minutes):
- **Backpressure / auto-scaling** (`make scenario-backpressure`): a continuous producer floods the queue; the runner monitors queue depth and spawns additional consumers until equilibrium.

### Running scenarios
- **Prereqs**: infrastructure provisioned (`make infra-up`), AWS CLI + Terraform on PATH.
- **Quick start** (venv is created automatically):
  ```bash
  make scenarios-fast        # run the 6 fast scenarios
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
| `make scenarios-fast` | Run the 6 fast scenarios |
| `make scenarios-slow` | Run slow scenarios (backpressure) |
| `make scenarios` | Run all scenarios (fast then slow) |

## Tear down
```bash
make infra-down
```

---

## Design Considerations for Queue-Based Systems

This section documents deeper design considerations that emerged from building and testing these scenarios.

### The Two-Table Idempotency Pattern

This codebase uses two DynamoDB tables for idempotency, each with a distinct role:

| Table | Role | Key insight |
|-------|------|-------------|
| `message_status` | **The side effect** — tracks processing lifecycle (STARTED → COMPLETED) | Updating this to COMPLETED *is* the durable business outcome we care about |
| `message_completed` | **The idempotency gate** — written with `attribute_not_exists` condition | If this write fails, we know the message was already processed |

**Why two tables instead of one?**

You could use a single table, but the two-table pattern provides:
1. **Separation of concerns**: Status tracking (with attempt counts, timestamps) vs. simple "was this done?" check
2. **Defense in depth**: `mark_started()` checks status first (fast path), transaction catches anything that slips through
3. **Clear semantics**: The completion table is append-only and authoritative; status table can be updated multiple times

**Transactional mode (`USE_TRANSACTIONAL_WRITES=1`)**

When enabled, the consumer uses `TransactWriteItems` to atomically:
1. Update `message_status` to COMPLETED (the side effect)
2. Write to `message_completed` with condition (the idempotency gate)

If we crash after the transaction but before SQS delete, the message gets redelivered. On redelivery, the transaction fails because `message_completed` already exists — and crucially, this also rolls back the status update, so we don't record a duplicate side effect.

### Side Effects Across Multiple External Systems

When your consumer needs to write to multiple systems (e.g., charge via Stripe, send an email, update a database), you can't wrap everything in a single transaction. A DynamoDB transaction can't include an HTTP call to Stripe.

**Common patterns:**

| Pattern | How it works | Trade-offs |
|---------|--------------|------------|
| **Outbox** | Write business data + outbox record in one DB transaction. A separate process reads the outbox and calls external systems, marking entries as processed. | Adds complexity (outbox reader), but guarantees the intent is durably recorded before external calls. |
| **Saga** | Break the operation into steps with compensating actions. If step 3 fails, run compensation for steps 1–2 (e.g., refund the charge). | Works for multi-system workflows, but compensation logic can be complex and some actions aren't reversible. |
| **Idempotency keys to external systems** | Pass a stable key (e.g., `Idempotency-Key` header to Stripe). On retry, the external system returns the cached result instead of re-executing. | Relies on external systems supporting idempotency. Most payment APIs do; arbitrary HTTP endpoints may not. |

The outbox pattern is particularly common: you never call an external system unless the intent is already durably recorded, and you never lose track of what succeeded or failed.

### Delivery Idempotency vs. Business Idempotency

There are two distinct duplication problems:

| Type | Cause | Solution |
|------|-------|----------|
| **Delivery idempotency** | SQS redelivers the same message (visibility timeout, crash before delete) | Dedupe on `message_id` — the ID the producer assigned to the message |
| **Business idempotency** | Producer sends the same logical work twice with different message IDs (user double-clicks, retry logic, bugs) | Dedupe on a business key like `order_id` or `payment_intent_id` |

This demo implements delivery idempotency: the `message_completed` table is keyed on `message_id`, which the producer generates as a UUID. If the same message is redelivered, the conditional write fails and we skip reprocessing.

But if the producer sends two messages with different UUIDs that both say "charge order-123", the consumer will process both — because they have different `message_id` values. To catch this, you need the producer to include a business-level idempotency key in the payload:

```json
{
  "id": "uuid-1",
  "order_id": "order-123",
  "action": "charge"
}
```

Then the consumer dedupes on `order_id + action`, not just `message_id`.

### How Real-World Systems Handle This

- **Stripe** requires callers to provide an `Idempotency-Key` header. If you don't, duplicate charges are your responsibility.

- **Payment systems** typically use `payment_intent_id` or `order_id` as the idempotency key, not the message/event ID. The message is just a delivery mechanism.

- **Event-driven architectures** often treat events as "facts that happened" and push idempotency responsibility to consumers. If you receive `OrderCreated` twice with different event IDs but the same `order_id`, you're expected to dedupe.

- **API gateways** sometimes generate client-side idempotency keys and reject duplicates at the edge before they hit the queue.

The key insight: **message-level idempotency protects against infrastructure problems (redelivery), but business-level idempotency protects against application problems (duplicate requests).** Most production systems need both.
