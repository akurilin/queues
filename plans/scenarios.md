# Queue Scenarios Plan (Persisted Context)

This file captures the agreed plan, schemas, and Terraform sketch so work can be resumed without the prior conversation. No changes have been applied yet—this is a blueprint.

## Goals
- Add runnable scenarios to exercise SQS queue behaviors (happy path + 10 deviations) using existing producer/consumer scripts.
- Keep AWS costs low (small message counts; short runs; on-demand DynamoDB; minimal ECS scale).
- Provide a Python-based runner to orchestrate scenarios and assert PASS/FAIL.
- Extend infrastructure with DynamoDB side-effect tables and ECS autoscaling on SQS backlog.

## Six Scenarios (what to run and assert)
1) **Happy Path**  
   Producer sends N (tiny). Consumer processes and deletes. Expect queue drains to 0 and N completions recorded.

2) **Consumer Crash Mid-Processing**  
   Consumer crashes before delete (e.g., high `CRASH_RATE` or explicit crash flag). Visibility expires; message is redelivered; next run processes and deletes. Assert message eventually marked completed and queue drains.

3) **Duplicate Delivery**  
   Force redelivery via slow processing vs. short visibility or `NO_DELETE_AFTER_PROCESS=1`. Expect duplicate receive; consumer must be idempotent (skip second side effect). Assert only one completion record per message.

4) **Poison Messages**  
   Producer injects malformed/bad payloads. Consumer fails them; SQS redrives to DLQ after `maxReceiveCount`. Assert DLQ depth matches poison count; main queue drains; status table shows failures.

5) **Backpressure / Scaling**  
   Burst producer rate to create backlog. ECS/Fargate autoscaling target-tracks `ApproximateNumberOfMessagesVisible` per task. Assert backlog forms then drains after scale-out; scale-in after cooldown.

6) **Downstream Side Effects**
   Side effect = DynamoDB writes (idempotent). Demonstrate: success path; crash after side effect before delete; duplicates skipped via conditional writes. Assert exactly one completion record per message; status reflects attempts.

7) **Partial Batch Failure**
   SQS lets you receive up to 10 messages per poll. If the consumer processes 8 successfully but fails on 2, it needs to delete only the 8 and leave the 2 for redelivery. The naive approach — treating the batch as all-or-nothing — either loses successful work (if you fail the whole batch) or silently drops failures (if you succeed the whole batch). In production, most consumers poll in batches for throughput, so getting this wrong means either reprocessing messages unnecessarily or losing them. AWS added the `ReportBatchItemFailures` feature for Lambda specifically because this is so common to get wrong. Assert that successful messages are deleted, failed messages are redelivered, and no messages are lost or duplicated unnecessarily.

8) **Visibility Timeout Extension**
   When a consumer picks up a message, it becomes invisible to other consumers for the visibility timeout period. If processing takes longer than that — say a downstream API is slow, or the work is just heavy — the message becomes visible again and another consumer picks it up. The duplicates scenario demonstrates the *consequence* of this (duplicate delivery), but this scenario demonstrates the *mitigation*: calling `ChangeMessageVisibility` to extend the timeout while still processing. In production, processing time is variable and unpredictable. Without heartbeating/extending visibility, any slow message becomes a duplicate, which at scale creates a storm of redundant work. Assert that the consumer extends visibility during long processing and that no duplicate delivery occurs despite processing time exceeding the original visibility timeout.

9) **Graceful Consumer Shutdown**
   Every time you deploy, scale down, or restart a consumer, the process receives a SIGTERM. If the consumer is mid-processing, it has two choices: drop the message (it'll come back after visibility timeout, but you've wasted work and added latency) or finish processing before exiting. In containerized environments like ECS, this happens on every deployment. If the consumer doesn't handle SIGTERM, you get a burst of redeliveries after every deploy, plus a window where messages sit unprocessed until the visibility timeout expires. The stop timeout in ECS defaults to 30 seconds before SIGKILL, so the consumer has a finite window to wrap up. Assert that the consumer finishes in-flight messages after receiving SIGTERM, stops polling for new messages, and exits cleanly without orphaning messages.

10) **Out-of-Order Processing**
    Standard SQS makes no ordering guarantees. If a producer sends events A, B, C representing a sequence (e.g., `order_created`, `order_updated`, `order_cancelled`), consumers might receive them as B, A, C or any other permutation. If the consumer applies these as state transitions, processing `order_updated` before `order_created` either fails or corrupts state. Production systems handle this with version numbers, timestamps, or last-write-wins semantics. FIFO queues exist but come with throughput tradeoffs (300 msg/s without batching, 3000 with). Most production systems use standard queues and handle ordering in application logic, so understanding and demonstrating that tradeoff is important. Assert that final state is correct regardless of delivery order, using version-based or timestamp-based reconciliation logic.

11) **Queue Backlog Drain After Outage**
    When the consumer goes down — a bad deploy, an infrastructure issue, a downstream dependency outage — messages pile up. When the consumer comes back, it faces a backlog of potentially thousands of messages. This stresses several things at once: can you scale consumers horizontally to drain faster? Do your downstream dependencies handle the burst? Does your idempotency logic hold up under load? Are messages still valid or have some expired/become stale? This is distinct from normal steady-state processing and is the scenario most likely to expose problems you didn't find during normal operation. Every production queue system eventually has an outage, and the recovery is often harder than the outage itself. Assert that the backlog drains fully, no messages are lost, idempotency holds under concurrent consumer load, and stale messages are handled gracefully.

## Code Changes Needed (not yet applied)
- **Consumer (`consumer/consume.py`)**
  - Add flags/envs:
    - `NO_DELETE_AFTER_PROCESS` (simulate delete drop)
    - `VISIBILITY_EXTENSION_SECONDS`, `EXTEND_EVERY_N` (heartbeat/extend visibility)
    - DynamoDB integration: table names, conditional writes for STARTED/COMPLETED; optional `TRANSactWrite` for atomic completion.
  - Side-effect hooks to write to DynamoDB tables with idempotency.

- **Producer (`producer/produce.py`)**
  - Add flags to:
    - Inject poison payload percentage.
    - Accept payload template file.
    - Optionally include message attributes/FIFO keys (if needed).

- **Runner (`scenarios/run.py`)**
  - Python CLI to run scenarios: set envs, invoke producer via subprocess, invoke consumer locally or assume ECS, poll SQS metrics/DLQ and DynamoDB tables, assert PASS/FAIL.
  - Optional thin shell wrappers per scenario.

## DynamoDB Design (agreed)
- **Table `message_status`**
  - PK: `message_id` (S)
  - Attrs: `status` (STARTED|COMPLETED|FAILED), `attempts` (N), `last_updated` (ISO string), `error_reason` (S, optional), `payload_digest` (S, optional).
  - Operations:
    - START: `PutItem` with `ConditionExpression attribute_not_exists(message_id)`; Item includes `status="STARTED"`, `attempts=1`, `last_updated`, `payload_digest`.
    - Duplicate START: `UpdateItem` with `ConditionExpression attribute_exists(message_id)`; `SET attempts = if_not_exists(attempts,1)+1, last_updated=:now`.
    - COMPLETE status: `UpdateItem` with `ConditionExpression attribute_exists(message_id)`; `SET status=:completed, attempts = if_not_exists(attempts,1)+1, last_updated=:now, error_reason=:empty`.
    - FAILED: `UpdateItem` with `ConditionExpression attribute_exists(message_id)`; set `status="FAILED"`, increment attempts, set `error_reason`, `last_updated`.

- **Table `message_completed`**
  - PK: `message_id` (S)
  - Attrs: `processed_at` (ISO), `payload_digest` (S).
  - COMPLETE insert: `PutItem` with `ConditionExpression attribute_not_exists(message_id)`.
  - If conditional fails → duplicate completion detected; treat as already processed (skip side effect).

- **Side-effect flow per message**
  1) `Put STARTED` (conditional) or `Update attempts` if already exists.
  2) Perform side effect (business logic).
  3) `Put` into `message_completed` with `attribute_not_exists` (idempotent).
  4) `Update` `message_status` to COMPLETED.  
     - Option: steps 3+4 inside a `TransactWriteItems` (idempotent because writes are conditional).

## Terraform Additions (sketch, not applied)
- **DynamoDB tables**
  ```hcl
  resource "aws_dynamodb_table" "message_status" {
    name         = "${var.project_name}-message-status"
    billing_mode = "PAY_PER_REQUEST"
    hash_key     = "message_id"

    attribute { name = "message_id" type = "S" }
  }

  resource "aws_dynamodb_table" "message_completed" {
    name         = "${var.project_name}-message-completed"
    billing_mode = "PAY_PER_REQUEST"
    hash_key     = "message_id"

    attribute { name = "message_id" type = "S" }
  }
  ```

- **IAM for consumer task role** (add to policy document):
  ```
  actions = [
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:TransactWriteItems",
    "dynamodb:GetItem"
  ]
  resources = [
    aws_dynamodb_table.message_status.arn,
    aws_dynamodb_table.message_completed.arn
  ]
  ```

- **Outputs/ENV**
  - Extend `.env` writer to include `MESSAGE_STATUS_TABLE`, `MESSAGE_COMPLETED_TABLE`.
  - Add same envs to ECS task definition for the consumer.

- **ECS autoscaling on SQS backlog (Scenario 5)**
  ```hcl
  resource "aws_appautoscaling_target" "ecs_service" {
    max_capacity       = var.autoscale_max_count        # e.g., 5
    min_capacity       = var.desired_count              # reuse existing desired_count as min
    resource_id        = "service/${aws_ecs_cluster.this.name}/${aws_ecs_service.consumer.name}"
    scalable_dimension = "ecs:service:DesiredCount"
    service_namespace  = "ecs"
  }

  resource "aws_appautoscaling_policy" "scale_on_sqs" {
    name               = "${local.name}-sqs-backlog"
    policy_type        = "TargetTrackingScaling"
    resource_id        = aws_appautoscaling_target.ecs_service.resource_id
    scalable_dimension = aws_appautoscaling_target.ecs_service.scalable_dimension
    service_namespace  = aws_appautoscaling_target.ecs_service.service_namespace

    target_tracking_scaling_policy_configuration {
      predefined_metric_specification {
        predefined_metric_type = "SQSQueueMessagesVisible"
        resource_label = "${module.sqs.queue_arn}"
      }
      target_value       = var.autoscale_target_messages_per_task  # e.g., 20
      scale_in_cooldown  = 60
      scale_out_cooldown = 30
    }
  }
  ```
  - New vars: `autoscale_max_count`, `autoscale_target_messages_per_task`.

## Runner Outline (Python)
- Location: `scenarios/run.py`
- Responsibilities:
  - Parse scenario name; set env vars/flags.
  - Call producer via subprocess (`producer/produce.py`).
  - Run consumer locally (or assume ECS already running).
  - Poll SQS queue depth and DLQ via boto3; poll DynamoDB tables for status/completion counts.
  - Assert expectations per scenario; print PASS/FAIL.
- Optional: thin shell wrappers calling `python run.py <scenario>`.

## Scenario-Specific Notes
- **1 Happy Path**: Small N; `MESSAGE_LIMIT=N`; expect N completion records; queue depth 0.
- **2 Crash Mid-Processing**: Use `CRASH_RATE` or crash-on-first flag; short visibility; rerun consumer; assert eventual completion and no backlog.
- **3 Duplicates**: Use `NO_DELETE_AFTER_PROCESS=1` or long sleep vs. short visibility; expect duplicate receive; completion table only one entry per message.
- **4 Poison**: Producer injects bad payloads; consumer fails; expect DLQ count == poison count; status shows FAILED.
- **5 Backpressure**: High producer rate; observe backlog; autoscaling scales out to drain; scales in after cooldown.
- **6 Downstream**: DynamoDB writes are the side effect; show crash after side effect before delete; ensure conditional write prevents double side effect; completion table has one entry per message.
- **7 Partial Batch Failure**: Producer sends batch of messages with some intentionally malformed; consumer receives batch, processes good ones, fails bad ones; assert good messages deleted, bad messages redelivered; no messages lost.
- **8 Visibility Timeout Extension**: Consumer processes a slow message but heartbeats `ChangeMessageVisibility` to extend the timeout; assert no duplicate delivery despite processing time exceeding original visibility timeout.
- **9 Graceful Shutdown**: Consumer is mid-processing when it receives SIGTERM; assert it finishes in-flight work, stops polling, exits cleanly, and no messages are orphaned or unnecessarily redelivered.
- **10 Out-of-Order**: Producer sends sequenced events (v1, v2, v3); consumer receives them out of order; assert final state is correct using version/timestamp reconciliation logic.
- **11 Backlog Drain**: Consumer is offline while producer sends a large burst; consumer(s) come back online and drain the backlog; assert all messages processed, idempotency holds under load, no messages lost.

## Why Python Runner (vs. shell)
- Shell + `aws`/`jq` can work but gets unwieldy for waits/assertions and DynamoDB condition logic. Python can reuse boto3, cleanly express assertions, and handle retries.

## Next Implementation Steps (when approved)
1) Terraform: add DynamoDB tables, IAM perms, autoscaling target/policy, env outputs.
2) Consumer: add flags/envs; implement DynamoDB conditional writes and optional visibility extensions/delete suppression.
3) Producer: add poison injection/template support.
4) Runner: build `scenarios/run.py` + optional shell wrappers.
5) Validate locally against small queues; then optionally exercise ECS autoscaling path.
