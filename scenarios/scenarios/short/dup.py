from __future__ import annotations

import argparse
import json
import time
import uuid

from lib.aws import queue_url_from_arn
from lib.cleanup import cleanup_scenario_state
from lib.consumer import run_local_consumer_async
from lib.waiters import ensure_queue_empty, wait_for_queue_empty, wait_for_messages_enqueued
from lib.types import ScenarioContext


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--slow-seconds",
        type=int,
        default=30,
        help="Seconds each consumer sleeps to simulate long processing",
    )
    parser.add_argument(
        "--second-start-delay",
        type=int,
        default=0,
        help="Delay before starting the second consumer (0 for concurrent start)",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=60,
        help="Idle timeout for consumers to exit when queue is empty",
    )
    parser.add_argument(
        "--consumer-timeout",
        type=int,
        default=120,
        help="Timeout (seconds) to wait for each consumer process",
    )
    parser.add_argument(
        "--queue-timeout",
        type=int,
        default=180,
        help="Timeout (seconds) to wait for queue to drain",
    )


def run(args: argparse.Namespace, ctx: ScenarioContext) -> None:
    """Duplicate delivery handled via idempotent side effects."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]
    status_table_name = outputs["message_status_table"]
    completed_table_name = outputs["message_side_effects_table"]

    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    cleanup_scenario_state(ctx.sqs, ctx.dynamo, outputs, "dup")
    status_table = ctx.dynamo.Table(status_table_name)
    completed_table = ctx.dynamo.Table(completed_table_name)

    message_id = str(uuid.uuid4())
    payload = {"id": message_id, "work": "duplicate-demo"}
    print(f"[dup] Sending message {message_id} ...")
    ctx.sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(payload))
    wait_for_messages_enqueued(ctx.sqs, queue_url, expected=1)

    common_env = {
        "MESSAGE_STATUS_TABLE": status_table_name,
        "MESSAGE_SIDE_EFFECTS_TABLE": completed_table_name,
        "SIDE_EFFECT_DELAY_SECONDS": str(args.slow_seconds),
        "MESSAGE_LIMIT": "1",
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "1",
        "QUEUE_URL": queue_url,
        "AWS_REGION": ctx.region,
    }

    print("[dup] Starting two local consumers concurrently (long work > visibility timeout) ...")
    proc1 = run_local_consumer_async(common_env)
    time.sleep(args.second_start_delay)
    proc2 = run_local_consumer_async(common_env)

    proc1.wait(timeout=args.consumer_timeout)
    proc2.wait(timeout=args.consumer_timeout)
    if proc1.returncode != 0:
        raise RuntimeError(f"[dup] first consumer failed with exit code {proc1.returncode}")
    if proc2.returncode != 0:
        raise RuntimeError(f"[dup] second consumer failed with exit code {proc2.returncode}")

    print("[dup] Waiting for queue to drain ...")
    wait_for_queue_empty(ctx.sqs, queue_url, timeout=args.queue_timeout)
    ensure_queue_empty(ctx.sqs, dlq_url, "DLQ")

    status_item = status_table.get_item(Key={"message_id": message_id}).get("Item")
    if not status_item:
        raise RuntimeError("[dup] Status record missing")
    status = status_item.get("status")
    attempts = int(status_item.get("attempts", 0))
    if status != "COMPLETED":
        raise RuntimeError(f"[dup] Expected COMPLETED status, found {status}")
    if attempts < 2:
        raise RuntimeError(f"[dup] Expected at least 2 attempts recorded, saw {attempts}")

    completed_item = completed_table.get_item(Key={"message_id": message_id}).get("Item")
    if not completed_item:
        raise RuntimeError("[dup] Completion record missing (idempotency failed)")

    print(
        f"[dup] PASS | attempts={attempts} completion_timestamp={completed_item.get('processed_at')}"
    )
