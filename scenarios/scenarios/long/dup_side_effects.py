from __future__ import annotations

import argparse
import time
from typing import Dict

from lib.aws import queue_url_from_arn
from lib.cleanup import cleanup_scenario_state
from lib.consumer import run_local_consumer, run_local_consumer_async
from lib.producer import run_producer
from lib.waiters import ensure_queue_empty, wait_for_messages_enqueued, wait_for_queue_empty
from lib.types import ScenarioContext
from lib.constants import DEFAULT_VISIBILITY_BUFFER


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--count", type=int, default=5, help="Messages to send")
    parser.add_argument(
        "--crash-window",
        type=int,
        default=15,
        help="Seconds to let consumer run before killing (should process all messages)",
    )
    parser.add_argument(
        "--visibility-wait",
        type=int,
        default=DEFAULT_VISIBILITY_BUFFER,
        help="Seconds to wait for visibility timeout to expire after crash",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=30,
        help="Idle timeout for second consumer run",
    )
    parser.add_argument(
        "--consumer-timeout",
        type=int,
        default=120,
        help="Timeout (seconds) for consumer subprocess",
    )
    parser.add_argument(
        "--queue-timeout",
        type=int,
        default=120,
        help="Timeout (seconds) to wait for queue to drain",
    )


def run(args: argparse.Namespace, ctx: ScenarioContext) -> None:
    """External side effects — check-before-doing prevents duplicate effects."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    status_table_name = outputs["message_status_table"]
    side_effects_table_name = outputs["message_side_effects_table"]

    cleanup_scenario_state(ctx.sqs, ctx.dynamo, outputs, "side-effects")

    message_count = args.count

    print(f"[side-effects] Sending {message_count} messages ...")
    run_producer(
        message_count,
        batch_size=min(10, message_count),
        region=ctx.region,
        queue_url=queue_url,
        profile=ctx.profile,
    )
    print("[side-effects] Confirming messages are enqueued ...")
    wait_for_messages_enqueued(ctx.sqs, queue_url, message_count)

    print("[side-effects] Running consumer with CRASH_AFTER_SIDE_EFFECT=1 ...")
    consumer_env = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": ctx.region,
        "MESSAGE_STATUS_TABLE": status_table_name,
        "MESSAGE_SIDE_EFFECTS_TABLE": side_effects_table_name,
        "CRASH_AFTER_SIDE_EFFECT": "1",
        "IDLE_TIMEOUT_SECONDS": "10",
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "5",
    }

    proc = run_local_consumer_async(consumer_env)
    time.sleep(args.crash_window)
    if proc.poll() is None:
        proc.terminate()
        proc.wait(timeout=10)

    print(f"[side-effects] Waiting {args.visibility_wait}s for visibility timeout ...")
    time.sleep(args.visibility_wait)

    print("[side-effects] Running consumer to handle redeliveries ...")
    consumer_env_normal = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": ctx.region,
        "MESSAGE_STATUS_TABLE": status_table_name,
        "MESSAGE_SIDE_EFFECTS_TABLE": side_effects_table_name,
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "5",
    }

    exit_code = run_local_consumer(env=consumer_env_normal, timeout=args.consumer_timeout)
    if exit_code != 0:
        raise RuntimeError(f"Second consumer run failed with exit code {exit_code}")

    print("[side-effects] Waiting for queue to drain ...")
    wait_for_queue_empty(ctx.sqs, queue_url, timeout=args.queue_timeout)

    side_effects_table = ctx.dynamo.Table(side_effects_table_name)
    side_effects_count = 0
    scan_kw: Dict = {"Select": "COUNT"}
    while True:
        resp = side_effects_table.scan(**scan_kw)
        side_effects_count += resp["Count"]
        if "LastEvaluatedKey" not in resp:
            break
        scan_kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if side_effects_count != message_count:
        raise RuntimeError(
            f"Expected {message_count} side effect records, found {side_effects_count}. "
            "Duplicate side effects may have occurred!"
        )

    status_table = ctx.dynamo.Table(status_table_name)
    status_count = 0
    scan_kw = {"Select": "COUNT", "FilterExpression": "#s = :completed"}
    scan_kw["ExpressionAttributeNames"] = {"#s": "status"}
    scan_kw["ExpressionAttributeValues"] = {":completed": "COMPLETED"}
    while True:
        resp = status_table.scan(**scan_kw)
        status_count += resp["Count"]
        if "LastEvaluatedKey" not in resp:
            break
        scan_kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if status_count != message_count:
        raise RuntimeError(
            f"Expected {message_count} COMPLETED statuses, found {status_count}"
        )

    ensure_queue_empty(ctx.sqs, dlq_url, "DLQ")

    print(
        f"[side-effects] PASS | {message_count} messages processed, "
        f"{side_effects_count} unique side effects (no duplicates)"
    )
