from __future__ import annotations

import argparse
from typing import Dict

from lib.aws import queue_url_from_arn, get_queue_depth
from lib.cleanup import cleanup_scenario_state
from lib.consumer import run_local_consumer
from lib.producer import run_producer
from lib.waiters import wait_for_messages_enqueued, wait_for_queue_empty
from lib.types import ScenarioContext


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--count", type=int, default=50, help="Total messages to send")
    parser.add_argument(
        "--poison-every",
        type=int,
        default=3,
        help="Make every Nth message poison (e.g., 3 means indices 2, 5, 8...)",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=30,
        help="Idle timeout for consumer to exit when queue is drained",
    )
    parser.add_argument(
        "--consumer-timeout",
        type=int,
        default=300,
        help="Timeout (seconds) for consumer subprocess",
    )
    parser.add_argument(
        "--queue-timeout",
        type=int,
        default=180,
        help="Timeout (seconds) to wait for queue to drain",
    )


def run(args: argparse.Namespace, ctx: ScenarioContext) -> None:
    """Partial batch failure — mixed good/poison in batches of 10."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    completed_table_name = outputs["message_side_effects_table"]

    cleanup_scenario_state(ctx.sqs, ctx.dynamo, outputs, "partial-batch")

    total_count = args.count
    poison_every = args.poison_every
    expected_poison = len(range(poison_every - 1, total_count, poison_every))
    expected_good = total_count - expected_poison

    print(
        f"[partial-batch] Sending {total_count} messages "
        f"(every {poison_every}th is poison: {expected_poison} poison, {expected_good} good) ..."
    )
    run_producer(
        total_count,
        batch_size=10,
        region=ctx.region,
        queue_url=queue_url,
        profile=ctx.profile,
        poison_every=poison_every,
    )
    print("[partial-batch] Confirming messages are enqueued ...")
    wait_for_messages_enqueued(ctx.sqs, queue_url, total_count)

    print("[partial-batch] Running consumer with MAX_MESSAGES=10, REJECT_PAYLOAD_MARKER=POISON ...")
    consumer_env = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": ctx.region,
        "REJECT_PAYLOAD_MARKER": "POISON",
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "10",
        "WAIT_TIME_SECONDS": "5",
        "MESSAGE_STATUS_TABLE": outputs["message_status_table"],
        "MESSAGE_SIDE_EFFECTS_TABLE": outputs["message_side_effects_table"],
    }

    exit_code = run_local_consumer(env=consumer_env, timeout=args.consumer_timeout)
    if exit_code != 0:
        raise RuntimeError(f"Partial-batch consumer failed with exit code {exit_code}")

    print("[partial-batch] Waiting for main queue to fully drain ...")
    wait_for_queue_empty(ctx.sqs, queue_url, timeout=args.queue_timeout)

    print("[partial-batch] Checking DLQ for poison messages ...")
    dlq_depth = get_queue_depth(ctx.sqs, dlq_url)
    dlq_count = dlq_depth["visible"] + dlq_depth["not_visible"]
    if dlq_count != expected_poison:
        raise RuntimeError(
            f"Expected {expected_poison} messages in DLQ, found {dlq_count} "
            f"(visible={dlq_depth['visible']}, not_visible={dlq_depth['not_visible']})"
        )

    completed_table = ctx.dynamo.Table(completed_table_name)
    completed_count = 0
    scan_kw: Dict = {"Select": "COUNT"}
    while True:
        resp = completed_table.scan(**scan_kw)
        completed_count += resp["Count"]
        if "LastEvaluatedKey" not in resp:
            break
        scan_kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if completed_count != expected_good:
        raise RuntimeError(
            f"Expected {expected_good} completed messages in DynamoDB, found {completed_count}"
        )

    print(
        f"[partial-batch] PASS | good={expected_good} completed, "
        f"poison={expected_poison} in DLQ"
    )
