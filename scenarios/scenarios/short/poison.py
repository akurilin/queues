from __future__ import annotations

import argparse

from lib.aws import queue_url_from_arn, get_queue_depth
from lib.cleanup import cleanup_scenario_state
from lib.consumer import run_local_consumer
from lib.producer import run_producer
from lib.waiters import wait_for_messages_enqueued, wait_for_queue_empty
from lib.types import ScenarioContext


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--count", type=int, default=5, help="Total messages to send")
    parser.add_argument("--poison-count", type=int, default=2, help="Number of poison messages")
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
    """Poison messages exhaust retries and land in the DLQ."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])

    cleanup_scenario_state(ctx.sqs, ctx.dynamo, outputs, "poison")

    total_count = args.count
    poison_count = args.poison_count
    good_count = total_count - poison_count

    print(f"[poison] Sending {total_count} messages ({poison_count} poison, {good_count} good) ...")
    run_producer(
        total_count,
        batch_size=min(10, total_count),
        region=ctx.region,
        queue_url=queue_url,
        profile=ctx.profile,
        poison_count=poison_count,
    )
    print("[poison] Confirming messages are enqueued ...")
    wait_for_messages_enqueued(ctx.sqs, queue_url, total_count)

    print("[poison] Running consumer with REJECT_PAYLOAD_MARKER=POISON ...")
    consumer_env = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": ctx.region,
        "REJECT_PAYLOAD_MARKER": "POISON",
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "5",
        "MESSAGE_STATUS_TABLE": outputs["message_status_table"],
        "MESSAGE_SIDE_EFFECTS_TABLE": outputs["message_side_effects_table"],
    }

    exit_code = run_local_consumer(env=consumer_env, timeout=args.consumer_timeout)
    if exit_code != 0:
        raise RuntimeError(f"Poison consumer failed with exit code {exit_code}")

    print("[poison] Waiting for main queue to fully drain ...")
    wait_for_queue_empty(ctx.sqs, queue_url, timeout=args.queue_timeout)

    print("[poison] Checking DLQ for poison messages ...")
    dlq_depth = get_queue_depth(ctx.sqs, dlq_url)
    dlq_count = dlq_depth["visible"] + dlq_depth["not_visible"]
    if dlq_count != poison_count:
        raise RuntimeError(
            f"Expected {poison_count} messages in DLQ, found {dlq_count} "
            f"(visible={dlq_depth['visible']}, not_visible={dlq_depth['not_visible']})"
        )

    print(f"[poison] PASS | good={good_count} processed, poison={poison_count} in DLQ")
