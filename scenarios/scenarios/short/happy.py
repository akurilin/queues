from __future__ import annotations

import argparse

from lib.aws import queue_url_from_arn
from lib.cleanup import cleanup_scenario_state
from lib.consumer import run_local_consumer
from lib.producer import run_producer
from lib.waiters import ensure_queue_empty, wait_for_messages_enqueued, wait_for_queue_empty
from lib.types import ScenarioContext


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--count", type=int, default=5, help="Messages to send")
    parser.add_argument("--batch-size", type=int, default=5, help="Producer batch size (<=10)")
    parser.add_argument(
        "--consumer-timeout",
        type=int,
        default=120,
        help="Timeout (seconds) for consumer subprocess",
    )


def run(args: argparse.Namespace, ctx: ScenarioContext) -> None:
    """Happy path — messages flow through cleanly."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])

    cleanup_scenario_state(ctx.sqs, ctx.dynamo, outputs, "happy")

    message_count = args.count or 5
    batch_size = min(args.batch_size or 5, 10)

    print(f"[happy] Sending {message_count} messages ...")
    run_producer(message_count, batch_size, ctx.region, queue_url, ctx.profile)
    print("[happy] Confirming messages are enqueued ...")
    wait_for_messages_enqueued(ctx.sqs, queue_url, message_count)

    print("[happy] Running local consumer to drain messages ...")
    exit_code = run_local_consumer(
        env={
            "QUEUE_URL": queue_url,
            "AWS_REGION": ctx.region,
            "MESSAGE_LIMIT": str(message_count),
            "LOG_LEVEL": "INFO",
            "IDLE_TIMEOUT_SECONDS": "30",
        },
        timeout=args.consumer_timeout,
    )
    if exit_code != 0:
        raise RuntimeError(f"Happy consumer failed with exit code {exit_code}")

    print("[happy] Waiting for queue to drain ...")
    wait_for_queue_empty(ctx.sqs, queue_url)
    ensure_queue_empty(ctx.sqs, dlq_url, "DLQ")
    print("[happy] PASS")
