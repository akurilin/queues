from __future__ import annotations

import argparse
import time

from lib.aws import queue_url_from_arn
from lib.cleanup import cleanup_scenario_state
from lib.consumer import run_local_consumer
from lib.producer import run_producer
from lib.waiters import ensure_queue_empty, wait_for_messages_enqueued, wait_for_queue_empty
from lib.types import ScenarioContext
from lib.constants import DEFAULT_VISIBILITY_BUFFER


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--visibility-wait",
        type=int,
        default=DEFAULT_VISIBILITY_BUFFER,
        help="Seconds to wait for visibility timeout to expire",
    )
    parser.add_argument(
        "--consumer-timeout",
        type=int,
        default=90,
        help="Timeout (seconds) for consumer subprocesses",
    )


def run(args: argparse.Namespace, ctx: ScenarioContext) -> None:
    """Consumer crash mid-processing, then redelivery."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])

    cleanup_scenario_state(ctx.sqs, ctx.dynamo, outputs, "crash")

    message_count = 1
    print(f"[crash] Sending {message_count} message ...")
    run_producer(message_count, batch_size=1, region=ctx.region, queue_url=queue_url, profile=ctx.profile)
    print("[crash] Confirming message is enqueued ...")
    wait_for_messages_enqueued(ctx.sqs, queue_url, message_count)

    print("[crash] Running local consumer that will crash mid-processing ...")
    exit_code = run_local_consumer(
        env={
            "QUEUE_URL": queue_url,
            "AWS_REGION": ctx.region,
            "CRASH_AFTER_RECEIVE": "1",
            "MESSAGE_LIMIT": str(message_count),
            "LOG_LEVEL": "INFO",
            "IDLE_TIMEOUT_SECONDS": "30",
        },
        timeout=args.consumer_timeout,
    )
    if exit_code == 0:
        raise RuntimeError("Crash consumer exited 0 but was expected to fail")

    visibility_wait = args.visibility_wait or DEFAULT_VISIBILITY_BUFFER
    print(f"[crash] Waiting {visibility_wait}s for visibility timeout to expire ...")
    time.sleep(visibility_wait)

    print("[crash] Rerunning consumer to process redelivered message ...")
    exit_code2 = run_local_consumer(
        env={
            "QUEUE_URL": queue_url,
            "AWS_REGION": ctx.region,
            "MESSAGE_LIMIT": str(message_count),
            "LOG_LEVEL": "INFO",
            "IDLE_TIMEOUT_SECONDS": "30",
        },
        timeout=args.consumer_timeout,
    )
    if exit_code2 != 0:
        raise RuntimeError(f"Second consumer failed with exit code {exit_code2}")

    print("[crash] Waiting for queue to drain ...")
    wait_for_queue_empty(ctx.sqs, queue_url)
    ensure_queue_empty(ctx.sqs, dlq_url, "DLQ")
    print("[crash] PASS")
