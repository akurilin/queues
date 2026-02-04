from __future__ import annotations

import argparse
import json
import os
import time
import uuid

from lib.aws import queue_url_from_arn
from lib.cleanup import cleanup_scenario_state
from lib.consumer import run_local_consumer_async
from lib.waiters import ensure_queue_empty, wait_for_messages_enqueued, wait_for_queue_empty
from lib.types import ScenarioContext


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--count", type=int, default=10, help="Messages to send")
    parser.add_argument(
        "--group-id",
        type=str,
        default="tenant-1",
        help="MessageGroupId to enforce FIFO ordering",
    )
    parser.add_argument(
        "--consumer-count",
        type=int,
        default=5,
        help="Number of local consumers to run concurrently",
    )
    parser.add_argument(
        "--log-path",
        type=str,
        default="/tmp/fifo_output",
        help="Path to log file used for ordering validation",
    )
    parser.add_argument(
        "--consumer-start-delay",
        type=float,
        default=1.0,
        help="Seconds to wait after starting consumers before sending messages",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=10,
        help="Idle timeout for consumers to exit when queue is empty",
    )
    parser.add_argument(
        "--consumer-timeout",
        type=int,
        default=60,
        help="Timeout (seconds) to wait for each consumer process",
    )
    parser.add_argument(
        "--queue-timeout",
        type=int,
        default=120,
        help="Timeout (seconds) to wait for queue to drain",
    )


def run(args: argparse.Namespace, ctx: ScenarioContext) -> None:
    """FIFO ordering with a single message group."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])

    cleanup_scenario_state(ctx.sqs, ctx.dynamo, outputs, "fifo-order")

    attrs = ctx.sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["FifoQueue"])[
        "Attributes"
    ]
    if attrs.get("FifoQueue") != "true":
        raise RuntimeError("[fifo-order] Queue is not FIFO; check terraform config")

    message_count = args.count
    group_id = args.group_id
    log_path = args.log_path

    if os.path.exists(log_path):
        os.remove(log_path)

    consumer_env = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": ctx.region,
        "MESSAGE_LIMIT": str(message_count),
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "1",
        "SIDE_EFFECT_LOG_PATH": log_path,
        "SIDE_EFFECT_LOG_FIELD": "sequence",
    }

    print(f"[fifo-order] Starting {args.consumer_count} consumers ...")
    procs = []
    for _ in range(args.consumer_count):
        procs.append(run_local_consumer_async(consumer_env, quiet=True))
    time.sleep(args.consumer_start_delay)

    print(f"[fifo-order] Sending {message_count} FIFO messages (group={group_id}) ...")
    entries = []
    for i in range(1, message_count + 1):
        payload = {"id": f"seq-{i}", "sequence": i}
        entries.append(
            {
                "Id": str(i),
                "MessageBody": json.dumps(payload),
                "MessageGroupId": group_id,
                "MessageDeduplicationId": f"{group_id}-{i}-{uuid.uuid4()}",
            }
        )
    for start in range(0, len(entries), 10):
        ctx.sqs.send_message_batch(QueueUrl=queue_url, Entries=entries[start : start + 10])

    wait_for_messages_enqueued(ctx.sqs, queue_url, message_count)

    for proc in procs:
        proc.wait(timeout=args.consumer_timeout)
        if proc.returncode != 0:
            raise RuntimeError(f"[fifo-order] Consumer failed with exit code {proc.returncode}")

    print("[fifo-order] Waiting for queue to drain ...")
    wait_for_queue_empty(ctx.sqs, queue_url, timeout=args.queue_timeout)
    ensure_queue_empty(ctx.sqs, dlq_url, "DLQ")

    if not os.path.exists(log_path):
        raise RuntimeError(f"[fifo-order] Log file missing: {log_path}")
    with open(log_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle.readlines() if line.strip()]
    try:
        data = [int(line) for line in lines]
    except ValueError as exc:
        raise RuntimeError(f"[fifo-order] Log contains non-integer entries: {lines}") from exc
    expected = list(range(1, message_count + 1))
    if data != expected:
        raise RuntimeError(f"[fifo-order] Log out of order: expected {expected}, got {data}")

    print(f"[fifo-order] PASS | log={log_path} entries={len(data)}")
