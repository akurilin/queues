from __future__ import annotations

import argparse
import json
import os
import random
import time

from lib.aws import queue_url_from_arn
from lib.cleanup import cleanup_scenario_state
from lib.consumer import run_local_consumer_async
from lib.waiters import ensure_queue_empty, wait_for_messages_enqueued, wait_for_queue_empty
from lib.types import ScenarioContext


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--count", type=int, default=10, help="Messages to send")
    parser.add_argument(
        "--entity-id",
        type=str,
        default="acct-1",
        help="Entity ID for versioned messages",
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
        default="/tmp/version_output",
        help="Path to log file used for version validation",
    )
    parser.add_argument(
        "--state-path",
        type=str,
        default="/tmp/version_state.json",
        help="Path to version state file",
    )
    parser.add_argument(
        "--seen-log-path",
        type=str,
        default="/tmp/version_seen_output",
        help="Path to log file that records every received version",
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
    """Out-of-order delivery with version-based reconciliation."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])

    cleanup_scenario_state(ctx.sqs, ctx.dynamo, outputs, "version-order")

    message_count = args.count
    entity_id = args.entity_id
    log_path = args.log_path
    state_path = args.state_path
    seen_log_path = args.seen_log_path

    for path in (log_path, state_path, seen_log_path):
        if os.path.exists(path):
            os.remove(path)

    consumer_env = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": ctx.region,
        "MESSAGE_LIMIT": str(message_count),
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "1",
        "VERSIONING_ENABLED": "1",
        "VERSION_STATE_PATH": state_path,
        "VERSION_LOG_PATH": log_path,
        "VERSION_SEEN_LOG_PATH": seen_log_path,
        "VERSION_KEY": "version",
        "ENTITY_KEY": "entity_id",
    }

    versions = list(range(1, message_count + 1))
    random.shuffle(versions)
    print(f"[version-order] Sending {message_count} messages in random order ...")
    entries = []
    for idx, version in enumerate(versions, start=1):
        payload = {"id": f"ver-{version}", "entity_id": entity_id, "version": version}
        entries.append(
            {
                "Id": str(idx),
                "MessageBody": json.dumps(payload),
            }
        )
    for start in range(0, len(entries), 10):
        ctx.sqs.send_message_batch(QueueUrl=queue_url, Entries=entries[start : start + 10])

    print(f"[version-order] Submitted {message_count} messages to the queue.")
    wait_for_messages_enqueued(ctx.sqs, queue_url, message_count)

    print(f"[version-order] Starting {args.consumer_count} consumers ...")
    procs = []
    for _ in range(args.consumer_count):
        procs.append(run_local_consumer_async(consumer_env, quiet=False))
    time.sleep(args.consumer_start_delay)

    for proc in procs:
        proc.wait(timeout=args.consumer_timeout)
        if proc.returncode != 0:
            raise RuntimeError(
                f"[version-order] Consumer failed with exit code {proc.returncode}"
            )

    print("[version-order] Waiting for queue to drain ...")
    wait_for_queue_empty(ctx.sqs, queue_url, timeout=args.queue_timeout)
    ensure_queue_empty(ctx.sqs, dlq_url, "DLQ")

    if not os.path.exists(log_path):
        raise RuntimeError(f"[version-order] Log file missing: {log_path}")
    if not os.path.exists(seen_log_path):
        raise RuntimeError(f"[version-order] Seen log file missing: {seen_log_path}")
    with open(log_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle.readlines() if line.strip()]
    with open(seen_log_path, "r", encoding="utf-8") as handle:
        seen_lines = [line.strip() for line in handle.readlines() if line.strip()]
    try:
        data = [int(line) for line in lines]
    except ValueError as exc:
        raise RuntimeError(f"[version-order] Log contains non-integer entries: {lines}") from exc
    try:
        seen_data = [int(line) for line in seen_lines]
    except ValueError as exc:
        raise RuntimeError(
            f"[version-order] Seen log contains non-integer entries: {seen_lines}"
        ) from exc

    if not data:
        raise RuntimeError("[version-order] No versions were applied")
    if len(data) > message_count:
        raise RuntimeError(
            f"[version-order] Unexpected log size: {len(data)} > {message_count}"
        )
    if any(value < 1 or value > message_count for value in data):
        raise RuntimeError(
            f"[version-order] Log contains out-of-range versions: {data}"
        )
    if any(data[i] >= data[i + 1] for i in range(len(data) - 1)):
        raise RuntimeError(f"[version-order] Log not strictly increasing: {data}")
    max_version = max(data)
    if max_version != message_count:
        raise RuntimeError(
            f"[version-order] Latest version not applied: expected {message_count}, got {max_version} | log={data}"
        )
    expected_versions = set(range(1, message_count + 1))
    seen_versions = set(seen_data)
    missing_versions = sorted(expected_versions - seen_versions)
    if missing_versions:
        raise RuntimeError(
            f"[version-order] Missing versions in seen log: {missing_versions} | seen={seen_data}"
        )

    print(f"[version-order] PASS | log={log_path} entries={len(data)}")
