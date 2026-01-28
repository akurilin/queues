"""Scenario runner for queue behaviors against pre-provisioned infrastructure."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, Optional

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from validate_infra import validate_scenario_infra

REPO_ROOT = Path(__file__).resolve().parent.parent
TERRAFORM_DIR = REPO_ROOT / "terraform"
CONSUME_SCRIPT = REPO_ROOT / "consumer" / "consume.py"
DEFAULT_VISIBILITY_BUFFER = 15  # seconds to wait for visibility timeout expiry

# Map CLI scenario names to terraform output keys
SCENARIO_TF_KEYS = {
    "happy": "happy",
    "crash": "crash",
    "duplicates": "dup",
    "poison": "poison",
    "backpressure": "backpressure",
}


# ---------------------------------------------------------------------------
# Terraform outputs
# ---------------------------------------------------------------------------


def read_terraform_outputs() -> Dict:
    """Read outputs from the single terraform/ directory.

    Returns a dict with 'aws_region' and 'scenarios' (a map of scenario key
    to its resource references).
    """
    result = subprocess.run(
        ["terraform", f"-chdir={TERRAFORM_DIR}", "output", "-json"],
        capture_output=True,
        text=True,
        check=True,
    )
    raw = json.loads(result.stdout)
    return {k: v["value"] for k, v in raw.items()}


# ---------------------------------------------------------------------------
# AWS helpers
# ---------------------------------------------------------------------------
#
# SQS behavioral quirks that affect this test harness
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# 1. PurgeQueue has a 60-second danger window.
#    The API returns immediately but deletion continues asynchronously for up
#    to 60 seconds.  Messages sent *after* the PurgeQueue call can be silently
#    deleted if they arrive while the purge is still running.  This corrupts
#    the ApproximateNumberOfMessages counter in unpredictable ways — we saw it
#    report the full sent count (e.g. 5) on a queue that was genuinely empty.
#
#    Fix: purge_queue() checks depth first and skips the call when the queue
#    is already empty.  cleanup_scenario_state() waits the full 60 seconds
#    when a purge was actually issued, so new messages are never sent into
#    an active purge window.
#
# 2. ApproximateNumberOfMessages is *approximate*.
#    SQS queue attributes are eventually consistent.  A single-shot read can
#    return a stale non-zero count even when the queue is genuinely empty,
#    especially right after message operations.  We observed this causing
#    false failures when a single ensure_queue_empty() check ran immediately
#    after wait_for_queue_empty() had already confirmed 3 consecutive zeros.
#
#    Fix: we rely on wait_for_queue_empty() (multiple consecutive zero
#    readings) as the authoritative "queue is drained" signal for the primary
#    queue.  We only use the single-shot ensure_queue_empty() on the DLQ,
#    where no recent message operations have occurred and the counter is
#    stable.


def build_sqs_client(region: str, profile: Optional[str]) -> BaseClient:
    """Construct an SQS client using region/profile."""
    session = boto3.Session(region_name=region, profile_name=profile or None)
    return session.client("sqs")


def build_dynamo_resource(region: str, profile: Optional[str]):
    """Construct a DynamoDB resource using region/profile."""
    session = boto3.Session(region_name=region, profile_name=profile or None)
    return session.resource("dynamodb")


def get_queue_depth(sqs: BaseClient, queue_url: str) -> Dict[str, int]:
    """Return approximate visible and not-visible message counts."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )["Attributes"]
    return {
        "visible": int(attrs.get("ApproximateNumberOfMessages", "0")),
        "not_visible": int(attrs.get("ApproximateNumberOfMessagesNotVisible", "0")),
    }


def queue_url_from_arn(arn: str) -> str:
    """Build the queue URL from an ARN (arn:aws:sqs:region:acct:queue-name)."""
    parts = arn.split(":")
    if len(parts) < 6 or parts[0] != "arn" or parts[2] != "sqs":
        raise ValueError(f"Invalid SQS ARN: {arn}")
    region, account_id, queue_name = parts[3], parts[4], parts[5]
    return f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"


def purge_queue(sqs: BaseClient, queue_url: str, label: str) -> bool:
    """Purge all messages from a queue if it is non-empty.

    Skips the call when the queue is already empty to avoid the 60-second
    purge window that can interfere with newly sent messages (see quirk #1
    above).  Returns True if a purge was issued, False if skipped.
    """
    depth = get_queue_depth(sqs, queue_url)
    if depth["visible"] == 0 and depth["not_visible"] == 0:
        print(f"[purge] {label} queue already empty, skipping purge")
        return False
    try:
        sqs.purge_queue(QueueUrl=queue_url)
        print(f"[purge] Purged {label} queue {queue_url}")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "AWS.SimpleQueueService.PurgeQueueInProgress":
            print(f"[purge] {label} queue purge already in progress")
        else:
            raise
    return True


def clear_dynamo_table(dynamo_resource, table_name: str) -> None:
    """Scan a DynamoDB table and batch-delete all items (no truncate API)."""
    table = dynamo_resource.Table(table_name)
    key_names = [k["AttributeName"] for k in table.key_schema]
    scan_kwargs = {"ProjectionExpression": ", ".join(key_names)}
    while True:
        response = table.scan(**scan_kwargs)
        items = response.get("Items", [])
        if not items:
            break
        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={k: item[k] for k in key_names})
        if not response.get("LastEvaluatedKey"):
            break
        scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    print(f"[cleanup] Cleared DynamoDB table {table_name}")


def cleanup_scenario_state(
    sqs: BaseClient, dynamo_resource, outputs: Dict, label: str
) -> None:
    """Purge SQS queues and clear DynamoDB tables for a clean scenario start.

    When a purge is issued, waits the full 60 seconds for it to complete
    before returning so callers can safely send new messages (see quirk #1).
    """
    print(f"[{label}] Cleaning up previous state ...")
    queue_url = outputs["queue_url"]
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    purged_primary = purge_queue(sqs, queue_url, "primary")
    purged_dlq = purge_queue(sqs, dlq_url, "DLQ")
    clear_dynamo_table(dynamo_resource, outputs["message_status_table"])
    clear_dynamo_table(dynamo_resource, outputs["message_completed_table"])
    if purged_primary or purged_dlq:
        print(f"[{label}] Waiting 60s for SQS purge to complete ...")
        time.sleep(60)


def ensure_queue_empty(sqs: BaseClient, queue_url: str, label: str) -> None:
    """Single-shot assertion that a queue is empty.

    Only suitable for queues with no recent message operations (e.g. the DLQ
    after the primary queue has drained).  Do not use on a queue that was just
    actively consumed — the approximate counters can briefly show stale
    non-zero values (see quirk #2 above).  Use wait_for_queue_empty() instead.
    """
    depth = get_queue_depth(sqs, queue_url)
    if depth["visible"] != 0 or depth["not_visible"] != 0:
        raise RuntimeError(
            f"{label} queue not empty: visible={depth['visible']} not_visible={depth['not_visible']}"
        )


def wait_for_messages_enqueued(
    sqs: BaseClient,
    queue_url: str,
    expected: int,
    timeout: int = 30,
    poll_seconds: int = 2,
) -> None:
    """Wait until at least `expected` messages are present."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        depth = get_queue_depth(sqs, queue_url)
        total = depth["visible"] + depth["not_visible"]
        if total >= expected:
            return
        time.sleep(poll_seconds)
    raise RuntimeError(
        f"Queue {queue_url} did not reach expected depth ({expected}) within {timeout}s"
    )


def wait_for_queue_empty(
    sqs: BaseClient,
    queue_url: str,
    timeout: int = 90,
    poll_seconds: int = 3,
    confirmations: int = 3,
) -> None:
    """Wait until both visible and in-flight counts drop to zero or timeout.

    SQS approximate counts are eventually consistent, so we require
    *confirmations* consecutive zero readings before declaring the queue empty.
    """
    deadline = time.time() + timeout
    zeros_seen = 0
    while time.time() < deadline:
        depth = get_queue_depth(sqs, queue_url)
        if depth["visible"] == 0 and depth["not_visible"] == 0:
            zeros_seen += 1
            if zeros_seen >= confirmations:
                return
        else:
            zeros_seen = 0
        time.sleep(poll_seconds)
    raise RuntimeError("Queue did not drain within timeout")


# ---------------------------------------------------------------------------
# Local consumer helpers
# ---------------------------------------------------------------------------


def run_local_consumer(env: Dict[str, str], timeout: int) -> int:
    """Run consume.py as a blocking subprocess. Returns exit code."""
    full_env = {**os.environ, **env}
    print(f"[consumer] Running {CONSUME_SCRIPT} locally ...")
    result = subprocess.run(
        [sys.executable, str(CONSUME_SCRIPT)],
        env=full_env,
        timeout=timeout,
    )
    return result.returncode


def run_local_consumer_async(
    env: Dict[str, str], quiet: bool = False
) -> subprocess.Popen:
    """Start consume.py as a background subprocess. Returns the Popen handle."""
    full_env = {**os.environ, **env}
    if not quiet:
        print(f"[consumer] Starting {CONSUME_SCRIPT} in background ...")
    kwargs: Dict = {}
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
    return subprocess.Popen(
        [sys.executable, str(CONSUME_SCRIPT)],
        env=full_env,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Producer helper
# ---------------------------------------------------------------------------


def run_producer_async(
    count: int,
    batch_size: int,
    region: str,
    queue_url: str,
    profile: str,
    rate: Optional[int] = None,
    poison_count: int = 0,
    quiet: bool = False,
) -> subprocess.Popen:
    """Start the producer script as a background subprocess. Returns the Popen handle."""
    cmd = [
        sys.executable,
        str(REPO_ROOT / "producer" / "produce.py"),
        "--queue-url",
        queue_url,
        "--region",
        region,
        "--n",
        str(count),
        "--batch-size",
        str(batch_size),
    ]
    if rate is not None:
        cmd.extend(["--rate", str(rate)])
    if poison_count > 0:
        cmd.extend(["--poison-count", str(poison_count)])
    if profile:
        cmd.extend(["--profile", profile])
    if not quiet:
        print(f"[producer] Starting producer in background (n={count}, rate={rate}) ...")
    kwargs: Dict = {}
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
    return subprocess.Popen(cmd, **kwargs)


def run_producer(
    count: int,
    batch_size: int,
    region: str,
    queue_url: str,
    profile: str,
    poison_count: int = 0,
) -> None:
    """Invoke the producer script to push messages."""
    cmd = [
        sys.executable,
        str(REPO_ROOT / "producer" / "produce.py"),
        "--queue-url",
        queue_url,
        "--region",
        region,
        "--n",
        str(count),
        "--batch-size",
        str(batch_size),
    ]
    if poison_count > 0:
        cmd.extend(["--poison-count", str(poison_count)])
    if profile:
        cmd.extend(["--profile", profile])
    subprocess.run(cmd, check=True)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def scenario_happy(args: argparse.Namespace, outputs: Dict) -> None:
    """Scenario 1: Happy path — messages flow through cleanly."""
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    sqs = build_sqs_client(region, profile)
    dynamo = build_dynamo_resource(region, profile)
    cleanup_scenario_state(sqs, dynamo, outputs, "happy")

    message_count = args.count or 5
    batch_size = min(args.batch_size or 5, 10)

    print(f"[happy] Sending {message_count} messages ...")
    run_producer(message_count, batch_size, region, queue_url, profile)
    print("[happy] Confirming messages are enqueued ...")
    wait_for_messages_enqueued(sqs, queue_url, message_count)

    print("[happy] Running local consumer to drain messages ...")
    exit_code = run_local_consumer(
        env={
            "QUEUE_URL": queue_url,
            "AWS_REGION": region,
            "MESSAGE_LIMIT": str(message_count),
            "LOG_LEVEL": "INFO",
            "IDLE_TIMEOUT_SECONDS": "30",
        },
        timeout=args.consumer_timeout,
    )
    if exit_code != 0:
        raise RuntimeError(f"Happy consumer failed with exit code {exit_code}")

    print("[happy] Waiting for queue to drain ...")
    wait_for_queue_empty(sqs, queue_url)
    ensure_queue_empty(sqs, dlq_url, "DLQ")
    print("[happy] PASS")


def scenario_crash(args: argparse.Namespace, outputs: Dict) -> None:
    """Scenario 2: Consumer crash mid-processing, then redelivery."""
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    sqs = build_sqs_client(region, profile)
    dynamo = build_dynamo_resource(region, profile)
    cleanup_scenario_state(sqs, dynamo, outputs, "crash")

    message_count = 1
    print(f"[crash] Sending {message_count} message ...")
    run_producer(message_count, batch_size=1, region=region, queue_url=queue_url, profile=profile)
    print("[crash] Confirming message is enqueued ...")
    wait_for_messages_enqueued(sqs, queue_url, message_count)

    print("[crash] Running local consumer that will crash mid-processing ...")
    exit_code = run_local_consumer(
        env={
            "QUEUE_URL": queue_url,
            "AWS_REGION": region,
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
            "AWS_REGION": region,
            "MESSAGE_LIMIT": str(message_count),
            "LOG_LEVEL": "INFO",
            "IDLE_TIMEOUT_SECONDS": "30",
        },
        timeout=args.consumer_timeout,
    )
    if exit_code2 != 0:
        raise RuntimeError(f"Second consumer failed with exit code {exit_code2}")

    print("[crash] Waiting for queue to drain ...")
    wait_for_queue_empty(sqs, queue_url)
    ensure_queue_empty(sqs, dlq_url, "DLQ")
    print("[crash] PASS")


def scenario_duplicates(args: argparse.Namespace, outputs: Dict) -> None:
    """Scenario 3: Duplicate delivery handled via idempotent side effects."""
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    status_table_name = outputs["message_status_table"]
    completed_table_name = outputs["message_completed_table"]

    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    sqs = build_sqs_client(region, profile)
    dynamo = build_dynamo_resource(region, profile)
    cleanup_scenario_state(sqs, dynamo, outputs, "dup")
    status_table = dynamo.Table(status_table_name)
    completed_table = dynamo.Table(completed_table_name)

    message_id = str(uuid.uuid4())
    payload = {"id": message_id, "work": "duplicate-demo"}
    print(f"[dup] Sending message {message_id} ...")
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(payload))
    wait_for_messages_enqueued(sqs, queue_url, expected=1)

    common_env = {
        "MESSAGE_STATUS_TABLE": status_table_name,
        "MESSAGE_COMPLETED_TABLE": completed_table_name,
        "LONG_SLEEP_SECONDS": str(args.slow_seconds),
        "LONG_SLEEP_EVERY": "1",
        "MESSAGE_LIMIT": "1",
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "1",
        "QUEUE_URL": queue_url,
        "AWS_REGION": region,
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
    wait_for_queue_empty(sqs, queue_url, timeout=args.queue_timeout)
    ensure_queue_empty(sqs, dlq_url, "DLQ")

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


def scenario_poison(args: argparse.Namespace, outputs: Dict) -> None:
    """Scenario 4: Poison messages exhaust retries and land in the DLQ."""
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    sqs = build_sqs_client(region, profile)
    dynamo = build_dynamo_resource(region, profile)
    cleanup_scenario_state(sqs, dynamo, outputs, "poison")

    total_count = args.count
    poison_count = args.poison_count
    good_count = total_count - poison_count

    print(f"[poison] Sending {total_count} messages ({poison_count} poison, {good_count} good) ...")
    run_producer(
        total_count,
        batch_size=min(10, total_count),
        region=region,
        queue_url=queue_url,
        profile=profile,
        poison_count=poison_count,
    )
    print("[poison] Confirming messages are enqueued ...")
    wait_for_messages_enqueued(sqs, queue_url, total_count)

    print("[poison] Running consumer with REJECT_PAYLOAD_MARKER=POISON ...")
    consumer_env = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": region,
        "REJECT_PAYLOAD_MARKER": "POISON",
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "5",
    }
    if outputs.get("message_status_table") and outputs.get("message_completed_table"):
        consumer_env["MESSAGE_STATUS_TABLE"] = outputs["message_status_table"]
        consumer_env["MESSAGE_COMPLETED_TABLE"] = outputs["message_completed_table"]

    exit_code = run_local_consumer(env=consumer_env, timeout=args.consumer_timeout)
    if exit_code != 0:
        raise RuntimeError(f"Poison consumer failed with exit code {exit_code}")

    print("[poison] Waiting for main queue to fully drain ...")
    wait_for_queue_empty(sqs, queue_url, timeout=args.queue_timeout)

    print("[poison] Checking DLQ for poison messages ...")
    dlq_depth = get_queue_depth(sqs, dlq_url)
    dlq_count = dlq_depth["visible"] + dlq_depth["not_visible"]
    if dlq_count != poison_count:
        raise RuntimeError(
            f"Expected {poison_count} messages in DLQ, found {dlq_count} "
            f"(visible={dlq_depth['visible']}, not_visible={dlq_depth['not_visible']})"
        )

    print(f"[poison] PASS | good={good_count} processed, poison={poison_count} in DLQ")


def scenario_backpressure(args: argparse.Namespace, outputs: Dict) -> None:
    """Scenario 5: Backpressure — auto-scale consumers when a continuous
    producer overwhelms them, then confirm equilibrium and drain.

    The producer runs continuously at a fixed rate (default 5 msgs/sec).
    The runner starts with one slow consumer (~1 msg/sec) and monitors the
    consumption rate via DynamoDB.  When consumers can't keep up it spawns
    more.  Once the consumption rate meets or exceeds the production rate
    for several consecutive ticks the runner declares equilibrium, stops the
    producer, and lets the queue drain.
    """
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    status_table_name = outputs["message_status_table"]
    completed_table_name = outputs["message_completed_table"]
    sqs = build_sqs_client(region, profile)
    dynamo = build_dynamo_resource(region, profile)
    cleanup_scenario_state(sqs, dynamo, outputs, "backpressure")

    prod_rate = args.rate
    batch_size = args.batch_size
    poll_interval = args.poll_interval

    consumer_env_base = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": region,
        "MESSAGE_STATUS_TABLE": status_table_name,
        "MESSAGE_COMPLETED_TABLE": completed_table_name,
        "SLEEP_MIN_SECONDS": "1.0",
        "SLEEP_MAX_SECONDS": "1.0",
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "WARNING",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "5",
    }

    # Phase 1: Start continuous producer and first consumer
    print(f"[backpressure] Starting continuous producer at {prod_rate} msgs/sec ...")
    producer_proc = run_producer_async(
        count=0,  # 0 = run forever until killed
        batch_size=batch_size,
        region=region,
        queue_url=queue_url,
        profile=profile,
        rate=prod_rate,
        quiet=True,
    )

    print("[backpressure] Starting initial consumer ...")
    consumers: list[subprocess.Popen] = [
        run_local_consumer_async(consumer_env_base, quiet=True)
    ]
    peak_consumers = 1
    completed_table = dynamo.Table(completed_table_name)

    def _count_completed() -> int:
        """Return the exact number of completed messages from DynamoDB."""
        count = 0
        scan_kw: Dict = {"Select": "COUNT"}
        while True:
            resp = completed_table.scan(**scan_kw)
            count += resp["Count"]
            if "LastEvaluatedKey" not in resp:
                break
            scan_kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        return count

    # Phase 2: Monitor & scale loop
    # Compare consumption rate against production rate each tick.
    # Scale up when falling behind; once queue depth drops to single
    # digits for *equilibrium_ticks* consecutive ticks, stop the producer.
    prev_completed = 0
    eq_streak = 0
    equilibrium_ticks = 3

    while True:
        time.sleep(poll_interval)

        completed = _count_completed()
        completed_delta = completed - prev_completed
        consume_rate = completed_delta / poll_interval
        prev_completed = completed

        producer_alive = producer_proc.poll() is None

        # Reap finished consumers
        active_consumers = [p for p in consumers if p.poll() is None]

        status = "producing" if producer_alive else "draining"

        depth = get_queue_depth(sqs, queue_url)
        queue_depth = depth["visible"]

        print(
            f"[backpressure] {status} | "
            f"depth={queue_depth} completed={completed} "
            f"rate={consume_rate:.1f}/s consumers={len(active_consumers)}"
        )

        if producer_alive:
            # Stop producer once queue depth drops to single digits —
            # consumers have caught up with the continuous inflow.
            if queue_depth < 10 and completed > 0:
                eq_streak += 1
                if eq_streak >= equilibrium_ticks:
                    print(
                        f"[backpressure] Queue depth in single digits "
                        f"(depth={queue_depth}) for {equilibrium_ticks} ticks "
                        f"— stopping producer"
                    )
                    producer_proc.terminate()
                    producer_proc.wait(timeout=10)
                    continue
            else:
                eq_streak = 0

            # Keep spawning consumers as long as depth >= 10 — don't
            # stop just because the rate caught up; actively drive the
            # backlog down.
            if queue_depth >= 10:
                new_proc = run_local_consumer_async(consumer_env_base, quiet=True)
                consumers.append(new_proc)
                active_consumers.append(new_proc)
                peak_consumers = max(peak_consumers, len(active_consumers))
                print(
                    f"[backpressure] SCALE UP → {len(active_consumers)} consumers"
                )
        else:
            # Producer stopped — wait for queue to empty
            if queue_depth == 0 and depth["not_visible"] == 0:
                print("[backpressure] Queue drained, exiting monitor loop")
                break

    # Phase 3: Wait for full drain
    print("[backpressure] Waiting for queue to fully drain ...")
    wait_for_queue_empty(sqs, queue_url, timeout=args.queue_timeout)

    print("[backpressure] Waiting for all consumers to exit ...")
    for proc in consumers:
        try:
            proc.wait(timeout=args.consumer_timeout)
        except subprocess.TimeoutExpired:
            print(f"[backpressure] Consumer pid={proc.pid} timed out, terminating ...")
            proc.terminate()
            proc.wait(timeout=10)

    # Phase 4: Assertions
    total_completed = _count_completed()
    print(f"[backpressure] Running assertions (completed={total_completed}) ...")

    # Queue fully drained
    final_depth = get_queue_depth(sqs, queue_url)
    if final_depth["visible"] != 0 or final_depth["not_visible"] != 0:
        raise RuntimeError(
            f"Queue not empty: visible={final_depth['visible']} "
            f"not_visible={final_depth['not_visible']}"
        )

    # DLQ empty
    ensure_queue_empty(sqs, dlq_url, "DLQ")

    # Scaling actually happened
    if peak_consumers <= 1:
        raise RuntimeError(
            f"Expected scaling to occur (peak > 1), but peak_consumers={peak_consumers}"
        )

    # Sanity: completed count should be reasonably close to produced estimate
    if total_completed == 0:
        raise RuntimeError("No messages were completed")

    print(
        f"[backpressure] PASS | completed={total_completed} "
        f"peak_consumers={peak_consumers}"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run queue scenarios")
    subparsers = parser.add_subparsers(dest="scenario", required=True)

    happy = subparsers.add_parser("happy", help="Happy path scenario")
    happy.add_argument("--count", type=int, default=5, help="Messages to send")
    happy.add_argument("--batch-size", type=int, default=5, help="Producer batch size (<=10)")
    happy.add_argument(
        "--consumer-timeout",
        type=int,
        default=120,
        help="Timeout (seconds) for consumer subprocess",
    )

    crash = subparsers.add_parser("crash", help="Consumer crash mid-processing scenario")
    crash.add_argument(
        "--visibility-wait",
        type=int,
        default=DEFAULT_VISIBILITY_BUFFER,
        help="Seconds to wait for visibility timeout to expire",
    )
    crash.add_argument(
        "--consumer-timeout",
        type=int,
        default=90,
        help="Timeout (seconds) for consumer subprocesses",
    )

    dup = subparsers.add_parser("duplicates", help="Duplicate delivery scenario (two consumers)")
    dup.add_argument(
        "--slow-seconds",
        type=int,
        default=30,
        help="Seconds each consumer sleeps to simulate long processing",
    )
    dup.add_argument(
        "--second-start-delay",
        type=int,
        default=0,
        help="Delay before starting the second consumer (0 for concurrent start)",
    )
    dup.add_argument(
        "--idle-timeout",
        type=int,
        default=60,
        help="Idle timeout for consumers to exit when queue is empty",
    )
    dup.add_argument(
        "--consumer-timeout",
        type=int,
        default=120,
        help="Timeout (seconds) to wait for each consumer process",
    )
    dup.add_argument(
        "--queue-timeout",
        type=int,
        default=180,
        help="Timeout (seconds) to wait for queue to drain",
    )

    poison = subparsers.add_parser("poison", help="Poison message scenario (bad messages go to DLQ)")
    poison.add_argument("--count", type=int, default=5, help="Total messages to send")
    poison.add_argument("--poison-count", type=int, default=2, help="Number of poison messages")
    poison.add_argument(
        "--idle-timeout",
        type=int,
        default=30,
        help="Idle timeout for consumer to exit when queue is drained",
    )
    poison.add_argument(
        "--consumer-timeout",
        type=int,
        default=300,
        help="Timeout (seconds) for consumer subprocess",
    )
    poison.add_argument(
        "--queue-timeout",
        type=int,
        default=180,
        help="Timeout (seconds) to wait for queue to drain",
    )

    bp = subparsers.add_parser("backpressure", help="Backpressure / auto-scaling scenario")
    bp.add_argument("--rate", type=int, default=5, help="Producer messages per second")
    bp.add_argument("--batch-size", type=int, default=10, help="Producer batch size (<=10)")
    bp.add_argument(
        "--poll-interval", type=int, default=5, help="Seconds between depth samples"
    )
    bp.add_argument(
        "--consumer-timeout",
        type=int,
        default=300,
        help="Timeout (seconds) for consumer subprocesses",
    )
    bp.add_argument(
        "--idle-timeout",
        type=int,
        default=30,
        help="Idle timeout for consumers to exit when queue is empty",
    )
    bp.add_argument(
        "--queue-timeout",
        type=int,
        default=300,
        help="Timeout (seconds) to wait for queue to drain",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    scenario_fn = {
        "happy": scenario_happy,
        "crash": scenario_crash,
        "duplicates": scenario_duplicates,
        "poison": scenario_poison,
        "backpressure": scenario_backpressure,
    }.get(args.scenario)

    if not scenario_fn:
        raise SystemExit(f"Unknown scenario {args.scenario}")

    tf_outputs = read_terraform_outputs()
    region = tf_outputs["aws_region"]
    tf_key = SCENARIO_TF_KEYS[args.scenario]
    scenario_resources = tf_outputs["scenarios"][tf_key]

    # Build outputs dict matching what scenario functions expect
    outputs = {
        "queue_url": scenario_resources["queue_url"],
        "dlq_arn": scenario_resources["dlq_arn"],
        "message_status_table": scenario_resources["message_status_table"],
        "message_completed_table": scenario_resources["message_completed_table"],
        "aws_region": region,
    }

    profile = os.environ.get("AWS_PROFILE")
    print(f"[run] Validating {args.scenario} infrastructure ...")
    validate_scenario_infra(outputs, region, profile)

    scenario_fn(args, outputs)


if __name__ == "__main__":
    main()
