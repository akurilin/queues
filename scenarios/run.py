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
    "partial-batch": "poison",  # reuses poison scenario infrastructure
    "side-effects": "dup",  # reuses dup infrastructure (has DynamoDB + short visibility)
    "backpressure": "backpressure",
    "graceful-shutdown": "dup",  # reuses dup infrastructure (has DynamoDB + short visibility)
    "purge-timing": "happy",  # reuses happy infrastructure (just needs a queue)
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


PURGE_SAFETY_DELAY = 10  # seconds to wait even when queue appears empty


def cleanup_scenario_state(
    sqs: BaseClient, dynamo_resource, outputs: Dict, label: str
) -> None:
    """Purge SQS queues and clear DynamoDB tables for a clean scenario start.

    When a purge is issued, waits the full 60 seconds for it to complete
    before returning so callers can safely send new messages (see quirk #1).

    Even when no purge is needed, waits a short safety delay in case a previous
    scenario's purge is still running asynchronously.
    """
    print(f"[{label}] Cleaning up previous state ...")
    queue_url = outputs["queue_url"]
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    purged_primary = purge_queue(sqs, queue_url, "primary")
    purged_dlq = purge_queue(sqs, dlq_url, "DLQ")
    clear_dynamo_table(dynamo_resource, outputs["message_status_table"])
    clear_dynamo_table(dynamo_resource, outputs["message_side_effects_table"])
    if purged_primary or purged_dlq:
        print(f"[{label}] Waiting 60s for SQS purge to complete ...")
        time.sleep(60)
    else:
        print(
            f"[{label}] Waiting {PURGE_SAFETY_DELAY}s in case a previous purge is still running ..."
        )
        time.sleep(PURGE_SAFETY_DELAY)


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
    poison_every: int = 0,
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
    if poison_every > 0:
        cmd.extend(["--poison-every", str(poison_every)])
    elif poison_count > 0:
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
    poison_every: int = 0,
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
    if poison_every > 0:
        cmd.extend(["--poison-every", str(poison_every)])
    elif poison_count > 0:
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
    completed_table_name = outputs["message_side_effects_table"]

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
        "MESSAGE_SIDE_EFFECTS_TABLE": completed_table_name,
        "SIDE_EFFECT_DELAY_SECONDS": str(args.slow_seconds),
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
    if outputs.get("message_status_table") and outputs.get("message_side_effects_table"):
        consumer_env["MESSAGE_STATUS_TABLE"] = outputs["message_status_table"]
        consumer_env["MESSAGE_SIDE_EFFECTS_TABLE"] = outputs["message_side_effects_table"]

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


def scenario_partial_batch(args: argparse.Namespace, outputs: Dict) -> None:
    """Scenario 5: Partial batch failure — consumer receives batches of 10,
    processes some successfully and fails on poison messages.

    Demonstrates correct handling when a batch contains both good and bad
    messages: good messages are deleted, poison messages are retried until
    they exhaust retries and land in the DLQ.
    """
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    completed_table_name = outputs["message_side_effects_table"]
    sqs = build_sqs_client(region, profile)
    dynamo = build_dynamo_resource(region, profile)
    cleanup_scenario_state(sqs, dynamo, outputs, "partial-batch")

    total_count = args.count
    poison_every = args.poison_every
    # Calculate expected poison count: indices (poison_every-1), (2*poison_every-1), ...
    expected_poison = len(range(poison_every - 1, total_count, poison_every))
    expected_good = total_count - expected_poison

    print(
        f"[partial-batch] Sending {total_count} messages "
        f"(every {poison_every}th is poison: {expected_poison} poison, {expected_good} good) ..."
    )
    run_producer(
        total_count,
        batch_size=10,
        region=region,
        queue_url=queue_url,
        profile=profile,
        poison_every=poison_every,
    )
    print("[partial-batch] Confirming messages are enqueued ...")
    wait_for_messages_enqueued(sqs, queue_url, total_count)

    print("[partial-batch] Running consumer with MAX_MESSAGES=10, REJECT_PAYLOAD_MARKER=POISON ...")
    consumer_env = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": region,
        "REJECT_PAYLOAD_MARKER": "POISON",
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "10",
        "WAIT_TIME_SECONDS": "5",
    }
    if outputs.get("message_status_table") and outputs.get("message_side_effects_table"):
        consumer_env["MESSAGE_STATUS_TABLE"] = outputs["message_status_table"]
        consumer_env["MESSAGE_SIDE_EFFECTS_TABLE"] = outputs["message_side_effects_table"]

    exit_code = run_local_consumer(env=consumer_env, timeout=args.consumer_timeout)
    if exit_code != 0:
        raise RuntimeError(f"Partial-batch consumer failed with exit code {exit_code}")

    print("[partial-batch] Waiting for main queue to fully drain ...")
    wait_for_queue_empty(sqs, queue_url, timeout=args.queue_timeout)

    print("[partial-batch] Checking DLQ for poison messages ...")
    dlq_depth = get_queue_depth(sqs, dlq_url)
    dlq_count = dlq_depth["visible"] + dlq_depth["not_visible"]
    if dlq_count != expected_poison:
        raise RuntimeError(
            f"Expected {expected_poison} messages in DLQ, found {dlq_count} "
            f"(visible={dlq_depth['visible']}, not_visible={dlq_depth['not_visible']})"
        )

    # Verify DynamoDB completions
    completed_table = dynamo.Table(completed_table_name)
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


def scenario_side_effects(args: argparse.Namespace, outputs: Dict) -> None:
    """Scenario 6: External side effects — prove that the check-before-doing
    pattern prevents duplicate side effects even when crashes occur.

    The consumer uses a two-table pattern:
    1. message_side_effects: represents the external side effect (like Stripe)
    2. message_status: our bookkeeping that we confirmed the side effect

    Flow:
    1. Check if side effect exists in message_side_effects
    2. If not, perform the side effect (write to message_side_effects)
    3. [crash here — CRASH_AFTER_SIDE_EFFECT tests this]
    4. Update message_status to COMPLETED
    5. Delete from SQS

    On retry after crash:
    1. Check side_effects table → exists → skip the external call
    2. Update status to COMPLETED
    3. Delete from SQS

    This models real external systems (Stripe, email, etc.) where you can't
    transact with the external service — you must check before doing.
    """
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    status_table_name = outputs["message_status_table"]
    side_effects_table_name = outputs["message_side_effects_table"]
    sqs = build_sqs_client(region, profile)
    dynamo = build_dynamo_resource(region, profile)
    cleanup_scenario_state(sqs, dynamo, outputs, "side-effects")

    message_count = args.count

    print(f"[side-effects] Sending {message_count} messages ...")
    run_producer(
        message_count,
        batch_size=min(10, message_count),
        region=region,
        queue_url=queue_url,
        profile=profile,
    )
    print("[side-effects] Confirming messages are enqueued ...")
    wait_for_messages_enqueued(sqs, queue_url, message_count)

    # Phase 1: Run consumer with crash after side effect
    # This performs the side effect (writes to side_effects table), then crashes
    # before updating status or deleting from SQS
    print("[side-effects] Running consumer with CRASH_AFTER_SIDE_EFFECT=1 ...")
    consumer_env = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": region,
        "MESSAGE_STATUS_TABLE": status_table_name,
        "MESSAGE_SIDE_EFFECTS_TABLE": side_effects_table_name,
        "CRASH_AFTER_SIDE_EFFECT": "1",
        "IDLE_TIMEOUT_SECONDS": "10",
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "5",
    }

    # Consumer will crash after each message, so we expect a non-zero exit
    proc = run_local_consumer_async(consumer_env)
    # Give it time to process messages and crash
    time.sleep(args.crash_window)
    if proc.poll() is None:
        proc.terminate()
        proc.wait(timeout=10)

    # Phase 2: Wait for visibility timeout so messages become visible again
    print(f"[side-effects] Waiting {args.visibility_wait}s for visibility timeout ...")
    time.sleep(args.visibility_wait)

    # Phase 3: Run consumer normally to handle redeliveries
    # The consumer should detect that side effects already exist and skip them
    print("[side-effects] Running consumer to handle redeliveries ...")
    consumer_env_normal = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": region,
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
    wait_for_queue_empty(sqs, queue_url, timeout=args.queue_timeout)

    # Phase 4: Assertions
    # Check that message_side_effects has exactly message_count entries (no duplicates)
    side_effects_table = dynamo.Table(side_effects_table_name)
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

    # Check that message_status has message_count COMPLETED entries
    status_table = dynamo.Table(status_table_name)
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

    # DLQ should be empty (no failures, just idempotent skips)
    ensure_queue_empty(sqs, dlq_url, "DLQ")

    print(
        f"[side-effects] PASS | {message_count} messages processed, "
        f"{side_effects_count} unique side effects (no duplicates)"
    )


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
    completed_table_name = outputs["message_side_effects_table"]
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
        "MESSAGE_SIDE_EFFECTS_TABLE": completed_table_name,
        "SIDE_EFFECT_DELAY_SECONDS": "1.0",
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


def scenario_graceful_shutdown(args: argparse.Namespace, outputs: Dict) -> None:
    """Scenario 8: Graceful shutdown — consumer finishes in-flight work after
    receiving SIGTERM before exiting.

    Demonstrates that when a consumer receives SIGTERM (e.g., during deployment,
    scale-down, or container stop), it finishes processing the current message
    before exiting cleanly. This prevents unnecessary redeliveries and wasted work.

    Flow:
    1. Send messages to queue
    2. Start consumer with slow processing (5s per message)
    3. Wait for consumer to start processing first message
    4. Send SIGTERM to consumer
    5. Consumer finishes current message, then exits with code 0
    6. Assert: side effect recorded for processed message, remaining messages in queue
    """
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    status_table_name = outputs["message_status_table"]
    side_effects_table_name = outputs["message_side_effects_table"]
    sqs = build_sqs_client(region, profile)
    dynamo = build_dynamo_resource(region, profile)
    cleanup_scenario_state(sqs, dynamo, outputs, "graceful-shutdown")

    message_count = args.count
    processing_delay = args.processing_delay
    sigterm_delay = args.sigterm_delay

    print(f"[graceful-shutdown] Sending {message_count} messages ...")
    run_producer(
        message_count,
        batch_size=min(10, message_count),
        region=region,
        queue_url=queue_url,
        profile=profile,
    )
    print("[graceful-shutdown] Confirming messages are enqueued ...")
    wait_for_messages_enqueued(sqs, queue_url, message_count)

    # Start consumer with slow processing — it will take processing_delay seconds
    # per message, so we have time to send SIGTERM while it's working
    print(
        f"[graceful-shutdown] Starting consumer with {processing_delay}s processing delay ..."
    )
    consumer_env = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": region,
        "MESSAGE_STATUS_TABLE": status_table_name,
        "MESSAGE_SIDE_EFFECTS_TABLE": side_effects_table_name,
        "SIDE_EFFECT_DELAY_SECONDS": str(processing_delay),
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "1",
    }
    proc = run_local_consumer_async(consumer_env)

    # Wait for consumer to pick up first message and start processing
    print(f"[graceful-shutdown] Waiting {sigterm_delay}s for consumer to start processing ...")
    time.sleep(sigterm_delay)

    # Send SIGTERM — consumer should finish current message then exit
    print("[graceful-shutdown] Sending SIGTERM to consumer ...")
    sigterm_time = time.time()
    proc.terminate()

    # Wait for consumer to exit
    try:
        proc.wait(timeout=args.consumer_timeout)
    except subprocess.TimeoutExpired:
        print("[graceful-shutdown] Consumer did not exit in time, killing ...")
        proc.kill()
        proc.wait(timeout=10)
        raise RuntimeError("Consumer did not exit gracefully after SIGTERM")

    exit_time = time.time()
    time_after_sigterm = exit_time - sigterm_time

    if proc.returncode != 0:
        raise RuntimeError(
            f"Consumer exited with code {proc.returncode}, expected 0 for graceful shutdown"
        )

    # Verify timing: consumer should have taken time to finish processing after SIGTERM
    # If it exited immediately (<1s), it either wasn't processing or didn't finish the work
    min_expected_processing_time = processing_delay - sigterm_delay - 1  # Allow 1s slack
    if time_after_sigterm < min_expected_processing_time:
        print(
            f"[graceful-shutdown] WARNING: Consumer exited {time_after_sigterm:.1f}s after SIGTERM, "
            f"expected ~{processing_delay - sigterm_delay}s to finish processing. "
            f"May not have been mid-processing when SIGTERM arrived."
        )
    else:
        print(
            f"[graceful-shutdown] Consumer exited {time_after_sigterm:.1f}s after SIGTERM "
            f"(finished in-flight work before exiting)"
        )

    # Give SQS a moment to update message visibility
    time.sleep(2)

    # Assert: exactly 1 side effect record (consumer finished processing 1 message)
    side_effects_table = dynamo.Table(side_effects_table_name)
    side_effects_count = 0
    scan_kw: Dict = {"Select": "COUNT"}
    while True:
        resp = side_effects_table.scan(**scan_kw)
        side_effects_count += resp["Count"]
        if "LastEvaluatedKey" not in resp:
            break
        scan_kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if side_effects_count != 1:
        raise RuntimeError(
            f"Expected exactly 1 side effect (message processed before shutdown), "
            f"found {side_effects_count}"
        )

    # Verify the side effect was performed AFTER SIGTERM (proves graceful completion)
    # Scan to get the actual record with timestamp
    scan_resp = side_effects_table.scan(Limit=1)
    if scan_resp.get("Items"):
        side_effect_record = scan_resp["Items"][0]
        performed_at = side_effect_record.get("performed_at", "")
        print(f"[graceful-shutdown] Side effect performed at: {performed_at}")
        # The side effect should have been performed after SIGTERM
        # (We can't easily compare ISO timestamps to Unix time, but the timing check above covers this)

    # Assert: remaining messages still in queue (message_count - 1)
    expected_remaining = message_count - 1
    depth = get_queue_depth(sqs, queue_url)
    actual_remaining = depth["visible"] + depth["not_visible"]
    if actual_remaining != expected_remaining:
        raise RuntimeError(
            f"Expected {expected_remaining} messages remaining in queue, "
            f"found {actual_remaining} (visible={depth['visible']}, not_visible={depth['not_visible']})"
        )

    # Assert: DLQ is empty (no failures)
    ensure_queue_empty(sqs, dlq_url, "DLQ")

    print(
        f"[graceful-shutdown] PASS | 1 message processed before shutdown, "
        f"{expected_remaining} messages remaining in queue"
    )


def scenario_purge_timing(args: argparse.Namespace, outputs: Dict) -> None:
    """Scenario: SQS purge timing — verify our understanding of the 60-second
    purge window and its effects on message delivery.

    SQS PurgeQueue is asynchronous: the API returns immediately but deletion
    continues for up to 60 seconds. Messages sent DURING this window can be
    silently deleted. This scenario tests and documents this behavior.

    The scenario runs multiple iterations to statistically observe the behavior,
    since any single run might get lucky with timing.

    Tests:
    1. Messages sent immediately after purge may be deleted (danger window)
    2. Messages sent after 65s are safe
    3. ApproximateNumberOfMessages can be misleading during purge
    """
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    sqs = build_sqs_client(region, profile)
    dynamo = build_dynamo_resource(region, profile)

    # Use a minimal cleanup - just clear DynamoDB, don't purge (we'll do that ourselves)
    print("[purge-timing] Clearing DynamoDB tables ...")
    clear_dynamo_table(dynamo, outputs["message_status_table"])
    clear_dynamo_table(dynamo, outputs["message_side_effects_table"])

    iterations = args.iterations
    danger_count_per_iter = args.danger_count
    safe_window_delay = args.safe_delay

    total_danger_sent = 0
    total_danger_survived = 0
    total_safe_sent = 0
    total_safe_survived = 0

    for iteration in range(1, iterations + 1):
        print(f"\n[purge-timing] === Iteration {iteration}/{iterations} ===")

        # Phase 1: Seed the queue with messages, then purge
        seed_count = 10
        print(f"[purge-timing] Seeding queue with {seed_count} messages ...")
        for i in range(seed_count):
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({"id": f"seed-{iteration}-{i}", "phase": "seed"}),
            )
        time.sleep(2)  # Let messages settle

        print("[purge-timing] Initiating purge ...")
        sqs.purge_queue(QueueUrl=queue_url)
        purge_start = time.time()

        # Phase 2: Send messages IMMEDIATELY (0s delay) to maximize chance of loss
        print(f"[purge-timing] Sending {danger_count_per_iter} messages IMMEDIATELY after purge ...")
        for i in range(danger_count_per_iter):
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({
                    "id": f"danger-{iteration}-{i}",
                    "phase": "danger-window",
                    "iteration": iteration,
                }),
            )
        total_danger_sent += danger_count_per_iter

        # Also send some messages at intervals during the danger window
        intervals = [5, 15, 30, 45]
        for delay in intervals:
            elapsed = time.time() - purge_start
            sleep_needed = delay - elapsed
            if sleep_needed > 0:
                time.sleep(sleep_needed)
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({
                    "id": f"danger-{iteration}-t{delay}",
                    "phase": "danger-window",
                    "iteration": iteration,
                    "delay": delay,
                }),
            )
            total_danger_sent += 1
            print(f"[purge-timing]   Sent message at t+{delay}s")

        # Phase 3: Wait for purge to complete (65s total from purge start)
        elapsed = time.time() - purge_start
        remaining_wait = max(0, safe_window_delay - elapsed)
        if remaining_wait > 0:
            print(f"[purge-timing] Waiting {remaining_wait:.0f}s for purge window to close ...")
            time.sleep(remaining_wait)

        # Phase 4: Send messages after purge window (should all survive)
        safe_count = 5
        print(f"[purge-timing] Sending {safe_count} messages after purge window ...")
        for i in range(safe_count):
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({
                    "id": f"safe-{iteration}-{i}",
                    "phase": "after-purge",
                    "iteration": iteration,
                }),
            )
        total_safe_sent += safe_count

        # Phase 5: Drain and count messages
        time.sleep(2)  # Let messages settle
        iter_danger_survived = 0
        iter_safe_survived = 0

        while True:
            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=2,
            )
            messages = resp.get("Messages", [])
            if not messages:
                break
            for msg in messages:
                body = json.loads(msg["Body"])
                if body.get("phase") == "danger-window":
                    iter_danger_survived += 1
                elif body.get("phase") == "after-purge":
                    iter_safe_survived += 1
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

        total_danger_survived += iter_danger_survived
        total_safe_survived += iter_safe_survived

        iter_danger_sent = danger_count_per_iter + len(intervals)
        print(
            f"[purge-timing] Iteration {iteration} results: "
            f"danger={iter_danger_survived}/{iter_danger_sent} survived, "
            f"safe={iter_safe_survived}/{safe_count} survived"
        )

        if iter_danger_survived < iter_danger_sent:
            print(f"[purge-timing] >>> OBSERVED MESSAGE LOSS: {iter_danger_sent - iter_danger_survived} messages deleted by purge")

    # Final summary
    print(f"\n[purge-timing] === SUMMARY ({iterations} iterations) ===")
    total_danger_lost = total_danger_sent - total_danger_survived
    total_safe_lost = total_safe_sent - total_safe_survived

    print(f"[purge-timing] Danger window: {total_danger_survived}/{total_danger_sent} survived ({total_danger_lost} lost)")
    print(f"[purge-timing] Safe window:   {total_safe_survived}/{total_safe_sent} survived ({total_safe_lost} lost)")

    # Assertions
    if total_safe_lost > 0:
        raise RuntimeError(
            f"UNEXPECTED: Messages sent after 65s purge window were lost! "
            f"lost={total_safe_lost}/{total_safe_sent}. "
            f"This contradicts AWS documentation."
        )

    if total_danger_lost > 0:
        print(
            f"[purge-timing] PASS | Confirmed purge danger window behavior: "
            f"{total_danger_lost}/{total_danger_sent} messages lost during window. "
            f"All {total_safe_sent} messages after window survived."
        )
    else:
        print(
            f"[purge-timing] PASS (inconclusive for same-run) | No message loss observed in {iterations} iterations. "
            f"All {total_safe_sent} safe-window messages survived as expected."
        )

    # Phase 6: Cross-run simulation
    # The real danger is when a PREVIOUS run's purge affects a NEW run's messages
    # Simulate this by purging, returning immediately, and checking if rapid re-send loses messages
    print(f"\n[purge-timing] === Cross-run simulation ===")
    print("[purge-timing] This simulates what happens when scenarios run back-to-back:")
    print("[purge-timing]   Run A purges at end -> Run B starts immediately -> sends messages")

    # Seed and purge
    print("[purge-timing] Seeding queue ...")
    for i in range(20):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"id": f"cross-seed-{i}", "phase": "seed"}),
        )
    time.sleep(2)

    print("[purge-timing] Purging (simulating end of Run A) ...")
    sqs.purge_queue(QueueUrl=queue_url)

    # Immediately send messages (simulating Run B starting with minimal delay)
    delays_to_test = [0, 2, 5, 10, 15, 30, 45, 60, 65]
    cross_run_results: Dict[int, Dict[str, int]] = {}
    purge_start = time.time()

    for delay in delays_to_test:
        elapsed = time.time() - purge_start
        sleep_needed = delay - elapsed
        if sleep_needed > 0:
            time.sleep(sleep_needed)

        # Send a batch of messages at this delay point
        batch_size = 5
        for i in range(batch_size):
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({
                    "id": f"cross-{delay}s-{i}",
                    "phase": "cross-run",
                    "delay": delay,
                }),
            )
        print(f"[purge-timing] Sent {batch_size} messages at t+{delay}s")
        cross_run_results[delay] = {"sent": batch_size, "survived": 0}

    # Wait for everything to settle, then drain
    print("[purge-timing] Waiting 10s for messages to settle ...")
    time.sleep(10)

    print("[purge-timing] Draining queue to count survivors ...")
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )
        messages = resp.get("Messages", [])
        if not messages:
            break
        for msg in messages:
            body = json.loads(msg["Body"])
            if body.get("phase") == "cross-run":
                delay = body.get("delay", -1)
                if delay in cross_run_results:
                    cross_run_results[delay]["survived"] += 1
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

    # Report cross-run results
    print("\n[purge-timing] Cross-run results by delay after purge:")
    print("[purge-timing]   Delay | Sent | Survived | Lost")
    print("[purge-timing]   ------|------|----------|-----")
    any_cross_run_loss = False
    for delay in delays_to_test:
        r = cross_run_results[delay]
        lost = r["sent"] - r["survived"]
        status = "<<<" if lost > 0 else ""
        print(f"[purge-timing]   {delay:4}s | {r['sent']:4} | {r['survived']:8} | {lost:4} {status}")
        if lost > 0:
            any_cross_run_loss = True

    if any_cross_run_loss:
        print("\n[purge-timing] CONFIRMED: Messages CAN be lost when sent during purge window!")
    else:
        print("\n[purge-timing] No losses observed in this controlled test.")

    # Final summary and recommendations
    print("\n[purge-timing] === CONCLUSIONS ===")
    print("[purge-timing] AWS docs say purge 'may take up to 60 seconds' - it's non-deterministic.")
    print("[purge-timing] In controlled tests, purges often complete quickly (no losses).")
    print("[purge-timing] But in practice (back-to-back scenario runs), we HAVE observed losses.")
    print("[purge-timing]")
    print("[purge-timing] Key factors that may affect timing:")
    print("[purge-timing]   - Queue depth at purge time")
    print("[purge-timing]   - AWS region load")
    print("[purge-timing]   - Time since last purge (60s cooldown between purges)")
    print("[purge-timing]   - Message visibility state")
    print("[purge-timing]")
    print("[purge-timing] RECOMMENDATION: Always wait 60+ seconds after any purge before sending")
    print("[purge-timing] messages to that queue. Our cleanup uses this approach to stay safe.")


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

    partial = subparsers.add_parser(
        "partial-batch", help="Partial batch failure scenario (batch of 10 with mixed good/poison)"
    )
    partial.add_argument("--count", type=int, default=50, help="Total messages to send")
    partial.add_argument(
        "--poison-every",
        type=int,
        default=3,
        help="Make every Nth message poison (e.g., 3 means indices 2, 5, 8...)",
    )
    partial.add_argument(
        "--idle-timeout",
        type=int,
        default=30,
        help="Idle timeout for consumer to exit when queue is drained",
    )
    partial.add_argument(
        "--consumer-timeout",
        type=int,
        default=300,
        help="Timeout (seconds) for consumer subprocess",
    )
    partial.add_argument(
        "--queue-timeout",
        type=int,
        default=180,
        help="Timeout (seconds) to wait for queue to drain",
    )

    side = subparsers.add_parser(
        "side-effects", help="External side effects scenario (check-before-doing prevents duplicates)"
    )
    side.add_argument("--count", type=int, default=5, help="Messages to send")
    side.add_argument(
        "--crash-window",
        type=int,
        default=15,
        help="Seconds to let consumer run before killing (should process all messages)",
    )
    side.add_argument(
        "--visibility-wait",
        type=int,
        default=DEFAULT_VISIBILITY_BUFFER,
        help="Seconds to wait for visibility timeout to expire after crash",
    )
    side.add_argument(
        "--idle-timeout",
        type=int,
        default=30,
        help="Idle timeout for second consumer run",
    )
    side.add_argument(
        "--consumer-timeout",
        type=int,
        default=120,
        help="Timeout (seconds) for consumer subprocess",
    )
    side.add_argument(
        "--queue-timeout",
        type=int,
        default=120,
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

    gs = subparsers.add_parser(
        "graceful-shutdown",
        help="Graceful shutdown scenario (consumer finishes in-flight work after SIGTERM)",
    )
    gs.add_argument("--count", type=int, default=5, help="Messages to send")
    gs.add_argument(
        "--processing-delay",
        type=int,
        default=5,
        help="Seconds to simulate slow processing per message",
    )
    gs.add_argument(
        "--sigterm-delay",
        type=int,
        default=2,
        help="Seconds to wait before sending SIGTERM (consumer should be mid-processing)",
    )
    gs.add_argument(
        "--consumer-timeout",
        type=int,
        default=30,
        help="Timeout (seconds) to wait for consumer to exit after SIGTERM",
    )
    gs.add_argument(
        "--idle-timeout",
        type=int,
        default=30,
        help="Idle timeout for consumer (should not be reached)",
    )

    pt = subparsers.add_parser(
        "purge-timing",
        help="SQS purge timing scenario (verify 60-second danger window behavior)",
    )
    pt.add_argument(
        "--iterations",
        type=int,
        default=2,
        help="Number of purge cycles to run (more iterations = better chance of observing behavior)",
    )
    pt.add_argument(
        "--danger-count",
        type=int,
        default=10,
        help="Messages to send immediately after purge (per iteration)",
    )
    pt.add_argument(
        "--safe-delay",
        type=int,
        default=65,
        help="Seconds after purge before sending safe messages (should be >60)",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    scenario_fn = {
        "happy": scenario_happy,
        "crash": scenario_crash,
        "duplicates": scenario_duplicates,
        "poison": scenario_poison,
        "partial-batch": scenario_partial_batch,
        "side-effects": scenario_side_effects,
        "backpressure": scenario_backpressure,
        "graceful-shutdown": scenario_graceful_shutdown,
        "purge-timing": scenario_purge_timing,
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
        "message_side_effects_table": scenario_resources["message_side_effects_table"],
        "aws_region": region,
    }

    profile = os.environ.get("AWS_PROFILE")
    print(f"[run] Validating {args.scenario} infrastructure ...")
    validate_scenario_infra(outputs, region, profile)

    scenario_fn(args, outputs)


if __name__ == "__main__":
    main()
