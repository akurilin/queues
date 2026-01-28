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


def purge_queue(sqs: BaseClient, queue_url: str, label: str) -> None:
    """Purge all messages from a queue. Silently ignores 'already purged' errors."""
    try:
        sqs.purge_queue(QueueUrl=queue_url)
        print(f"[purge] Purged {label} queue {queue_url}")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "AWS.SimpleQueueService.PurgeQueueInProgress":
            print(f"[purge] {label} queue purge already in progress")
        else:
            raise
    # SQS purge is async — give it a moment to take effect
    time.sleep(3)


def ensure_queue_empty(sqs: BaseClient, queue_url: str, label: str) -> None:
    """Verify a queue has no visible or in-flight messages."""
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


def run_local_consumer_async(env: Dict[str, str]) -> subprocess.Popen:
    """Start consume.py as a background subprocess. Returns the Popen handle."""
    full_env = {**os.environ, **env}
    print(f"[consumer] Starting {CONSUME_SCRIPT} in background ...")
    return subprocess.Popen(
        [sys.executable, str(CONSUME_SCRIPT)],
        env=full_env,
    )


# ---------------------------------------------------------------------------
# Producer helper
# ---------------------------------------------------------------------------


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
    ensure_queue_empty(sqs, queue_url, "primary")
    ensure_queue_empty(sqs, dlq_url, "DLQ")
    print("[happy] PASS")


def scenario_crash(args: argparse.Namespace, outputs: Dict) -> None:
    """Scenario 2: Consumer crash mid-processing, then redelivery."""
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    sqs = build_sqs_client(region, profile)

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
    ensure_queue_empty(sqs, queue_url, "primary")
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
    ensure_queue_empty(sqs, queue_url, "primary")
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

    print("[poison] Purging queues to start clean ...")
    purge_queue(sqs, queue_url, "primary")
    purge_queue(sqs, dlq_url, "DLQ")

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
    ensure_queue_empty(sqs, queue_url, "primary")

    print("[poison] Checking DLQ for poison messages ...")
    dlq_depth = get_queue_depth(sqs, dlq_url)
    dlq_count = dlq_depth["visible"] + dlq_depth["not_visible"]
    if dlq_count != poison_count:
        raise RuntimeError(
            f"Expected {poison_count} messages in DLQ, found {dlq_count} "
            f"(visible={dlq_depth['visible']}, not_visible={dlq_depth['not_visible']})"
        )

    print(f"[poison] PASS | good={good_count} processed, poison={poison_count} in DLQ")


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

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    scenario_fn = {
        "happy": scenario_happy,
        "crash": scenario_crash,
        "duplicates": scenario_duplicates,
        "poison": scenario_poison,
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
