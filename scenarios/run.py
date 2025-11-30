"""Scenario runner for queue behaviors (initial focus: happy path + crash)."""

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
from botocore.session import Session


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_VISIBILITY_BUFFER = 15  # seconds to wait for visibility timeout expiry
DEFAULT_ECS_CLUSTER = os.getenv("ECS_CLUSTER", "sqs-demo-cluster")
DEFAULT_ECS_SERVICE = os.getenv("ECS_SERVICE", "sqs-demo-service")
DEFAULT_ECS_CONTAINER = os.getenv("ECS_CONTAINER", "sqs-demo-consumer")


def load_env_file() -> Dict[str, str]:
    """Load simple KEY=VALUE pairs from .env if present."""
    env_path = REPO_ROOT / ".env"
    values: Dict[str, str] = {}
    if not env_path.exists():
        return values
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        values[key.strip()] = val.strip()
    return values


def resolve_env(overrides: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Resolve environment values from process env, .env file, then overrides."""
    env_file = load_env_file()
    resolved: Dict[str, str] = {}
    resolved["QUEUE_URL"] = os.environ.get("QUEUE_URL") or env_file.get("QUEUE_URL", "")
    resolved["AWS_REGION"] = (
        os.environ.get("AWS_REGION") or env_file.get("AWS_REGION") or "us-west-1"
    )
    profile = os.environ.get("AWS_PROFILE") or env_file.get("AWS_PROFILE") or ""
    if profile:
        resolved["AWS_PROFILE"] = profile
    for key in ("DLQ_ARN", "MESSAGE_STATUS_TABLE", "MESSAGE_COMPLETED_TABLE"):
        value = os.environ.get(key) or env_file.get(key)
        if value:
            resolved[key] = value
    if overrides:
        resolved.update({k: v for k, v in overrides.items() if v is not None})
    return resolved


def build_sqs_client(region: str, profile: Optional[str]) -> BaseClient:
    """Construct an SQS client using region/profile."""
    session: Session = boto3.Session(region_name=region, profile_name=profile or None)
    return session.client("sqs")


def build_dynamo_resource(region: str, profile: Optional[str]):
    """Construct a DynamoDB resource using region/profile."""
    session: Session = boto3.Session(region_name=region, profile_name=profile or None)
    return session.resource("dynamodb")


def clear_dynamo_tables(dynamo, table_names: list[str], prefix: str = "") -> None:
    """Delete all items from the given DynamoDB tables."""
    for name in table_names:
        table = dynamo.Table(name)
        print(f"{prefix} Clearing DynamoDB table {name} ...")
        while True:
            resp = table.scan(ProjectionExpression="message_id")
            items = resp.get("Items", [])
            if not items:
                break
            with table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(Key={"message_id": item["message_id"]})
            if "LastEvaluatedKey" not in resp:
                break


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


# --- ECS helpers ---


def describe_service(cluster: str, service: str) -> dict:
    """Describe the ECS service to fetch network config and desired count."""
    data = subprocess.check_output(
        [
            "aws",
            "ecs",
            "describe-services",
            "--cluster",
            cluster,
            "--services",
            service,
        ]
    )
    import json

    payload = json.loads(data)
    services = payload.get("services", [])
    if not services:
        raise RuntimeError(f"ECS service {service} not found on cluster {cluster}")
    return services[0]


def describe_service_config(cluster: str, service: str) -> tuple[dict, int, list[str], list[str], str]:
    """Fetch service description and unpack desired count, networking, and task def."""
    service_desc = describe_service(cluster, service)
    previous_desired = service_desc.get("desiredCount", 0)
    awsvpc = service_desc["networkConfiguration"]["awsvpcConfiguration"]
    subnets = awsvpc["subnets"]
    security_groups = awsvpc["securityGroups"]
    task_definition = service_desc["taskDefinition"]
    return service_desc, previous_desired, subnets, security_groups, task_definition


def update_service_desired(cluster: str, service: str, desired: int) -> None:
    """Update desired count for the ECS service and wait for stability."""
    subprocess.check_call(
        [
            "aws",
            "ecs",
            "update-service",
            "--cluster",
            cluster,
            "--service",
            service,
            "--desired-count",
            str(desired),
        ]
    )
    subprocess.check_call(
        [
            "aws",
            "ecs",
            "wait",
            "services-stable",
            "--cluster",
            cluster,
            "--services",
            service,
        ]
    )


def run_ecs_task(
    cluster: str,
    task_definition: str,
    container_name: str,
    subnets: list[str],
    security_groups: list[str],
    env: Dict[str, str],
) -> str:
    """Start a one-off ECS task with env overrides; return task ARN."""
    env_overrides = [{"name": k, "value": v} for k, v in env.items()]
    import json

    cmd = [
        "aws",
        "ecs",
        "run-task",
        "--cluster",
        cluster,
        "--task-definition",
        task_definition,
        "--launch-type",
        "FARGATE",
        "--network-configuration",
        json.dumps(
            {
                "awsvpcConfiguration": {
                    "subnets": subnets,
                    "securityGroups": security_groups,
                    "assignPublicIp": "ENABLED",
                }
            }
        ),
        "--overrides",
        json.dumps(
            {
                "containerOverrides": [
                    {"name": container_name, "environment": env_overrides}
                ]
            }
        ),
    ]
    data = subprocess.check_output(cmd)
    payload = json.loads(data)
    failures = payload.get("failures", [])
    if failures:
        raise RuntimeError(f"ECS run-task failed: {failures}")
    tasks = payload.get("tasks", [])
    if not tasks:
        raise RuntimeError("ECS run-task returned no tasks")
    return tasks[0]["taskArn"]


def wait_for_task_stop(cluster: str, task_arn: str, timeout: int = 300) -> dict:
    """Wait until the task stops and return its description."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        desc = subprocess.check_output(
            ["aws", "ecs", "describe-tasks", "--cluster", cluster, "--tasks", task_arn]
        )
        import json

        payload = json.loads(desc)
        tasks = payload.get("tasks", [])
        if tasks and tasks[0].get("lastStatus") == "STOPPED":
            return tasks[0]
        time.sleep(3)
    raise RuntimeError("ECS task did not stop in time")


def get_container_exit_code(task: dict, container_name: str) -> Optional[int]:
    """Extract exit code for a specific container from task description."""
    for c in task.get("containers", []):
        if c.get("name") == container_name:
            return c.get("exitCode")
    return None


def wait_for_queue_empty(
    sqs: BaseClient, queue_url: str, timeout: int = 90, poll_seconds: int = 3
) -> None:
    """Wait until both visible and in-flight counts drop to zero or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        depth = get_queue_depth(sqs, queue_url)
        if depth["visible"] == 0 and depth["not_visible"] == 0:
            return
        time.sleep(poll_seconds)
    raise RuntimeError("Queue did not drain within timeout")


def purge_queue_if_needed(sqs: BaseClient, queue_url: str, label: str, timeout: int = 90) -> None:
    """Purge the queue if it has messages and wait for it to be empty."""
    depth = get_queue_depth(sqs, queue_url)
    if depth["visible"] == 0 and depth["not_visible"] == 0:
        return
    print(f"[setup] Purging {label} queue (visible={depth['visible']} not_visible={depth['not_visible']}) ...")
    sqs.purge_queue(QueueUrl=queue_url)
    wait_for_queue_empty(sqs, queue_url, timeout=timeout)


def clear_queues(sqs: BaseClient, queue_url: str, dlq_url: Optional[str], prefix: str) -> None:
    """Ensure primary (and DLQ if present) are empty, purging if needed."""
    print(f"{prefix} Clearing primary queue ...")
    purge_queue_if_needed(sqs, queue_url, "primary")
    ensure_queue_empty(sqs, queue_url, "primary")
    if dlq_url:
        print(f"{prefix} Clearing DLQ ...")
        purge_queue_if_needed(sqs, dlq_url, "DLQ")
        ensure_queue_empty(sqs, dlq_url, "DLQ")


def run_producer(count: int, batch_size: int, region: str, queue_url: str, profile: str) -> None:
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
    if profile:
        cmd.extend(["--profile", profile])
    subprocess.run(cmd, check=True)


def run_consumer(
    queue_url: str,
    region: str,
    profile: str,
    extra_env: Optional[Dict[str, str]] = None,
    expect_success: bool = True,
    timeout: int = 120,
) -> subprocess.CompletedProcess:
    """Run the consumer script with env overrides."""
    env = os.environ.copy()
    env.update(
        {
            "QUEUE_URL": queue_url,
            "AWS_REGION": region,
        }
    )
    if profile:
        env["AWS_PROFILE"] = profile
    if extra_env:
        env.update(extra_env)

    cmd = [sys.executable, str(REPO_ROOT / "consumer" / "consume.py")]
    result = subprocess.run(cmd, env=env, timeout=timeout, check=False)
    if expect_success and result.returncode != 0:
        raise RuntimeError(f"Consumer failed unexpectedly (exit {result.returncode})")
    if not expect_success and result.returncode == 0:
        raise RuntimeError("Consumer was expected to fail but exited 0")
    return result


def start_consumer_process(
    queue_url: str,
    region: str,
    profile: str,
    extra_env: Optional[Dict[str, str]] = None,
) -> subprocess.Popen:
    """Start the consumer process without waiting for completion."""
    env = os.environ.copy()
    env.update(
        {
            "QUEUE_URL": queue_url,
            "AWS_REGION": region,
        }
    )
    if profile:
        env["AWS_PROFILE"] = profile
    if extra_env:
        env.update(extra_env)

    cmd = [sys.executable, str(REPO_ROOT / "consumer" / "consume.py")]
    return subprocess.Popen(cmd, env=env)


def stop_consumer_process(proc: subprocess.Popen, timeout: int = 10) -> None:
    """Attempt graceful stop, then kill if needed."""
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()


def scenario_happy(args: argparse.Namespace, env: Dict[str, str]) -> None:
    """Scenario 1: Happy path."""
    queue_url = env["QUEUE_URL"]
    region = env["AWS_REGION"]
    profile = env.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(env["DLQ_ARN"]) if env.get("DLQ_ARN") else None
    sqs = build_sqs_client(region, profile)
    cluster = DEFAULT_ECS_CLUSTER
    service = DEFAULT_ECS_SERVICE
    container = DEFAULT_ECS_CONTAINER

    message_count = args.count or 5
    batch_size = min(args.batch_size or 5, 10)

    clear_queues(sqs, queue_url, dlq_url, "[happy]")

    (
        service_desc,
        previous_desired,
        subnets,
        security_groups,
        task_definition,
    ) = describe_service_config(cluster, service)

    if previous_desired != 0:
        print(f"[happy] Scaling service {service} to 0 to avoid interference (was {previous_desired}) ...")
        update_service_desired(cluster, service, 0)

    print(f"[happy] Sending {message_count} messages ...")
    run_producer(message_count, batch_size, region, queue_url, profile)
    print("[happy] Confirming messages are enqueued ...")
    wait_for_messages_enqueued(sqs, queue_url, message_count)

    print("[happy] Running ECS task to drain messages ...")
    task_arn = run_ecs_task(
        cluster,
        task_definition,
        container,
        subnets,
        security_groups,
        env={
            "QUEUE_URL": queue_url,
            "AWS_REGION": region,
            "MESSAGE_LIMIT": str(message_count),
            "LOG_LEVEL": "INFO",
            "IDLE_TIMEOUT_SECONDS": "30",
        },
    )
    task_desc = wait_for_task_stop(cluster, task_arn, timeout=args.consumer_timeout)
    exit_code = get_container_exit_code(task_desc, container)
    if exit_code not in (0, None):
        raise RuntimeError(f"Happy task failed with exit code {exit_code}")

    print("[happy] Waiting for queue to drain ...")
    wait_for_queue_empty(sqs, queue_url)
    ensure_queue_empty(sqs, queue_url, "primary")
    if dlq_url:
        ensure_queue_empty(sqs, dlq_url, "DLQ")
    print("[happy] PASS")

    if previous_desired != 0:
        print(f"[happy] Restoring service desiredCount to {previous_desired} ...")
        update_service_desired(cluster, service, previous_desired)


def scenario_crash(args: argparse.Namespace, env: Dict[str, str]) -> None:
    """Scenario 2: Consumer crash mid-processing."""
    queue_url = env["QUEUE_URL"]
    region = env["AWS_REGION"]
    profile = env.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(env["DLQ_ARN"]) if env.get("DLQ_ARN") else None
    sqs = build_sqs_client(region, profile)
    cluster = DEFAULT_ECS_CLUSTER
    service = DEFAULT_ECS_SERVICE
    container = DEFAULT_ECS_CONTAINER

    print("[crash] Clearing queues before start ...")
    purge_queue_if_needed(sqs, queue_url, "primary")
    ensure_queue_empty(sqs, queue_url, "primary")
    if dlq_url:
        purge_queue_if_needed(sqs, dlq_url, "DLQ")
        ensure_queue_empty(sqs, dlq_url, "DLQ")

    message_count = 1
    print(f"[crash] Sending {message_count} message ...")
    run_producer(message_count, batch_size=1, region=region, queue_url=queue_url, profile=profile)
    print("[crash] Confirming message is enqueued ...")
    wait_for_messages_enqueued(sqs, queue_url, message_count)

    (
        service_desc,
        previous_desired,
        subnets,
        security_groups,
        task_definition,
    ) = describe_service_config(cluster, service)

    if previous_desired != 0:
        print(f"[crash] Scaling service {service} to 0 to avoid interference (was {previous_desired}) ...")
        update_service_desired(cluster, service, 0)

    print("[crash] Running ECS task that will crash mid-processing ...")
    crash_task = run_ecs_task(
        cluster,
        task_definition,
        container,
        subnets,
        security_groups,
        env={
            "QUEUE_URL": queue_url,
            "AWS_REGION": region,
            "CRASH_AFTER_RECEIVE": "1",
            "MESSAGE_LIMIT": str(message_count),
            "LOG_LEVEL": "INFO",
            "IDLE_TIMEOUT_SECONDS": "30",
        },
    )
    print(f"[crash] Crash task ARN: {crash_task}")
    crash_desc = wait_for_task_stop(cluster, crash_task, timeout=args.consumer_timeout)
    crash_exit = get_container_exit_code(crash_desc, container)
    if crash_exit == 0:
        raise RuntimeError("Crash task exited 0 but was expected to fail")

    visibility_wait = args.visibility_wait or DEFAULT_VISIBILITY_BUFFER
    print(f"[crash] Waiting {visibility_wait}s for visibility timeout to expire ...")
    time.sleep(visibility_wait)

    print("[crash] Rerunning consumer to process redelivered message ...")
    task2 = run_ecs_task(
        cluster,
        task_definition,
        container,
        subnets,
        security_groups,
        env={
            "QUEUE_URL": queue_url,
            "AWS_REGION": region,
            "MESSAGE_LIMIT": str(message_count),
            "LOG_LEVEL": "INFO",
            "IDLE_TIMEOUT_SECONDS": "30",
        },
    )
    task2_desc = wait_for_task_stop(cluster, task2, timeout=args.consumer_timeout)
    exit2 = get_container_exit_code(task2_desc, container)
    if exit2 not in (0, None):
        raise RuntimeError(f"Second task failed with exit code {exit2}")

    print("[crash] Waiting for queue to drain ...")
    wait_for_queue_empty(sqs, queue_url)
    ensure_queue_empty(sqs, queue_url, "primary")
    if dlq_url:
        ensure_queue_empty(sqs, dlq_url, "DLQ")
    print("[crash] PASS")

    if previous_desired != 0:
        print(f"[crash] Restoring service desiredCount to {previous_desired} ...")
        update_service_desired(cluster, service, previous_desired)


def scenario_duplicates(args: argparse.Namespace, env: Dict[str, str]) -> None:
    """Scenario 3: Duplicate delivery handled via idempotent side effects."""
    queue_url = env["QUEUE_URL"]
    region = env["AWS_REGION"]
    profile = env.get("AWS_PROFILE", "")
    status_table_name = env.get("MESSAGE_STATUS_TABLE")
    completed_table_name = env.get("MESSAGE_COMPLETED_TABLE")
    if not status_table_name or not completed_table_name:
        raise RuntimeError(
            "MESSAGE_STATUS_TABLE and MESSAGE_COMPLETED_TABLE must be set for duplicates scenario"
        )

    cluster = DEFAULT_ECS_CLUSTER
    service = DEFAULT_ECS_SERVICE
    container = DEFAULT_ECS_CONTAINER

    dlq_url = queue_url_from_arn(env["DLQ_ARN"]) if env.get("DLQ_ARN") else None
    sqs = build_sqs_client(region, profile)
    dynamo = build_dynamo_resource(region, profile)
    status_table = dynamo.Table(status_table_name)
    completed_table = dynamo.Table(completed_table_name)
    (
        service_desc,
        previous_desired,
        subnets,
        security_groups,
        task_definition,
    ) = describe_service_config(cluster, service)

    print("[dup] Clearing queues before start ...")
    clear_queues(sqs, queue_url, dlq_url, "[dup]")
    clear_dynamo_tables(dynamo, [status_table_name, completed_table_name], prefix="[dup]")

    print(f"[dup] Scaling service {service} to 0 to avoid interference (was {previous_desired}) ...")
    update_service_desired(cluster, service, 0)

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

    print("[dup] Starting two ECS tasks concurrently (long work > visibility timeout) ...")
    task1 = run_ecs_task(
        cluster,
        task_definition,
        container,
        subnets,
        security_groups,
        env=common_env,
    )
    time.sleep(args.second_start_delay)
    task2 = run_ecs_task(
        cluster,
        task_definition,
        container,
        subnets,
        security_groups,
        env=common_env,
    )

    task1_desc = wait_for_task_stop(cluster, task1, timeout=args.consumer_timeout)
    task2_desc = wait_for_task_stop(cluster, task2, timeout=args.consumer_timeout)
    exit1 = get_container_exit_code(task1_desc, container)
    exit2 = get_container_exit_code(task2_desc, container)
    if exit1 not in (0, None):
        raise RuntimeError(f"[dup] first task failed with exit code {exit1}")
    if exit2 not in (0, None):
        raise RuntimeError(f"[dup] second task failed with exit code {exit2}")

    if previous_desired != 0:
        print(f"[dup] Restoring service desiredCount to {previous_desired} ...")
        update_service_desired(cluster, service, previous_desired)

    print("[dup] Waiting for queue to drain ...")
    wait_for_queue_empty(sqs, queue_url, timeout=args.queue_timeout)
    ensure_queue_empty(sqs, queue_url, "primary")
    if dlq_url:
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
        help="Timeout (seconds) for ECS task to complete",
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

    clean = subparsers.add_parser("clean", help="Reset state: purge queues and exit")
    clean.add_argument(
        "--consumer-timeout",
        type=int,
        default=90,
        help="Timeout placeholder (kept for symmetry; not used currently)",
    )

    return parser.parse_args()


def scenario_clean(env: Dict[str, str]) -> None:
    """Scenario 0: Clean queues to a known empty state."""
    queue_url = env["QUEUE_URL"]
    region = env["AWS_REGION"]
    profile = env.get("AWS_PROFILE", "")
    status_table = env.get("MESSAGE_STATUS_TABLE")
    completed_table = env.get("MESSAGE_COMPLETED_TABLE")
    dlq_url = queue_url_from_arn(env["DLQ_ARN"]) if env.get("DLQ_ARN") else None
    sqs = build_sqs_client(region, profile)
    dynamo_tables: list[str] = []
    if status_table:
        dynamo_tables.append(status_table)
    if completed_table:
        dynamo_tables.append(completed_table)

    clear_queues(sqs, queue_url, dlq_url, "[clean]")
    if dynamo_tables:
        dynamo = build_dynamo_resource(region, profile)
        clear_dynamo_tables(dynamo, dynamo_tables, prefix="[clean]")
    print("[clean] PASS (queues empty)")


def main() -> None:
    args = parse_args()
    env = resolve_env()
    if not env["QUEUE_URL"]:
        raise SystemExit("QUEUE_URL is required (set env or .env)")

    if args.scenario == "happy":
        scenario_happy(args, env)
    elif args.scenario == "crash":
        scenario_crash(args, env)
    elif args.scenario == "duplicates":
        scenario_duplicates(args, env)
    elif args.scenario == "clean":
        scenario_clean(env)
    else:
        raise SystemExit(f"Unknown scenario {args.scenario}")


if __name__ == "__main__":
    main()
