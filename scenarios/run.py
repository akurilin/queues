"""Scenario runner for queue behaviors (initial focus: happy path + crash)."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
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
    if overrides:
        resolved.update({k: v for k, v in overrides.items() if v is not None})
    return resolved


def build_sqs_client(region: str, profile: Optional[str]) -> BaseClient:
    """Construct an SQS client using region/profile."""
    session: Session = boto3.Session(region_name=region, profile_name=profile or None)
    return session.client("sqs")


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

    print("[happy] Verifying primary queue is empty before start ...")
    ensure_queue_empty(sqs, queue_url, "primary")
    if dlq_url:
        ensure_queue_empty(sqs, dlq_url, "DLQ")

    service_desc = describe_service(cluster, service)
    previous_desired = service_desc.get("desiredCount", 0)
    subnets = service_desc["networkConfiguration"]["awsvpcConfiguration"]["subnets"]
    security_groups = service_desc["networkConfiguration"]["awsvpcConfiguration"][
        "securityGroups"
    ]
    task_definition = service_desc["taskDefinition"]

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

    print("[crash] Verifying primary queue is empty before start ...")
    ensure_queue_empty(sqs, queue_url, "primary")
    if dlq_url:
        ensure_queue_empty(sqs, dlq_url, "DLQ")

    message_count = 1
    print(f"[crash] Sending {message_count} message ...")
    run_producer(message_count, batch_size=1, region=region, queue_url=queue_url, profile=profile)
    print("[crash] Confirming message is enqueued ...")
    wait_for_messages_enqueued(sqs, queue_url, message_count)

    service_desc = describe_service(cluster, service)
    previous_desired = service_desc.get("desiredCount", 0)
    subnets = service_desc["networkConfiguration"]["awsvpcConfiguration"]["subnets"]
    security_groups = service_desc["networkConfiguration"]["awsvpcConfiguration"][
        "securityGroups"
    ]
    task_definition = service_desc["taskDefinition"]

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

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    env = resolve_env()
    if not env["QUEUE_URL"]:
        raise SystemExit("QUEUE_URL is required (set env or .env)")

    if args.scenario == "happy":
        scenario_happy(args, env)
    elif args.scenario == "crash":
        scenario_crash(args, env)
    else:
        raise SystemExit(f"Unknown scenario {args.scenario}")


if __name__ == "__main__":
    main()
