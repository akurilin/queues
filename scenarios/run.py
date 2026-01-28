"""Scenario runner for queue behaviors with per-scenario infrastructure."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional

import boto3
from botocore.client import BaseClient
from botocore.session import Session

from validate_infra import validate_scenario_infra

REPO_ROOT = Path(__file__).resolve().parent.parent
SCENARIOS_TF_DIR = Path(__file__).resolve().parent / "terraform"
DEFAULT_VISIBILITY_BUFFER = 15  # seconds to wait for visibility timeout expiry


# ---------------------------------------------------------------------------
# Terraform lifecycle
# ---------------------------------------------------------------------------


def terraform_cmd(
    args: list[str], tf_dir: Optional[str] = None, stream: bool = False
) -> subprocess.CompletedProcess:
    """Run a terraform command in the scenarios/terraform/ directory.

    When *stream* is True the command's stdout/stderr go straight to the
    terminal so the user can watch progress (useful for apply/destroy).
    """
    cwd = tf_dir or str(SCENARIOS_TF_DIR)
    cmd = ["terraform", f"-chdir={cwd}"] + args
    if stream:
        return subprocess.run(cmd, text=True, check=True)
    return subprocess.run(cmd, capture_output=True, text=True, check=True)


def generate_workspace_name(scenario: str) -> str:
    """Return a unique workspace name: {scenario}-{random_8_hex}."""
    return f"{scenario}-{secrets.token_hex(4)}"


def _tfvars_path(workspace: str) -> Path:
    """Return the path to the workspace-specific .tfvars file."""
    return SCENARIOS_TF_DIR / f"{workspace}.tfvars.json"


def get_main_tf_outputs() -> Dict[str, str]:
    """Read terraform outputs from the main infrastructure."""
    result = subprocess.run(
        ["terraform", f"-chdir={REPO_ROOT / 'terraform'}", "output", "-json"],
        capture_output=True,
        text=True,
        check=True,
    )
    raw = json.loads(result.stdout)
    return {k: v["value"] for k, v in raw.items()}


def infra_up(
    scenario: str,
    container_image: str,
    main_outputs: Dict,
    tf_vars: Optional[Dict[str, str]] = None,
) -> Dict:
    """Provision per-scenario infrastructure. Returns terraform outputs + _workspace."""
    workspace = generate_workspace_name(scenario)
    tfvars = _tfvars_path(workspace)

    # Build tfvars
    vars_dict = {
        "scenario_name": workspace,
        "container_image": container_image,
        "aws_region": main_outputs.get("aws_region", os.environ.get("AWS_REGION", "us-west-1")),
        "subnets": main_outputs["subnet_ids"],
        "security_group_ids": main_outputs["security_group_ids"],
        "execution_role_arn": main_outputs["execution_role_arn"],
        "log_group_name": main_outputs["log_group_name"],
    }
    if tf_vars:
        vars_dict.update(tf_vars)

    tfvars.write_text(json.dumps(vars_dict, indent=2))
    print(f"[infra] Wrote {tfvars}")

    print(f"[infra] terraform init ...")
    terraform_cmd(["init", "-input=false"])

    print(f"[infra] Creating workspace {workspace} ...")
    terraform_cmd(["workspace", "new", workspace])

    print(f"[infra] terraform apply ...")
    terraform_cmd(["apply", "-auto-approve", f"-var-file={tfvars}"], stream=True)

    print(f"[infra] Reading outputs ...")
    result = terraform_cmd(["output", "-json"])
    raw = json.loads(result.stdout)
    outputs = {k: v["value"] for k, v in raw.items()}
    outputs["_workspace"] = workspace
    outputs["_container_image"] = container_image
    return outputs


def infra_down(workspace: str) -> None:
    """Destroy per-scenario infrastructure and clean up workspace."""
    tfvars = _tfvars_path(workspace)
    try:
        print(f"[infra] Selecting workspace {workspace} ...")
        terraform_cmd(["workspace", "select", workspace])

        print(f"[infra] terraform destroy ...")
        if tfvars.exists():
            terraform_cmd(["destroy", "-auto-approve", f"-var-file={tfvars}"], stream=True)
        else:
            terraform_cmd(["destroy", "-auto-approve"], stream=True)

        print(f"[infra] Cleaning up workspace {workspace} ...")
        terraform_cmd(["workspace", "select", "default"])
        terraform_cmd(["workspace", "delete", workspace])
    finally:
        if tfvars.exists():
            tfvars.unlink()
            print(f"[infra] Removed {tfvars}")


@contextmanager
def scenario_infra(
    scenario: str,
    container_image: str,
    main_outputs: Dict,
    tf_vars: Optional[Dict[str, str]] = None,
):
    """Context manager: provision → validate → yield outputs → destroy."""
    outputs = infra_up(scenario, container_image, main_outputs, tf_vars)
    workspace = outputs["_workspace"]
    try:
        profile = os.environ.get("AWS_PROFILE")
        region = outputs.get("aws_region", "us-west-1")
        validate_scenario_infra(outputs, main_outputs, region, profile, container_image)
        yield outputs
    finally:
        infra_down(workspace)


# ---------------------------------------------------------------------------
# AWS helpers
# ---------------------------------------------------------------------------


def build_sqs_client(region: str, profile: Optional[str]) -> BaseClient:
    """Construct an SQS client using region/profile."""
    session: Session = boto3.Session(region_name=region, profile_name=profile or None)
    return session.client("sqs")


def build_dynamo_resource(region: str, profile: Optional[str]):
    """Construct a DynamoDB resource using region/profile."""
    session: Session = boto3.Session(region_name=region, profile_name=profile or None)
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


# ---------------------------------------------------------------------------
# ECS helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Producer helper
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def scenario_happy(args: argparse.Namespace, outputs: Dict, main_outputs: Dict) -> None:
    """Scenario 1: Happy path — messages flow through cleanly."""
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    sqs = build_sqs_client(region, profile)

    cluster = outputs["cluster_name"]
    task_definition = outputs["task_definition_arn"]
    container = outputs["container_name"]
    subnets = main_outputs["subnet_ids"]
    security_groups = main_outputs["security_group_ids"]

    message_count = args.count or 5
    batch_size = min(args.batch_size or 5, 10)

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
    ensure_queue_empty(sqs, dlq_url, "DLQ")
    print("[happy] PASS")


def scenario_crash(args: argparse.Namespace, outputs: Dict, main_outputs: Dict) -> None:
    """Scenario 2: Consumer crash mid-processing, then redelivery."""
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    sqs = build_sqs_client(region, profile)

    cluster = outputs["cluster_name"]
    task_definition = outputs["task_definition_arn"]
    container = outputs["container_name"]
    subnets = main_outputs["subnet_ids"]
    security_groups = main_outputs["security_group_ids"]

    message_count = 1
    print(f"[crash] Sending {message_count} message ...")
    run_producer(message_count, batch_size=1, region=region, queue_url=queue_url, profile=profile)
    print("[crash] Confirming message is enqueued ...")
    wait_for_messages_enqueued(sqs, queue_url, message_count)

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
    ensure_queue_empty(sqs, dlq_url, "DLQ")
    print("[crash] PASS")


def scenario_duplicates(args: argparse.Namespace, outputs: Dict, main_outputs: Dict) -> None:
    """Scenario 3: Duplicate delivery handled via idempotent side effects."""
    queue_url = outputs["queue_url"]
    region = outputs["aws_region"]
    profile = os.environ.get("AWS_PROFILE", "")
    status_table_name = outputs["message_status_table"]
    completed_table_name = outputs["message_completed_table"]

    cluster = outputs["cluster_name"]
    task_definition = outputs["task_definition_arn"]
    container = outputs["container_name"]
    subnets = main_outputs["subnet_ids"]
    security_groups = main_outputs["security_group_ids"]

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

    print("[dup] Starting two ECS tasks concurrently (long work > visibility timeout) ...")
    task1 = run_ecs_task(
        cluster, task_definition, container, subnets, security_groups, env=common_env,
    )
    time.sleep(args.second_start_delay)
    task2 = run_ecs_task(
        cluster, task_definition, container, subnets, security_groups, env=common_env,
    )

    task1_desc = wait_for_task_stop(cluster, task1, timeout=args.consumer_timeout)
    task2_desc = wait_for_task_stop(cluster, task2, timeout=args.consumer_timeout)
    exit1 = get_container_exit_code(task1_desc, container)
    exit2 = get_container_exit_code(task2_desc, container)
    if exit1 not in (0, None):
        raise RuntimeError(f"[dup] first task failed with exit code {exit1}")
    if exit2 not in (0, None):
        raise RuntimeError(f"[dup] second task failed with exit code {exit2}")

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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def latest_ecr_tag(repo_url: str) -> str:
    """Return the tag of the most recently pushed image in the ECR repo."""
    repo_name = repo_url.split("/", 1)[-1]
    region = repo_url.split(".")[3]
    ecr = boto3.Session(region_name=region).client("ecr")
    resp = ecr.describe_images(
        repositoryName=repo_name,
        filter={"tagStatus": "TAGGED"},
    )
    images = resp.get("imageDetails", [])
    if not images:
        raise SystemExit(f"No tagged images found in ECR repo {repo_name}")
    newest = max(images, key=lambda i: i["imagePushedAt"])
    tags = newest.get("imageTags", [])
    if not tags:
        raise SystemExit(f"Most recent image in {repo_name} has no tags")
    return tags[0]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run queue scenarios")
    parser.add_argument(
        "--image-tag", type=str, default=None, help="Container image tag (default: most recent)"
    )
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

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print("[main] Reading main terraform outputs ...")
    main_outputs = get_main_tf_outputs()

    ecr_repo_url = main_outputs["ecr_repository_url"]
    image_tag = args.image_tag or latest_ecr_tag(ecr_repo_url)
    container_image = f"{ecr_repo_url}:{image_tag}"
    print(f"[main] Container image: {container_image}")

    scenario_fn = {
        "happy": scenario_happy,
        "crash": scenario_crash,
        "duplicates": scenario_duplicates,
    }.get(args.scenario)

    if not scenario_fn:
        raise SystemExit(f"Unknown scenario {args.scenario}")

    with scenario_infra(args.scenario, container_image, main_outputs) as outputs:
        scenario_fn(args, outputs, main_outputs)


if __name__ == "__main__":
    main()
