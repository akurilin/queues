"""Validate that scenario infrastructure exists and is correctly configured."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional

import boto3

REPO_ROOT = Path(__file__).resolve().parent.parent
SCENARIOS_TF_DIR = Path(__file__).resolve().parent / "terraform"


def _ok(resource: str) -> None:
    print(f"[validate] OK {resource}")


def _fail(resource: str, msg: str) -> None:
    raise RuntimeError(f"[validate] FAIL {resource}: {msg}")


def _session(region: str, profile: Optional[str] = None) -> boto3.Session:
    return boto3.Session(region_name=region, profile_name=profile or None)


def validate_sqs_queue(session: boto3.Session, queue_url: str, dlq_arn: str) -> None:
    """Check that the SQS queue and DLQ exist and the redrive policy is wired up."""
    sqs = session.client("sqs")

    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["VisibilityTimeout", "RedrivePolicy", "QueueArn"],
    )["Attributes"]
    _ok(f"SQS queue {queue_url}")

    redrive_raw = attrs.get("RedrivePolicy")
    if redrive_raw:
        redrive = json.loads(redrive_raw)
        if redrive.get("deadLetterTargetArn") != dlq_arn:
            _fail("SQS redrive policy", f"expected DLQ ARN {dlq_arn}, got {redrive.get('deadLetterTargetArn')}")
    _ok("SQS redrive policy points to DLQ")

    # Validate DLQ exists (convert ARN → URL)
    dlq_parts = dlq_arn.split(":")
    dlq_name = dlq_parts[5]
    dlq_url = sqs.get_queue_url(QueueName=dlq_name)["QueueUrl"]
    sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])
    _ok(f"SQS DLQ {dlq_name}")


def validate_dynamodb_tables(
    session: boto3.Session, status_table: str, completed_table: str
) -> None:
    """Check that both DynamoDB tables exist with expected key schema."""
    dynamo = session.client("dynamodb")

    for table_name in (status_table, completed_table):
        desc = dynamo.describe_table(TableName=table_name)["Table"]
        keys = {k["AttributeName"]: k["KeyType"] for k in desc["KeySchema"]}
        if keys.get("message_id") != "HASH":
            _fail(f"DynamoDB {table_name}", f"expected hash key 'message_id', got {keys}")
        billing = desc.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED")
        if billing != "PAY_PER_REQUEST":
            _fail(f"DynamoDB {table_name}", f"expected PAY_PER_REQUEST billing, got {billing}")
        _ok(f"DynamoDB table {table_name}")


def validate_ecs_cluster(session: boto3.Session, cluster_name: str) -> None:
    """Check that the ECS cluster exists and is ACTIVE."""
    ecs = session.client("ecs")
    resp = ecs.describe_clusters(clusters=[cluster_name])
    clusters = resp.get("clusters", [])
    if not clusters:
        _fail(f"ECS cluster {cluster_name}", "not found")
    status = clusters[0].get("status")
    if status != "ACTIVE":
        _fail(f"ECS cluster {cluster_name}", f"status is {status}, expected ACTIVE")
    _ok(f"ECS cluster {cluster_name}")


def validate_task_definition(
    session: boto3.Session,
    task_definition_arn: str,
    container_image: str,
    queue_url: str,
    container_name: str,
) -> None:
    """Check that the task definition exists and its container config is correct."""
    ecs = session.client("ecs")
    resp = ecs.describe_task_definition(taskDefinition=task_definition_arn)
    td = resp["taskDefinition"]

    containers = td.get("containerDefinitions", [])
    matched = [c for c in containers if c.get("name") == container_name]
    if not matched:
        _fail(f"ECS task def {task_definition_arn}", f"no container named {container_name}")
    container = matched[0]

    if container.get("image") != container_image:
        _fail(
            f"ECS task def container image",
            f"expected {container_image}, got {container.get('image')}",
        )

    env_vars = {e["name"]: e["value"] for e in container.get("environment", [])}
    if env_vars.get("QUEUE_URL") != queue_url:
        _fail(
            "ECS task def QUEUE_URL env",
            f"expected {queue_url}, got {env_vars.get('QUEUE_URL')}",
        )
    _ok(f"ECS task definition {task_definition_arn}")


def validate_subnets(session: boto3.Session, subnet_ids: list[str]) -> None:
    """Check that all subnets exist and are available."""
    ec2 = session.client("ec2")
    resp = ec2.describe_subnets(SubnetIds=subnet_ids)
    found = {s["SubnetId"] for s in resp["Subnets"]}
    missing = set(subnet_ids) - found
    if missing:
        _fail("Subnets", f"missing: {missing}")
    for s in resp["Subnets"]:
        if s["State"] != "available":
            _fail(f"Subnet {s['SubnetId']}", f"state is {s['State']}, expected available")
    _ok(f"Subnets ({len(subnet_ids)} found)")


def validate_security_groups(session: boto3.Session, sg_ids: list[str]) -> None:
    """Check that all security groups exist."""
    ec2 = session.client("ec2")
    resp = ec2.describe_security_groups(GroupIds=sg_ids)
    found = {sg["GroupId"] for sg in resp["SecurityGroups"]}
    missing = set(sg_ids) - found
    if missing:
        _fail("Security groups", f"missing: {missing}")
    _ok(f"Security groups ({len(sg_ids)} found)")


def validate_execution_role(session: boto3.Session, role_arn: str) -> None:
    """Check that the execution IAM role exists."""
    iam = session.client("iam")
    role_name = role_arn.split("/")[-1]
    iam.get_role(RoleName=role_name)
    _ok(f"Execution role {role_name}")


def validate_container_image(session: boto3.Session, container_image: str) -> None:
    """Check that the ECR repository exists and contains at least one image."""
    repo_and_tag = container_image.split("/", 1)[-1]
    # Strip any tag/digest suffix — we just need the repo name.
    repo_name = repo_and_tag.rsplit(":", 1)[0] if ":" in repo_and_tag else repo_and_tag

    ecr = session.client("ecr")
    resp = ecr.describe_images(
        repositoryName=repo_name,
        maxResults=1,
    )
    if not resp.get("imageDetails"):
        _fail(f"ECR repo {repo_name}", "repository exists but contains no images")
    _ok(f"ECR repo {repo_name} (has images)")


def validate_scenario_infra(
    outputs: Dict[str, str],
    main_outputs: Dict[str, str],
    region: str,
    profile: Optional[str] = None,
    container_image: Optional[str] = None,
) -> None:
    """Run all validation checks for a scenario's infrastructure."""
    session = _session(region, profile)

    # Per-scenario resources
    validate_sqs_queue(session, outputs["queue_url"], outputs["dlq_arn"])
    validate_dynamodb_tables(
        session, outputs["message_status_table"], outputs["message_completed_table"]
    )
    validate_ecs_cluster(session, outputs["cluster_name"])
    validate_task_definition(
        session,
        outputs["task_definition_arn"],
        container_image or main_outputs.get("_container_image", ""),
        outputs["queue_url"],
        outputs["container_name"],
    )

    # Shared resources
    validate_subnets(session, main_outputs["subnet_ids"])
    validate_security_groups(session, main_outputs["security_group_ids"])
    validate_execution_role(session, main_outputs["execution_role_arn"])

    if container_image:
        validate_container_image(session, container_image)

    print("[validate] All checks passed.")


def _get_terraform_outputs(chdir: str) -> Dict:
    """Read terraform output -json from the given directory."""
    result = subprocess.run(
        ["terraform", f"-chdir={chdir}", "output", "-json"],
        capture_output=True,
        text=True,
        check=True,
    )
    raw = json.loads(result.stdout)
    return {k: v["value"] for k, v in raw.items()}


def _get_workspace_outputs(workspace: str) -> Dict:
    """Select a workspace and read its outputs."""
    tf_dir = str(SCENARIOS_TF_DIR)
    subprocess.run(
        ["terraform", f"-chdir={tf_dir}", "workspace", "select", workspace],
        check=True,
        capture_output=True,
        text=True,
    )
    return _get_terraform_outputs(tf_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate scenario infrastructure")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--main", action="store_true", help="Validate main (shared) infrastructure only")
    group.add_argument("--workspace", type=str, help="Validate a specific scenario workspace")
    parser.add_argument("--profile", type=str, default=None, help="AWS profile")
    args = parser.parse_args()

    main_outputs = _get_terraform_outputs(str(REPO_ROOT / "terraform"))
    region = main_outputs.get("aws_region", os.environ.get("AWS_REGION", "us-west-1"))

    session = _session(region, args.profile)

    if args.main:
        print("[validate] Checking shared infrastructure ...")
        validate_subnets(session, main_outputs["subnet_ids"])
        validate_security_groups(session, main_outputs["security_group_ids"])
        validate_execution_role(session, main_outputs["execution_role_arn"])
        if main_outputs.get("ecr_repository_url"):
            validate_container_image(session, main_outputs["ecr_repository_url"])
        print("[validate] Shared infrastructure OK.")
    else:
        print(f"[validate] Checking workspace {args.workspace} ...")
        outputs = _get_workspace_outputs(args.workspace)
        container_image = main_outputs.get("ecr_repository_url", "")
        validate_scenario_infra(outputs, main_outputs, region, args.profile, container_image)


if __name__ == "__main__":
    main()
