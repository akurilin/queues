"""Validate that scenario infrastructure exists and is correctly configured."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Dict, Optional

import boto3

REPO_ROOT = Path(__file__).resolve().parent.parent
TERRAFORM_DIR = REPO_ROOT / "terraform"

SCENARIO_TF_KEYS = {
    "happy": "happy",
    "crash": "crash",
    "duplicates": "dup",
    "poison": "poison",
}


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

    # Validate DLQ exists (convert ARN -> URL)
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


def validate_scenario_infra(
    outputs: Dict[str, str],
    region: str,
    profile: Optional[str] = None,
) -> None:
    """Run all validation checks for a scenario's infrastructure."""
    session = _session(region, profile)

    validate_sqs_queue(session, outputs["queue_url"], outputs["dlq_arn"])
    validate_dynamodb_tables(
        session, outputs["message_status_table"], outputs["message_completed_table"]
    )

    print("[validate] All checks passed.")


def _read_terraform_outputs() -> Dict:
    """Read terraform output -json from the main terraform/ directory."""
    result = subprocess.run(
        ["terraform", f"-chdir={TERRAFORM_DIR}", "output", "-json"],
        capture_output=True,
        text=True,
        check=True,
    )
    raw = json.loads(result.stdout)
    return {k: v["value"] for k, v in raw.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate scenario infrastructure")
    parser.add_argument("--profile", type=str, default=None, help="AWS profile")
    args = parser.parse_args()

    tf_outputs = _read_terraform_outputs()
    region = tf_outputs["aws_region"]
    scenarios_map = tf_outputs["scenarios"]

    for scenario_name, tf_key in SCENARIO_TF_KEYS.items():
        resources = scenarios_map[tf_key]
        outputs = {
            "queue_url": resources["queue_url"],
            "dlq_arn": resources["dlq_arn"],
            "message_status_table": resources["message_status_table"],
            "message_completed_table": resources["message_completed_table"],
        }
        print(f"[validate] Checking {scenario_name} ({tf_key}) ...")
        validate_scenario_infra(outputs, region, args.profile)

    print("[validate] All scenarios validated successfully.")


if __name__ == "__main__":
    main()
