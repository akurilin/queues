from __future__ import annotations

from typing import Dict, Optional

import boto3
from botocore.client import BaseClient


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
