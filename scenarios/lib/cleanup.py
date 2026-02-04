from __future__ import annotations

import time
from typing import Dict

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from lib.aws import get_queue_depth, queue_url_from_arn
from lib.constants import PURGE_SAFETY_DELAY


def purge_queue(sqs: BaseClient, queue_url: str, label: str) -> bool:
    """Purge all messages from a queue if it is non-empty.

    Skips the call when the queue is already empty to avoid the 60-second
    purge window that can interfere with newly sent messages. Returns True
    if a purge was issued, False if skipped.
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
    before returning so callers can safely send new messages.

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
