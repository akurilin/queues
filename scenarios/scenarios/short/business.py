from __future__ import annotations

import argparse
import json
import uuid
from typing import Dict

from lib.aws import queue_url_from_arn
from lib.cleanup import cleanup_scenario_state
from lib.consumer import run_local_consumer
from lib.waiters import ensure_queue_empty, wait_for_messages_enqueued, wait_for_queue_empty
from lib.types import ScenarioContext


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=30,
        help="Idle timeout for consumer to exit when queue is empty",
    )
    parser.add_argument(
        "--consumer-timeout",
        type=int,
        default=120,
        help="Timeout (seconds) for consumer subprocess",
    )
    parser.add_argument(
        "--queue-timeout",
        type=int,
        default=120,
        help="Timeout (seconds) to wait for queue to drain",
    )


def run(args: argparse.Namespace, ctx: ScenarioContext) -> None:
    """Business idempotency — two message IDs for the same logical work."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]
    status_table_name = outputs["message_status_table"]
    side_effects_table_name = outputs["message_side_effects_table"]

    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    cleanup_scenario_state(ctx.sqs, ctx.dynamo, outputs, "business-idempotency")

    order_id = f"order-{uuid.uuid4()}"
    payloads = [
        {"id": str(uuid.uuid4()), "order_id": order_id, "work": "charge"},
        {"id": str(uuid.uuid4()), "order_id": order_id, "work": "charge"},
    ]

    print(f"[business-idempotency] Sending 2 messages for order_id={order_id} ...")
    for payload in payloads:
        ctx.sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(payload))
    wait_for_messages_enqueued(ctx.sqs, queue_url, expected=2)

    print("[business-idempotency] Running consumer with BUSINESS_IDEMPOTENCY_FIELD=order_id ...")
    exit_code = run_local_consumer(
        env={
            "QUEUE_URL": queue_url,
            "AWS_REGION": ctx.region,
            "MESSAGE_STATUS_TABLE": status_table_name,
            "MESSAGE_SIDE_EFFECTS_TABLE": side_effects_table_name,
            "BUSINESS_IDEMPOTENCY_FIELD": "order_id",
            "MESSAGE_LIMIT": "2",
            "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
            "LOG_LEVEL": "INFO",
            "MAX_MESSAGES": "1",
            "WAIT_TIME_SECONDS": "1",
        },
        timeout=args.consumer_timeout,
    )
    if exit_code != 0:
        raise RuntimeError(f"Business idempotency consumer failed with exit code {exit_code}")

    print("[business-idempotency] Waiting for queue to drain ...")
    wait_for_queue_empty(ctx.sqs, queue_url, timeout=args.queue_timeout)
    ensure_queue_empty(ctx.sqs, dlq_url, "DLQ")

    side_effects_table = ctx.dynamo.Table(side_effects_table_name)
    side_effects_count = 0
    scan_kw: Dict = {"Select": "COUNT"}
    while True:
        resp = side_effects_table.scan(**scan_kw)
        side_effects_count += resp["Count"]
        if "LastEvaluatedKey" not in resp:
            break
        scan_kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    expected_key = f"biz:order_id:{order_id}"
    side_effect_item = side_effects_table.get_item(
        Key={"message_id": expected_key}
    ).get("Item")

    if side_effects_count != 1 or not side_effect_item:
        raise RuntimeError(
            f"Expected 1 side effect for business key {expected_key}, "
            f"found count={side_effects_count}"
        )

    status_table = ctx.dynamo.Table(status_table_name)
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

    if status_count != 2:
        raise RuntimeError(f"Expected 2 COMPLETED statuses, found {status_count}")

    print(
        f"[business-idempotency] PASS | order_id={order_id} side_effects=1 statuses=2"
    )
