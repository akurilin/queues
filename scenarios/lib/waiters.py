from __future__ import annotations

import time
from typing import Dict

from botocore.client import BaseClient

from lib.aws import get_queue_depth


def ensure_queue_empty(sqs: BaseClient, queue_url: str, label: str) -> None:
    """Single-shot assertion that a queue is empty.

    Only suitable for queues with no recent message operations (e.g. the DLQ
    after the primary queue has drained).
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


def wait_for_queue_depth(
    sqs: BaseClient,
    queue_url: str,
    expected: int,
    timeout: int = 30,
    poll_seconds: int = 2,
    confirmations: int = 2,
) -> Dict[str, int]:
    """Wait until queue depth reaches expected count or timeout.

    SQS approximate counts are eventually consistent, so we require
    *confirmations* consecutive matching readings before returning.

    Returns the final depth reading.
    """
    deadline = time.time() + timeout
    matches_seen = 0
    last_depth: Dict[str, int] = {"visible": -1, "not_visible": -1}
    while time.time() < deadline:
        last_depth = get_queue_depth(sqs, queue_url)
        total = last_depth["visible"] + last_depth["not_visible"]
        if total == expected:
            matches_seen += 1
            if matches_seen >= confirmations:
                return last_depth
        else:
            matches_seen = 0
        time.sleep(poll_seconds)
    raise RuntimeError(
        f"Queue depth did not reach {expected} within {timeout}s "
        f"(last seen: visible={last_depth['visible']}, not_visible={last_depth['not_visible']})"
    )
