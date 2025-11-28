"""End-to-end smoke test against a real SQS queue.

This test intentionally hits the live queue. Run only when the queue is idle and
you have AWS credentials configured. It spins up the consumer for a single
message (`MESSAGE_LIMIT=1`), sends a unique payload, and asserts the message is
gone afterwards.
"""

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Tuple

import boto3
import pytest


def load_env_file() -> None:
    """Load a simple KEY=VALUE .env file next to this test without new deps."""
    env_path = Path(__file__).with_name(".env")
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_env_file()

# Clean up empty AWS_PROFILE to avoid boto treating it as a real profile name.
if not os.environ.get("AWS_PROFILE", "").strip():
    os.environ.pop("AWS_PROFILE", None)

QUEUE_URL = os.getenv("QUEUE_URL")
AWS_REGION = os.getenv("AWS_REGION")
AWS_PROFILE = os.getenv("AWS_PROFILE")


def sqs_client() -> Any:
    profile = (AWS_PROFILE or "").strip() or None
    session = boto3.Session(region_name=AWS_REGION, profile_name=profile)
    return session.client("sqs")


def queue_counts(client: Any) -> Tuple[int, int]:
    attrs = client.get_queue_attributes(
        QueueUrl=QUEUE_URL,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )["Attributes"]
    visible = int(attrs.get("ApproximateNumberOfMessages", 0))
    not_visible = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
    return visible, not_visible


def wait_for_counts(client: Any, expect_visible: int, expect_not_visible: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        visible, not_visible = queue_counts(client)
        if visible == expect_visible and not_visible == expect_not_visible:
            return
        if time.monotonic() >= deadline:
            pytest.fail(
                f"Queue counts unexpected after timeout: "
                f"visible={visible} not_visible={not_visible} (expected {expect_visible}/{expect_not_visible})"
            )
        time.sleep(0.5)


@pytest.mark.skipif(not QUEUE_URL, reason="QUEUE_URL env var required for e2e test")
def test_consumer_processes_and_deletes_message(tmp_path: Path) -> None:
    client = sqs_client()

    visible, not_visible = queue_counts(client)
    if visible or not_visible:
        pytest.skip(
            f"Queue is not empty (visible={visible} not_visible={not_visible}); "
            "run when idle to avoid interfering with real traffic",
        )

    # Baseline sanity check: expect empty.
    wait_for_counts(client, expect_visible=0, expect_not_visible=0, timeout=3)

    message_id = str(uuid.uuid4())
    payload: Dict[str, str] = {"id": message_id, "test": "pytest-e2e"}
    client.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(payload))

    # After enqueue, expect exactly one visible, none in flight.
    wait_for_counts(client, expect_visible=1, expect_not_visible=0, timeout=5)

    env = os.environ.copy()
    env.update(
        {
            "QUEUE_URL": QUEUE_URL,
            "AWS_REGION": AWS_REGION or "",
            "MESSAGE_LIMIT": "1",  # stop after one processed message
            "WAIT_TIME_SECONDS": "1",
            "MAX_MESSAGES": "1",
            "SLEEP_MIN_SECONDS": "0",
            "SLEEP_MAX_SECONDS": "0",
            "LOG_LEVEL": "INFO",
        }
    )
    profile_clean = (AWS_PROFILE or "").strip()
    if profile_clean:
        env["AWS_PROFILE"] = profile_clean
    elif "AWS_PROFILE" in env:
        env.pop("AWS_PROFILE")

    consumer_proc = subprocess.Popen(
        [sys.executable, "app.py"],
        cwd=Path(__file__).parent,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        stdout, _ = consumer_proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        consumer_proc.kill()
        stdout, _ = consumer_proc.communicate()
        pytest.fail(f"consumer did not exit within timeout. logs:\n{stdout}")

    assert consumer_proc.returncode == 0, f"consumer failed: {stdout}"

    # Give SQS a moment to settle visibility before checking for leftovers.
    wait_for_counts(client, expect_visible=0, expect_not_visible=0, timeout=10)

    for _ in range(3):
        resp = client.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        messages = resp.get("Messages", [])
        if not messages:
            return

        for msg in messages:
            try:
                body = json.loads(msg.get("Body", "{}"))
            except json.JSONDecodeError:
                continue
            if isinstance(body, dict) and body.get("id") == message_id:
                pytest.fail(f"Message {message_id} still on queue: {msg}")

        # Let any messages we peeked at become visible again before next poll.
        time.sleep(1)
