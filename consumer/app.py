"""SQS consumer with configurable failure/sleep knobs for demo experiments."""

import json
import logging
import os
import random
import sys
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, Optional

import boto3


class CrashError(Exception):
    """Raised when we intentionally crash the worker for chaos testing."""


logger = logging.getLogger("consumer")


def env_int(name: str, default: int) -> int:
    """Read an int from the environment with a safe fallback."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid int for %s=%s, using default %s", name, raw, default)
        return default


def env_float(name: str, default: float) -> float:
    """Read a float from the environment with a safe fallback."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float for %s=%s, using default %s", name, raw, default)
        return default


def get_log_level() -> int:
    """Resolve LOG_LEVEL to a logging level constant."""
    raw = os.getenv("LOG_LEVEL", "INFO").upper()
    return getattr(logging, raw, logging.INFO)


def build_sqs_client(region: Optional[str]) -> Any:
    """Create a boto3 SQS client scoped to the given region."""
    session = boto3.Session(region_name=region)
    return session.client("sqs")


def main() -> None:
    """Entry point: poll SQS with configurable behavior for demo/failure modes."""
    logging.basicConfig(
        level=get_log_level(),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    queue_url = os.getenv("QUEUE_URL")
    if not queue_url:
        logger.error("QUEUE_URL is required")
        sys.exit(1)

    region = os.getenv("AWS_REGION")
    wait_time = env_int("WAIT_TIME_SECONDS", 10)
    max_messages = env_int("MAX_MESSAGES", 5)
    max_messages = max(1, min(max_messages, 10))

    sleep_min = env_float("SLEEP_MIN_SECONDS", 0.1)
    sleep_max = env_float("SLEEP_MAX_SECONDS", 1.0)
    if sleep_max < sleep_min:
        sleep_max = sleep_min

    long_sleep_seconds = env_float("LONG_SLEEP_SECONDS", 0.0)
    long_sleep_every = env_int("LONG_SLEEP_EVERY", 0)

    crash_rate = env_float("CRASH_RATE", 0.0)
    idempotency_cache_size = env_int("IDEMPOTENCY_CACHE_SIZE", 1000)
    message_limit = env_int("MESSAGE_LIMIT", 0)  # Optional stop-after-N for tests

    logger.info(
        "Starting consumer | queue=%s wait=%ss max_messages=%s crash_rate=%.2f",
        queue_url,
        wait_time,
        max_messages,
        crash_rate,
    )

    sqs = build_sqs_client(region)

    # Optional in-memory dedupe to simulate idempotency handling.
    seen_ids: Optional[Deque[str]] = (
        deque(maxlen=idempotency_cache_size) if idempotency_cache_size > 0 else None
    )
    seen_set: set[str] = set()
    processed_count = 0

    def remember_message_id(message_id: str) -> None:
        if seen_ids is None:
            return
        if len(seen_ids) == seen_ids.maxlen:
            oldest = seen_ids.popleft()
            seen_set.discard(oldest)
        seen_ids.append(message_id)
        seen_set.add(message_id)

    def is_duplicate(message_id: str) -> bool:
        if seen_ids is None:
            return False
        return message_id in seen_set

    try:
        while True:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
            )

            messages = response.get("Messages", [])
            if not messages:
                continue

            for message in messages:
                processed_count += 1
                try:
                    handle_message(
                        sqs,
                        queue_url,
                        message,
                        processed_count,
                        sleep_min,
                        sleep_max,
                        long_sleep_seconds,
                        long_sleep_every,
                        crash_rate,
                        is_duplicate,
                        remember_message_id,
                    )
                    if message_limit and processed_count >= message_limit:
                        logger.info("Message limit %s reached; exiting", message_limit)
                        return
                except CrashError:
                    logger.error("Intentional crash triggered, exiting")
                    raise
                except Exception:
                    logger.exception("Failed to process message; will be retried")
    except KeyboardInterrupt:
        logger.info("Received interrupt, shutting down")
    except CrashError:
        # Already logged above; propagate to stop the task.
        raise
    except Exception:
        logger.exception("Fatal error in main loop")
        raise


def handle_message(
    sqs_client: Any,
    queue_url: str,
    message: Dict[str, Any],
    count: int,
    sleep_min: float,
    sleep_max: float,
    long_sleep_seconds: float,
    long_sleep_every: int,
    crash_rate: float,
    is_duplicate: Callable[[str], bool],
    remember_message_id: Callable[[str], None],
) -> None:
    """Process a single message with optional chaos behaviors."""
    body_raw = message.get("Body", "")
    try:
        payload = json.loads(body_raw)
    except json.JSONDecodeError:
        payload = {"raw": body_raw}

    message_id = None
    if isinstance(payload, dict):
        message_id = payload.get("id")
    message_id = message_id or message.get("MessageId")

    if message_id and is_duplicate(message_id):
        logger.warning("Duplicate detected; dropping message id=%s", message_id)
        delete_message(sqs_client, queue_url, message)
        return

    if crash_rate > 0 and random.random() < crash_rate:
        raise CrashError("Intentional crash for testing")

    sleep_time = random.uniform(sleep_min, sleep_max)
    if long_sleep_seconds > 0 and long_sleep_every > 0 and count % long_sleep_every == 0:
        sleep_time = max(sleep_time, long_sleep_seconds)
        logger.warning("Simulating long processing time: sleeping %.2fs", sleep_time)

    logger.info("Processing message id=%s payload=%s", message_id, payload)
    time.sleep(sleep_time)

    delete_message(sqs_client, queue_url, message)

    if message_id:
        remember_message_id(message_id)

    logger.info("Done message id=%s", message_id)


def delete_message(sqs_client, queue_url: str, message: Dict[str, Any]) -> None:
    """Remove the processed message from the queue."""
    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=message["ReceiptHandle"],
    )


if __name__ == "__main__":
    main()
