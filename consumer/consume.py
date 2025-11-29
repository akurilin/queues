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
    # Configure logging with level from environment (allows DEBUG/INFO/WARNING/ERROR)
    logging.basicConfig(
        level=get_log_level(),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # Get queue URL from environment (required for SQS operations)
    queue_url = os.getenv("QUEUE_URL")
    # Validate that queue URL exists - cannot proceed without it
    if not queue_url:
        logger.error("QUEUE_URL is required")
        # Exit with error code 1 to indicate failure
        sys.exit(1)

    # Get AWS region from environment (optional, uses default if not set)
    region = os.getenv("AWS_REGION")
    # Read long polling wait time (how long to wait for messages, default 10s)
    wait_time = env_int("WAIT_TIME_SECONDS", 10)
    # Read maximum messages to fetch per request (default 5)
    max_messages = env_int("MAX_MESSAGES", 1)
    # Clamp max_messages between 1 and 10 (SQS API limit is 10 per receive call)
    max_messages = max(1, min(max_messages, 10))

    # Read minimum sleep time between processing messages (simulates work, default 0.1s)
    sleep_min = env_float("SLEEP_MIN_SECONDS", 0.1)
    # Read maximum sleep time between processing messages (default 1.0s)
    sleep_max = env_float("SLEEP_MAX_SECONDS", 1.0)
    # Ensure max is at least min (prevent invalid range)
    if sleep_max < sleep_min:
        sleep_max = sleep_min

    # Read long sleep duration for simulating slow processing (default 0 = disabled)
    long_sleep_seconds = env_float("LONG_SLEEP_SECONDS", 0.0)
    # Read how often to apply long sleep (every Nth message, default 0 = disabled)
    long_sleep_every = env_int("LONG_SLEEP_EVERY", 0)

    # Read crash rate for chaos testing (0.0-1.0 probability, default 0 = no crashes)
    crash_rate = env_float("CRASH_RATE", 0.0)
    # Read size of idempotency cache (how many message IDs to remember, default 1000)
    idempotency_cache_size = env_int("IDEMPOTENCY_CACHE_SIZE", 1000)
    # Read optional message limit for testing (stop after N messages, default 0 = unlimited)
    message_limit = env_int("MESSAGE_LIMIT", 0)  # Optional stop-after-N for tests

    # Log startup configuration so user knows what behavior is enabled
    logger.info(
        "Starting consumer | queue=%s wait=%ss max_messages=%s crash_rate=%.2f",
        queue_url,
        wait_time,
        max_messages,
        crash_rate,
    )

    # Create SQS client for making API calls
    sqs = build_sqs_client(region)

    # Optional in-memory dedupe to simulate idempotency handling.
    # Create a bounded deque to track recent message IDs (FIFO with max size)
    seen_ids: Optional[Deque[str]] = (
        deque(maxlen=idempotency_cache_size) if idempotency_cache_size > 0 else None
    )
    # Create a set for O(1) duplicate lookups (complements the deque)
    seen_set: set[str] = set()
    # Track total messages processed (for limit checking and logging)
    processed_count = 0

    # Inner function to add a message ID to the deduplication cache
    def remember_message_id(message_id: str) -> None:
        # Skip if idempotency is disabled (cache size is 0)
        if seen_ids is None:
            return
        # If cache is full, remove oldest entry to make room (FIFO eviction)
        if len(seen_ids) == seen_ids.maxlen:
            # Remove oldest ID from deque
            oldest = seen_ids.popleft()
            # Also remove from set to keep them in sync
            seen_set.discard(oldest)
        # Add new ID to end of deque (most recent)
        seen_ids.append(message_id)
        # Add to set for fast lookup
        seen_set.add(message_id)

    # Inner function to check if a message ID has been seen before
    def is_duplicate(message_id: str) -> bool:
        # If idempotency is disabled, never consider duplicates
        if seen_ids is None:
            return False
        # Check if ID exists in the set (O(1) lookup)
        return message_id in seen_set

    # Wrap main loop in try/except to handle graceful shutdown and errors
    try:
        # Infinite loop to continuously poll for messages
        while True:
            # Poll SQS for messages (long polling if wait_time > 0)
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
            )

            # Extract messages from response (empty list if none received)
            messages = response.get("Messages", [])
            # If no messages, continue to next poll iteration
            if not messages:
                continue

            # Process each message in the batch
            for message in messages:
                # Increment counter before processing (tracks total attempted)
                processed_count += 1
                # Wrap in try/except to handle per-message errors without stopping loop
                try:
                    # Process the message with all configured behaviors
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
                    # Check if we've hit the optional message limit (for testing)
                    if message_limit and processed_count >= message_limit:
                        logger.info("Message limit %s reached; exiting", message_limit)
                        # Exit gracefully when limit reached
                        return
                # Handle intentional crashes (chaos testing)
                except CrashError:
                    logger.error("Intentional crash triggered, exiting")
                    # Re-raise to stop the consumer (propagates to outer handler)
                    raise
                # Handle any other processing errors (log but continue)
                except Exception:
                    logger.exception("Failed to process message; will be retried")
    # Handle Ctrl+C gracefully (user-initiated shutdown)
    except KeyboardInterrupt:
        logger.info("Received interrupt, shutting down")
    # Handle intentional crashes from inner loop (already logged, just propagate)
    except CrashError:
        # Already logged above; propagate to stop the task.
        raise
    # Handle any other fatal errors (log and re-raise to crash the process)
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
    # Extract raw message body from SQS message structure
    body_raw = message.get("Body", "")
    # Try to parse body as JSON (expected format from producer)
    try:
        payload = json.loads(body_raw)
    # If not valid JSON, wrap raw string in a dict for consistent handling
    except json.JSONDecodeError:
        payload = {"raw": body_raw}

    # Initialize message_id to None (will try to extract from payload or message)
    message_id = None
    # If payload is a dict, try to get "id" field (custom message ID from producer)
    if isinstance(payload, dict):
        message_id = payload.get("id")
    # Fall back to SQS-generated MessageId if no custom ID found
    message_id = message_id or message.get("MessageId")

    # Check if this message has been processed before (idempotency check)
    if message_id and is_duplicate(message_id):
        logger.warning("Duplicate detected; dropping message id=%s", message_id)
        # Delete from queue to prevent reprocessing (already handled)
        delete_message(sqs_client, queue_url, message)
        # Exit early - don't process duplicate
        return

    # Check if we should intentionally crash (chaos testing - simulates worker failure)
    if crash_rate > 0 and random.random() < crash_rate:
        # Raise exception to crash the worker (will be caught in main loop)
        raise CrashError("Intentional crash for testing")

    # Calculate random sleep time to simulate variable processing duration
    sleep_time = random.uniform(sleep_min, sleep_max)
    # Check if we should apply a long sleep (simulates slow processing every Nth message)
    if (
        long_sleep_seconds > 0
        and long_sleep_every > 0
        and count % long_sleep_every == 0
    ):
        # Use the longer of random sleep or configured long sleep
        sleep_time = max(sleep_time, long_sleep_seconds)
        logger.warning("Simulating long processing time: sleeping %.2fs", sleep_time)

    # Log that we're starting to process this message
    logger.info("Processing message id=%s payload=%s", message_id, payload)
    # Sleep to simulate work being done (processing time)
    time.sleep(sleep_time)

    # Delete message from queue after successful processing (prevents redelivery)
    delete_message(sqs_client, queue_url, message)

    # If we have a message ID, remember it for duplicate detection
    if message_id:
        remember_message_id(message_id)

    # Log successful completion of message processing
    logger.info("Done message id=%s", message_id)


def delete_message(sqs_client, queue_url: str, message: Dict[str, Any]) -> None:
    """Remove the processed message from the queue."""
    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=message["ReceiptHandle"],
    )


if __name__ == "__main__":
    main()
