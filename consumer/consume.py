"""SQS consumer with configurable failure/sleep knobs for demo experiments."""

import hashlib
import json
import logging
import os
import random
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, Optional

import boto3
from botocore.exceptions import ClientError


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


def build_dynamo_resource(region: Optional[str]) -> Any:
    """Create a boto3 DynamoDB resource scoped to the given region."""
    session = boto3.Session(region_name=region)
    return session.resource("dynamodb")


def build_dynamo_client(region: Optional[str]) -> Any:
    """Create a boto3 DynamoDB client scoped to the given region.

    Note: We use a dedicated client (not resource.meta.client) for
    transact_write_items because the resource's internal client has
    different serialization behavior that causes key validation errors.
    """
    session = boto3.Session(region_name=region)
    return session.client("dynamodb")


def now_iso() -> str:
    """UTC timestamp for table writes."""
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


def payload_digest(payload: Any) -> str:
    """Deterministic digest for the payload to help with debugging/idempotency."""
    try:
        rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    except TypeError:
        rendered = repr(payload)
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


class DynamoTracker:
    """DynamoDB-backed idempotency using a two-table pattern.

    ## Two-Table Idempotency Pattern

    This class uses two DynamoDB tables with distinct roles:

    ### message_status (The Side Effect / Processing State)
    - Tracks the processing lifecycle: STARTED → COMPLETED or FAILED
    - Records attempt counts for observability
    - In transactional mode, updating this to COMPLETED is the "side effect"
      that we want to happen exactly once

    ### message_completed (The Idempotency Gate)
    - A simple table with just message_id as the key
    - Written with `attribute_not_exists(message_id)` condition
    - If the write fails, we know this message was already processed
    - This is the authoritative "was this done?" check

    ## Why Two Tables?

    The two-table pattern provides defense in depth:
    1. `mark_started()` checks if status is already COMPLETED (fast path)
    2. `mark_completed_transactional()` uses a DynamoDB transaction to
       atomically update status AND write the completion record
    3. If we crash after the transaction but before SQS delete, the
       redelivered message will be caught by either check

    ## Transactional vs Non-Transactional Mode

    - **Non-transactional** (default): Two separate writes. A crash between
      them could leave inconsistent state (status=COMPLETED but no completion
      record, or vice versa).

    - **Transactional** (USE_TRANSACTIONAL_WRITES=1): Both writes happen
      atomically. Either both succeed or neither does. This guarantees the
      "side effect" (status update) only happens if the idempotency gate
      also succeeds.
    """

    def __init__(
        self, resource: Any, client: Any, status_table: str, completed_table: str
    ) -> None:
        self.resource = resource
        # Use a dedicated client for transact_write_items (not resource.meta.client)
        # because the resource's internal client has different serialization behavior
        self.client = client
        # message_status: tracks processing lifecycle, serves as "side effect" in txn mode
        self.status_table = resource.Table(status_table)
        self.status_table_name = status_table
        # message_completed: idempotency gate with conditional writes
        self.completed_table = resource.Table(completed_table)
        self.completed_table_name = completed_table

    @staticmethod
    def _is_conditional_failure(exc: ClientError) -> bool:
        return exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"

    def mark_started(self, message_id: str, payload: Any) -> str:
        """Record STARTED; return status state (new|existing|completed)."""
        timestamp = now_iso()
        digest = payload_digest(payload)
        try:
            self.status_table.put_item(
                Item={
                    "message_id": message_id,
                    "status": "STARTED",
                    "attempts": 1,
                    "last_updated": timestamp,
                    "payload_digest": digest,
                },
                ConditionExpression="attribute_not_exists(message_id)",
            )
            return "new"
        except ClientError as exc:
            if not self._is_conditional_failure(exc):
                logger.warning("Failed to write STARTED for %s: %s", message_id, exc)
                return "error"

        status = None
        try:
            response = self.status_table.get_item(Key={"message_id": message_id})
            status = response.get("Item", {}).get("status")
        except ClientError as exc:
            logger.warning("Failed to fetch existing status for %s: %s", message_id, exc)

        try:
            self.status_table.update_item(
                Key={"message_id": message_id},
                UpdateExpression="SET attempts = if_not_exists(attempts, :zero) + :one, last_updated=:now",
                ExpressionAttributeValues={
                    ":zero": 0,
                    ":one": 1,
                    ":now": timestamp,
                },
                ConditionExpression="attribute_exists(message_id)",
            )
        except ClientError as exc:
            if not self._is_conditional_failure(exc):
                logger.warning("Failed to bump attempts for %s: %s", message_id, exc)

        if status == "COMPLETED":
            return "completed"
        return "existing"

    def mark_completed(self, message_id: str, payload: Any) -> str:
        """Record completion with idempotent conditional writes."""
        timestamp = now_iso()
        digest = payload_digest(payload)
        completion_recorded = False

        try:
            self.completed_table.put_item(
                Item={
                    "message_id": message_id,
                    "processed_at": timestamp,
                    "payload_digest": digest,
                },
                ConditionExpression="attribute_not_exists(message_id)",
            )
            completion_recorded = True
        except ClientError as exc:
            if not self._is_conditional_failure(exc):
                logger.warning("Failed to write completion for %s: %s", message_id, exc)

        try:
            self.status_table.update_item(
                Key={"message_id": message_id},
                UpdateExpression=(
                    "SET #s = :completed, attempts = if_not_exists(attempts, :zero) + :one, "
                    "last_updated = :now, error_reason = :empty"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":completed": "COMPLETED",
                    ":one": 1,
                    ":zero": 0,
                    ":now": timestamp,
                    ":empty": "",
                },
                ConditionExpression="attribute_exists(message_id)",
            )
        except ClientError as exc:
            if not self._is_conditional_failure(exc):
                logger.warning("Failed to update status to COMPLETED for %s: %s", message_id, exc)

        return "new_completion" if completion_recorded else "duplicate_completion"

    def mark_completed_transactional(self, message_id: str, payload: Any) -> str:
        """Record completion atomically using DynamoDB transactions.

        This wraps both the side effect (updating message_status to COMPLETED)
        and the idempotency gate (writing to message_completed) in a single
        transaction. If the idempotency check fails, the entire transaction
        rolls back — no partial writes.
        """
        timestamp = now_iso()
        digest = payload_digest(payload)

        transact_items = [
            # The side effect: update status to COMPLETED
            {
                "Update": {
                    "TableName": self.status_table_name,
                    "Key": {"message_id": {"S": message_id}},
                    "UpdateExpression": "SET #s = :completed, last_updated = :now",
                    "ExpressionAttributeNames": {"#s": "status"},
                    "ExpressionAttributeValues": {
                        ":completed": {"S": "COMPLETED"},
                        ":now": {"S": timestamp},
                    },
                    "ConditionExpression": "attribute_exists(message_id)",
                }
            },
            # The idempotency gate: write completion record
            {
                "Put": {
                    "TableName": self.completed_table_name,
                    "Item": {
                        "message_id": {"S": message_id},
                        "processed_at": {"S": timestamp},
                        "payload_digest": {"S": digest},
                    },
                    "ConditionExpression": "attribute_not_exists(message_id)",
                }
            },
        ]

        try:
            self.client.transact_write_items(TransactItems=transact_items)
            return "new_completion"
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code == "TransactionCanceledException":
                # Check cancellation reasons to determine if it was the idempotency check
                reasons = exc.response.get("CancellationReasons", [])
                for reason in reasons:
                    if reason.get("Code") == "ConditionalCheckFailed":
                        logger.info(
                            "Transaction cancelled due to idempotency check for %s",
                            message_id,
                        )
                        return "duplicate_completion"
                logger.warning(
                    "Transaction cancelled for %s (reasons: %s)", message_id, reasons
                )
                return "duplicate_completion"
            logger.warning(
                "Failed transactional completion for %s: %s", message_id, exc
            )
            raise


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
    # Force crash immediately after receiving a message (deterministic crash for tests)
    crash_after_receive = bool(env_int("CRASH_AFTER_RECEIVE", 0))
    # Read size of idempotency cache (how many message IDs to remember, default 1000)
    idempotency_cache_size = env_int("IDEMPOTENCY_CACHE_SIZE", 1000)
    # Read optional message limit for testing (stop after N messages, default 0 = unlimited)
    message_limit = env_int("MESSAGE_LIMIT", 0)  # Optional stop-after-N for tests
    # Optional idle timeout: exit if no messages arrive for this many seconds (0 = disabled)
    idle_timeout_seconds = env_int("IDLE_TIMEOUT_SECONDS", 0)
    # Optional payload marker to reject messages (for poison message testing)
    reject_payload_marker = os.getenv("REJECT_PAYLOAD_MARKER", "")
    # Use transactional writes for atomic side effect + idempotency (default false)
    use_transactional_writes = bool(env_int("USE_TRANSACTIONAL_WRITES", 0))
    # Crash after side effect but before delete (for testing transactional idempotency)
    crash_after_side_effect = bool(env_int("CRASH_AFTER_SIDE_EFFECT", 0))

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
    # Optional Dynamo tracker for cross-consumer idempotency
    dynamo_tracker: Optional[DynamoTracker] = None
    status_table = os.getenv("MESSAGE_STATUS_TABLE")
    completed_table = os.getenv("MESSAGE_COMPLETED_TABLE")
    if status_table and completed_table:
        dynamo_resource = build_dynamo_resource(region)
        dynamo_client = build_dynamo_client(region)
        dynamo_tracker = DynamoTracker(
            dynamo_resource, dynamo_client, status_table, completed_table
        )
        logger.info(
            "DynamoDB tracking enabled | status_table=%s completed_table=%s",
            status_table,
            completed_table,
        )

    # Optional in-memory dedupe to simulate idempotency handling.
    # Create a bounded deque to track recent message IDs (FIFO with max size)
    seen_ids: Optional[Deque[str]] = (
        deque(maxlen=idempotency_cache_size) if idempotency_cache_size > 0 else None
    )
    # Create a set for O(1) duplicate lookups (complements the deque)
    seen_set: set[str] = set()
    # Track total messages processed (for limit checking and logging)
    processed_count = 0
    # Track last time a message was seen (for idle timeout)
    last_message_time = time.time()

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
                if idle_timeout_seconds > 0 and (time.time() - last_message_time) >= idle_timeout_seconds:
                    logger.info(
                        "Idle timeout of %ss reached with no messages; exiting",
                        idle_timeout_seconds,
                    )
                    return
                continue
            # Reset idle timer when messages arrive
            last_message_time = time.time()

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
                        crash_after_receive,
                        is_duplicate,
                        remember_message_id,
                        dynamo_tracker,
                        reject_payload_marker,
                        use_transactional_writes,
                        crash_after_side_effect,
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
    crash_after_receive: bool,
    is_duplicate: Callable[[str], bool],
    remember_message_id: Callable[[str], None],
    dynamo_tracker: Optional[DynamoTracker],
    reject_payload_marker: str = "",
    use_transactional_writes: bool = False,
    crash_after_side_effect: bool = False,
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

    tracker_state = None
    if message_id and dynamo_tracker:
        tracker_state = dynamo_tracker.mark_started(message_id, payload)
        if tracker_state == "completed":
            logger.info("Skipping already completed message id=%s", message_id)
            delete_message(sqs_client, queue_url, message)
            if message_id:
                remember_message_id(message_id)
            return

    # Reject poison messages (marker found in raw body)
    if reject_payload_marker and reject_payload_marker in body_raw:
        raise ValueError(f"Poison message rejected (marker={reject_payload_marker!r}, id={message_id})")

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

    # Force a deterministic crash after doing the work but before delete (test hook)
    if crash_after_receive:
        raise CrashError("Intentional post-receive crash for testing")

    completion_state = None
    if message_id and dynamo_tracker:
        if use_transactional_writes:
            completion_state = dynamo_tracker.mark_completed_transactional(
                message_id, payload
            )
        else:
            completion_state = dynamo_tracker.mark_completed(message_id, payload)

    # Crash after side effect but before delete (for testing transactional idempotency)
    if crash_after_side_effect:
        raise CrashError("Intentional crash after side effect for testing")

    # Delete message from queue after successful processing (prevents redelivery)
    delete_message(sqs_client, queue_url, message)

    # If we have a message ID, remember it for duplicate detection
    if message_id:
        remember_message_id(message_id)

    # Log successful completion of message processing
    if completion_state == "duplicate_completion":
        logger.info("Done message id=%s (detected duplicate completion)", message_id)
    else:
        logger.info("Done message id=%s", message_id)


def delete_message(sqs_client, queue_url: str, message: Dict[str, Any]) -> None:
    """Remove the processed message from the queue."""
    try:
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message["ReceiptHandle"],
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ReceiptHandleIsInvalid":
            logger.warning("Could not delete message (stale receipt handle); assuming it was already reclaimed")
            return
        raise


if __name__ == "__main__":
    main()
