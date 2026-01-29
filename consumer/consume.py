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

    ## Two-Table Pattern for External Side Effects

    This class models how production systems handle external side effects
    (like calling Stripe, sending emails, etc.) that cannot be transacted
    with your own database.

    ### message_side_effects (The External Side Effect)
    - Represents the external side effect itself (e.g., "Stripe recorded a $50 transfer")
    - Writing to this table simulates calling an external service
    - Once written, the side effect "happened" — you can't undo it
    - Check this table BEFORE attempting the side effect to avoid duplicates

    ### message_status (Our Bookkeeping)
    - Tracks our orchestrator's knowledge: "we confirmed the side effect happened"
    - Updated to COMPLETED after we verify the side effect succeeded
    - If we crash between side effect and status update, retry will:
      1. Check side_effects table → see it exists → skip the external call
      2. Update status to COMPLETED
      3. Delete from queue

    ## The Pattern

    ```
    1. mark_started() → record STARTED in status table
    2. Check: does message_id exist in side_effects table?
       - If YES → side effect already done, skip to step 4
    3. DO SIDE EFFECT: write to side_effects table (simulates external call)
    4. [crash possible here]
    5. Update status to COMPLETED (our bookkeeping)
    6. Delete from SQS
    ```

    This models real-world scenarios where you can't transact with external
    systems. You must check before doing, and record after doing.
    """

    def __init__(
        self, resource: Any, status_table: str, side_effects_table: str
    ) -> None:
        self.resource = resource
        # message_status: our bookkeeping of processing lifecycle
        self.status_table = resource.Table(status_table)
        self.status_table_name = status_table
        # message_side_effects: represents the external side effect
        self.side_effects_table = resource.Table(side_effects_table)
        self.side_effects_table_name = side_effects_table

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

    def check_side_effect_exists(self, message_id: str) -> bool:
        """Check if the side effect was already performed.

        This is the critical check-before-doing step. Before calling an
        external service (or simulating one), check if we already did it.
        """
        try:
            response = self.side_effects_table.get_item(Key={"message_id": message_id})
            return "Item" in response
        except ClientError as exc:
            logger.warning("Failed to check side effect for %s: %s", message_id, exc)
            return False

    def do_side_effect(self, message_id: str, payload: Any) -> str:
        """Perform the side effect (simulates calling an external service).

        This represents the external call — like Stripe recording a transfer,
        or an email service sending a message. Once this succeeds, the side
        effect "happened" and cannot be undone.

        Returns: "new" if side effect was performed, "already_done" if it existed
        """
        timestamp = now_iso()
        digest = payload_digest(payload)

        try:
            self.side_effects_table.put_item(
                Item={
                    "message_id": message_id,
                    "performed_at": timestamp,
                    "payload_digest": digest,
                },
                ConditionExpression="attribute_not_exists(message_id)",
            )
            logger.info("Side effect performed for message_id=%s", message_id)
            return "new"
        except ClientError as exc:
            if self._is_conditional_failure(exc):
                logger.info("Side effect already exists for message_id=%s", message_id)
                return "already_done"
            logger.warning("Failed to perform side effect for %s: %s", message_id, exc)
            raise

    def mark_completed(self, message_id: str) -> None:
        """Update our bookkeeping to record that we confirmed the side effect.

        This is called AFTER the side effect succeeds (or we verify it already
        happened). It updates our status table to COMPLETED so we know we're done.
        """
        timestamp = now_iso()

        try:
            self.status_table.update_item(
                Key={"message_id": message_id},
                UpdateExpression=(
                    "SET #s = :completed, "
                    "last_updated = :now"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":completed": "COMPLETED",
                    ":now": timestamp,
                },
                ConditionExpression="attribute_exists(message_id)",
            )
        except ClientError as exc:
            if not self._is_conditional_failure(exc):
                logger.warning("Failed to update status to COMPLETED for %s: %s", message_id, exc)


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
    # Crash after side effect but before marking complete (for testing idempotency)
    crash_after_side_effect = bool(env_int("CRASH_AFTER_SIDE_EFFECT", 0))
    # Delay before side effect to simulate slow external service (for visibility timeout testing)
    side_effect_delay = env_float("SIDE_EFFECT_DELAY_SECONDS", 0.0)

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
    side_effects_table = os.getenv("MESSAGE_SIDE_EFFECTS_TABLE")
    if status_table and side_effects_table:
        dynamo_resource = build_dynamo_resource(region)
        dynamo_tracker = DynamoTracker(
            dynamo_resource, status_table, side_effects_table
        )
        logger.info(
            "DynamoDB tracking enabled | status_table=%s side_effects_table=%s",
            status_table,
            side_effects_table,
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
                        crash_rate,
                        crash_after_receive,
                        is_duplicate,
                        remember_message_id,
                        dynamo_tracker,
                        reject_payload_marker,
                        crash_after_side_effect,
                        side_effect_delay,
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
    crash_rate: float,
    crash_after_receive: bool,
    is_duplicate: Callable[[str], bool],
    remember_message_id: Callable[[str], None],
    dynamo_tracker: Optional[DynamoTracker],
    reject_payload_marker: str = "",
    crash_after_side_effect: bool = False,
    side_effect_delay: float = 0.0,
) -> None:
    """Process a single message using the check-before-doing pattern.

    The flow for external side effects:
    1. mark_started() → record STARTED in status table
    2. Check: does message_id exist in side_effects table?
       - If YES → side effect already done, skip to step 5
    3. DO SIDE EFFECT: write to side_effects table (simulates external call)
    4. [crash possible here — CRASH_AFTER_SIDE_EFFECT tests this]
    5. Update status to COMPLETED (our bookkeeping)
    6. Delete from SQS
    """
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

    # Step 1: Record that we're starting to process this message
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

    # Check if this message has been processed before (in-memory idempotency check)
    if message_id and is_duplicate(message_id):
        logger.warning("Duplicate detected; dropping message id=%s", message_id)
        delete_message(sqs_client, queue_url, message)
        return

    # Check if we should intentionally crash (chaos testing - simulates worker failure)
    if crash_rate > 0 and random.random() < crash_rate:
        raise CrashError("Intentional crash for testing")

    # Force a deterministic crash after receiving but before side effect (test hook)
    if crash_after_receive:
        raise CrashError("Intentional post-receive crash for testing")

    logger.info("Processing message id=%s payload=%s", message_id, payload)

    # Steps 2-5: The check-before-doing pattern for side effects
    side_effect_state = None
    if message_id and dynamo_tracker:
        # Step 2: Check if side effect was already performed
        if dynamo_tracker.check_side_effect_exists(message_id):
            # Side effect already done (we crashed after step 3 but before step 5)
            logger.info("Side effect already exists for id=%s, skipping to completion", message_id)
            side_effect_state = "already_done"
        else:
            # Optional delay to simulate slow external service (for visibility timeout testing)
            if side_effect_delay > 0:
                logger.info("Simulating slow external service: sleeping %.1fs", side_effect_delay)
                time.sleep(side_effect_delay)
            # Step 3: Perform the side effect (simulates external service call)
            side_effect_state = dynamo_tracker.do_side_effect(message_id, payload)

        # Step 4: Crash point for testing — side effect done but not marked complete
        if crash_after_side_effect:
            raise CrashError("Intentional crash after side effect for testing")

        # Step 5: Update our bookkeeping to record completion
        dynamo_tracker.mark_completed(message_id)

    # Step 6: Delete message from queue after successful processing
    delete_message(sqs_client, queue_url, message)

    # Remember this message ID for fast in-memory duplicate detection
    if message_id:
        remember_message_id(message_id)

    # Log successful completion
    if side_effect_state == "already_done":
        logger.info("Done message id=%s (side effect was already performed)", message_id)
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
