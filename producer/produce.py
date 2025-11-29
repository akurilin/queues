"""Utility script to push synthetic messages into an SQS queue.

The script is intended for load testing or demonstrations. It batches messages
and optionally throttles sends to a target rate.
"""

import argparse
import json
import logging
import os
import random
import time
import uuid
from typing import Any, List, Optional

import boto3
from botocore.client import BaseClient


def parse_args() -> argparse.Namespace:
    """Parse and return command-line arguments.

    Defaults are pulled from environment variables when available so the script
    can run without explicit flags.
    """

    parser = argparse.ArgumentParser(description="Send messages to an SQS queue")
    parser.add_argument(
        "--queue-url", default=os.getenv("QUEUE_URL"), help="Target queue URL"
    )
    parser.add_argument(
        "--profile", default=os.getenv("AWS_PROFILE"), help="AWS profile to use"
    )
    parser.add_argument("--region", default=os.getenv("AWS_REGION"), help="AWS region")
    parser.add_argument("--n", type=int, default=10, help="Number of messages to send")
    parser.add_argument(
        "--rate",
        type=float,
        default=0,
        help="Messages per second (0 for as fast as possible)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=10, help="Batch size per request (max 10)"
    )
    return parser.parse_args()


def build_sqs_client(region: Optional[str], profile: Optional[str]) -> BaseClient:
    """Construct an SQS client using the given region/profile configuration."""

    session = boto3.Session(region_name=region, profile_name=profile)
    return session.client("sqs")


def make_entries(start: int, count: int) -> List[dict[str, str]]:
    """Create a batch of SQS message entries with deterministic IDs."""

    entries: List[dict[str, str]] = []
    for i in range(start, start + count):
        payload = {
            "id": str(uuid.uuid4()),
            "work": random.randint(1, 1000),
        }
        entries.append(
            {
                "Id": str(i),
                "MessageBody": json.dumps(payload),
            }
        )
    return entries


def maybe_throttle(start_time: float, sent: int, rate: float) -> None:
    """Sleep if needed to keep average send rate at or below the target."""

    if rate <= 0:
        return
    expected = sent / rate
    actual = time.time() - start_time
    if actual < expected:
        time.sleep(expected - actual)


def main() -> None:
    """Entry point for sending a burst of synthetic messages to SQS."""

    # Parse command-line arguments (with fallback to environment variables)
    args = parse_args()
    # Configure logging to show timestamps and log levels for monitoring progress
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )

    # Validate that a queue URL was provided (required for sending messages)
    if not args.queue_url:
        logging.error("queue URL required via --queue-url or QUEUE_URL env var")
        # Exit early if no queue URL - cannot proceed without a target
        return

    # Clamp batch size between 1 and 10 (SQS send_message_batch has a max of 10 entries)
    batch_size = max(1, min(args.batch_size, 10))

    # Create an SQS client using the specified AWS region and profile
    sqs: BaseClient = build_sqs_client(args.region, args.profile)

    # Ensure total message count is non-negative (prevent negative values from args)
    total = max(args.n, 0)
    # Track successful message sends for final statistics
    sent = 0
    # Track failed message sends for error reporting
    failed = 0
    # Record start time to calculate elapsed time and actual send rate
    start_time = time.time()

    # Log initial configuration so user knows what the script will do
    logging.info(
        "Sending %s messages to %s (batch=%s rate=%s/sec)",
        total,
        args.queue_url,
        batch_size,
        args.rate,
    )

    # Initialize counter to track which message we're on (for batch entry IDs)
    message_index = 0
    # Loop until we've sent all requested messages
    while message_index < total:
        # Calculate how many messages to send in this batch (may be smaller at the end)
        chunk = min(batch_size, total - message_index)
        # Generate the batch of message entries with unique IDs and payloads
        entries = make_entries(message_index, chunk)
        # Send the batch to SQS and get response (may contain failures)
        response: dict[str, Any] = sqs.send_message_batch(
            QueueUrl=args.queue_url, Entries=entries
        )

        # Extract any failed entries from the SQS response (empty list if all succeeded)
        failed_entries = response.get("Failed", [])
        # Count how many messages failed in this batch
        batch_failed = len(failed_entries)
        # Calculate successful sends by subtracting failures from attempted sends
        batch_sent = chunk - batch_failed

        # Accumulate successful sends across all batches
        sent += batch_sent
        # Accumulate failures across all batches
        failed += batch_failed

        # Log warnings for any failures so user can see what went wrong
        if batch_failed:
            logging.warning("%s messages failed: %s", batch_failed, failed_entries)

        # Advance the index by the chunk size to process next batch
        message_index += chunk
        # Throttle if needed to maintain the target send rate (rate limiting)
        maybe_throttle(start_time, sent, args.rate)

    # Calculate total elapsed time (use small epsilon to prevent division by zero)
    elapsed = max(time.time() - start_time, 0.0001)
    # Calculate actual average send rate (messages per second)
    rate = sent / elapsed
    # Log final statistics showing total sent, failed, and actual throughput
    logging.info("Done. sent=%s failed=%s avg_rate=%.2f msg/sec", sent, failed, rate)


if __name__ == "__main__":
    main()
