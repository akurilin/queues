import argparse
import json
import logging
import os
import random
import time
import uuid
from typing import List

import boto3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send messages to an SQS queue")
    parser.add_argument("--queue-url", default=os.getenv("QUEUE_URL"), help="Target queue URL")
    parser.add_argument("--profile", default=os.getenv("AWS_PROFILE"), help="AWS profile to use")
    parser.add_argument("--region", default=os.getenv("AWS_REGION"), help="AWS region")
    parser.add_argument("--n", type=int, default=10, help="Number of messages to send")
    parser.add_argument("--rate", type=float, default=0, help="Messages per second (0 for as fast as possible)")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size per request (max 10)")
    return parser.parse_args()


def build_sqs_client(region: str, profile: str):
    session = boto3.Session(region_name=region, profile_name=profile)
    return session.client("sqs")


def make_entries(start: int, count: int) -> List[dict]:
    entries = []
    for i in range(start, start + count):
        payload = {
            "id": str(uuid.uuid4()),
            "work": random.randint(1, 1000),
        }
        entries.append({
            "Id": str(i),
            "MessageBody": json.dumps(payload),
        })
    return entries


def maybe_throttle(start_time: float, sent: int, rate: float) -> None:
    if rate <= 0:
        return
    expected = sent / rate
    actual = time.time() - start_time
    if actual < expected:
        time.sleep(expected - actual)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not args.queue_url:
        logging.error("queue URL required via --queue-url or QUEUE_URL env var")
        return

    batch_size = max(1, min(args.batch_size, 10))

    sqs = build_sqs_client(args.region, args.profile)

    total = max(args.n, 0)
    sent = 0
    failed = 0
    start_time = time.time()

    logging.info(
        "Sending %s messages to %s (batch=%s rate=%s/sec)",
        total,
        args.queue_url,
        batch_size,
        args.rate,
    )

    message_index = 0
    while message_index < total:
        chunk = min(batch_size, total - message_index)
        entries = make_entries(message_index, chunk)
        response = sqs.send_message_batch(QueueUrl=args.queue_url, Entries=entries)

        failed_entries = response.get("Failed", [])
        batch_failed = len(failed_entries)
        batch_sent = chunk - batch_failed

        sent += batch_sent
        failed += batch_failed

        if batch_failed:
            logging.warning("%s messages failed: %s", batch_failed, failed_entries)

        message_index += chunk
        maybe_throttle(start_time, sent, args.rate)

    elapsed = max(time.time() - start_time, 0.0001)
    rate = sent / elapsed
    logging.info("Done. sent=%s failed=%s avg_rate=%.2f msg/sec", sent, failed, rate)


if __name__ == "__main__":
    main()
