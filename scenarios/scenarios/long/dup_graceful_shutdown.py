from __future__ import annotations

import argparse
import subprocess
import time
from typing import Dict

from lib.aws import get_queue_depth, queue_url_from_arn
from lib.cleanup import cleanup_scenario_state
from lib.consumer import run_local_consumer_async
from lib.producer import run_producer
from lib.waiters import ensure_queue_empty, wait_for_messages_enqueued, wait_for_queue_depth
from lib.types import ScenarioContext


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--count", type=int, default=5, help="Messages to send")
    parser.add_argument(
        "--processing-delay",
        type=int,
        default=5,
        help="Seconds to simulate slow processing per message",
    )
    parser.add_argument(
        "--sigterm-delay",
        type=int,
        default=2,
        help="Seconds to wait before sending SIGTERM (consumer should be mid-processing)",
    )
    parser.add_argument(
        "--consumer-timeout",
        type=int,
        default=30,
        help="Timeout (seconds) to wait for consumer to exit after SIGTERM",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=30,
        help="Idle timeout for consumer (should not be reached)",
    )


def run(args: argparse.Namespace, ctx: ScenarioContext) -> None:
    """Graceful shutdown — consumer finishes in-flight work after SIGTERM."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    side_effects_table_name = outputs["message_side_effects_table"]

    cleanup_scenario_state(ctx.sqs, ctx.dynamo, outputs, "graceful-shutdown")

    message_count = args.count
    processing_delay = args.processing_delay
    sigterm_delay = args.sigterm_delay

    print(f"[graceful-shutdown] Sending {message_count} messages ...")
    run_producer(
        message_count,
        batch_size=min(10, message_count),
        region=ctx.region,
        queue_url=queue_url,
        profile=ctx.profile,
    )
    print("[graceful-shutdown] Confirming messages are enqueued ...")
    wait_for_messages_enqueued(ctx.sqs, queue_url, message_count)

    print(
        f"[graceful-shutdown] Starting consumer with {processing_delay}s processing delay ..."
    )
    consumer_env = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": ctx.region,
        "MESSAGE_SIDE_EFFECTS_TABLE": side_effects_table_name,
        "SIDE_EFFECT_DELAY_SECONDS": str(processing_delay),
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "INFO",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "1",
    }
    proc = run_local_consumer_async(consumer_env)

    print(f"[graceful-shutdown] Waiting up to {sigterm_delay}s for in-flight work ...")
    deadline = time.time() + sigterm_delay
    in_flight = False
    while time.time() < deadline:
        depth = get_queue_depth(ctx.sqs, queue_url)
        if depth["not_visible"] > 0:
            in_flight = True
            break
        time.sleep(0.5)
    if not in_flight:
        print(
            "[graceful-shutdown] WARNING: no in-flight message detected before SIGTERM; "
            "consumer may not be mid-processing."
        )

    print("[graceful-shutdown] Sending SIGTERM to consumer ...")
    sigterm_time = time.time()
    proc.terminate()

    try:
        proc.wait(timeout=args.consumer_timeout)
    except subprocess.TimeoutExpired:
        print("[graceful-shutdown] Consumer did not exit in time, killing ...")
        proc.kill()
        proc.wait(timeout=10)
        raise RuntimeError("Consumer did not exit gracefully after SIGTERM")

    exit_time = time.time()
    time_after_sigterm = exit_time - sigterm_time

    if proc.returncode != 0:
        raise RuntimeError(
            f"Consumer exited with code {proc.returncode}, expected 0 for graceful shutdown"
        )

    min_expected_processing_time = processing_delay - sigterm_delay - 1
    if time_after_sigterm < min_expected_processing_time:
        print(
            f"[graceful-shutdown] WARNING: Consumer exited {time_after_sigterm:.1f}s after SIGTERM, "
            f"expected ~{processing_delay - sigterm_delay}s to finish processing. "
            f"May not have been mid-processing when SIGTERM arrived."
        )
    else:
        print(
            f"[graceful-shutdown] Consumer exited {time_after_sigterm:.1f}s after SIGTERM "
            f"(finished in-flight work before exiting)"
        )

    side_effects_table = ctx.dynamo.Table(side_effects_table_name)
    side_effects_count = 0
    scan_kw: Dict = {"Select": "COUNT"}
    while True:
        resp = side_effects_table.scan(**scan_kw)
        side_effects_count += resp["Count"]
        if "LastEvaluatedKey" not in resp:
            break
        scan_kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if side_effects_count != 1:
        raise RuntimeError(
            f"Expected exactly 1 side effect (message processed before shutdown), "
            f"found {side_effects_count}"
        )

    scan_resp = side_effects_table.scan(Limit=1)
    if scan_resp.get("Items"):
        side_effect_record = scan_resp["Items"][0]
        performed_at = side_effect_record.get("performed_at", "")
        print(f"[graceful-shutdown] Side effect performed at: {performed_at}")

    expected_remaining = message_count - 1
    print(f"[graceful-shutdown] Waiting for queue depth to reach {expected_remaining} ...")
    try:
        wait_for_queue_depth(ctx.sqs, queue_url, expected_remaining, timeout=30)
    except RuntimeError as exc:
        raise RuntimeError(
            f"Expected {expected_remaining} messages remaining in queue: {exc}"
        ) from exc

    ensure_queue_empty(ctx.sqs, dlq_url, "DLQ")

    print(
        f"[graceful-shutdown] PASS | 1 message processed before shutdown, "
        f"{expected_remaining} messages remaining in queue"
    )
