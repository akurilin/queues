from __future__ import annotations

import argparse
import time
import subprocess
from typing import Dict

from lib.aws import queue_url_from_arn, get_queue_depth
from lib.cleanup import cleanup_scenario_state
from lib.consumer import run_local_consumer_async
from lib.producer import run_producer_async
from lib.waiters import ensure_queue_empty, wait_for_queue_empty
from lib.types import ScenarioContext


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--rate", type=int, default=5, help="Producer messages per second")
    parser.add_argument("--batch-size", type=int, default=10, help="Producer batch size (<=10)")
    parser.add_argument(
        "--poll-interval", type=int, default=5, help="Seconds between depth samples"
    )
    parser.add_argument(
        "--consumer-timeout",
        type=int,
        default=300,
        help="Timeout (seconds) for consumer subprocesses",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=30,
        help="Idle timeout for consumers to exit when queue is empty",
    )
    parser.add_argument(
        "--queue-timeout",
        type=int,
        default=300,
        help="Timeout (seconds) to wait for queue to drain",
    )


def run(args: argparse.Namespace, ctx: ScenarioContext) -> None:
    """Backpressure — auto-scale consumers against a continuous producer."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]
    dlq_url = queue_url_from_arn(outputs["dlq_arn"])
    status_table_name = outputs["message_status_table"]
    completed_table_name = outputs["message_side_effects_table"]

    cleanup_scenario_state(ctx.sqs, ctx.dynamo, outputs, "backpressure")

    prod_rate = args.rate
    batch_size = args.batch_size
    poll_interval = args.poll_interval

    consumer_env_base = {
        "QUEUE_URL": queue_url,
        "AWS_REGION": ctx.region,
        "MESSAGE_STATUS_TABLE": status_table_name,
        "MESSAGE_SIDE_EFFECTS_TABLE": completed_table_name,
        "SIDE_EFFECT_DELAY_SECONDS": "1.0",
        "IDLE_TIMEOUT_SECONDS": str(args.idle_timeout),
        "LOG_LEVEL": "WARNING",
        "MAX_MESSAGES": "1",
        "WAIT_TIME_SECONDS": "5",
    }

    print(f"[backpressure] Starting continuous producer at {prod_rate} msgs/sec ...")
    producer_proc = run_producer_async(
        count=0,
        batch_size=batch_size,
        region=ctx.region,
        queue_url=queue_url,
        profile=ctx.profile,
        rate=prod_rate,
        quiet=True,
    )

    print("[backpressure] Starting initial consumer ...")
    consumers: list[subprocess.Popen] = [
        run_local_consumer_async(consumer_env_base, quiet=True)
    ]
    peak_consumers = 1
    completed_table = ctx.dynamo.Table(completed_table_name)

    def _count_completed() -> int:
        count = 0
        scan_kw: Dict = {"Select": "COUNT"}
        while True:
            resp = completed_table.scan(**scan_kw)
            count += resp["Count"]
            if "LastEvaluatedKey" not in resp:
                break
            scan_kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        return count

    prev_completed = 0
    eq_streak = 0
    equilibrium_ticks = 3

    while True:
        time.sleep(poll_interval)

        completed = _count_completed()
        completed_delta = completed - prev_completed
        consume_rate = completed_delta / poll_interval
        prev_completed = completed

        producer_alive = producer_proc.poll() is None

        active_consumers = [p for p in consumers if p.poll() is None]

        status = "producing" if producer_alive else "draining"

        depth = get_queue_depth(ctx.sqs, queue_url)
        queue_depth = depth["visible"]

        print(
            f"[backpressure] {status} | "
            f"depth={queue_depth} completed={completed} "
            f"rate={consume_rate:.1f}/s consumers={len(active_consumers)}"
        )

        if producer_alive:
            if queue_depth < 10 and completed > 0:
                eq_streak += 1
                if eq_streak >= equilibrium_ticks:
                    print(
                        f"[backpressure] Queue depth in single digits "
                        f"(depth={queue_depth}) for {equilibrium_ticks} ticks "
                        f"— stopping producer"
                    )
                    producer_proc.terminate()
                    producer_proc.wait(timeout=10)
                    continue
            else:
                eq_streak = 0

            if queue_depth >= 10:
                new_proc = run_local_consumer_async(consumer_env_base, quiet=True)
                consumers.append(new_proc)
                active_consumers.append(new_proc)
                peak_consumers = max(peak_consumers, len(active_consumers))
                print(
                    f"[backpressure] SCALE UP → {len(active_consumers)} consumers"
                )
        else:
            if queue_depth == 0 and depth["not_visible"] == 0:
                print("[backpressure] Queue drained, exiting monitor loop")
                break

    print("[backpressure] Waiting for queue to fully drain ...")
    wait_for_queue_empty(ctx.sqs, queue_url, timeout=args.queue_timeout)

    print("[backpressure] Waiting for all consumers to exit ...")
    for proc in consumers:
        try:
            proc.wait(timeout=args.consumer_timeout)
        except subprocess.TimeoutExpired:
            print(f"[backpressure] Consumer pid={proc.pid} timed out, terminating ...")
            proc.terminate()
            proc.wait(timeout=10)

    total_completed = _count_completed()
    print(f"[backpressure] Running assertions (completed={total_completed}) ...")

    final_depth = get_queue_depth(ctx.sqs, queue_url)
    if final_depth["visible"] != 0 or final_depth["not_visible"] != 0:
        raise RuntimeError(
            f"Queue not empty: visible={final_depth['visible']} "
            f"not_visible={final_depth['not_visible']}"
        )

    ensure_queue_empty(ctx.sqs, dlq_url, "DLQ")

    if peak_consumers <= 1:
        raise RuntimeError(
            f"Expected scaling to occur (peak > 1), but peak_consumers={peak_consumers}"
        )

    if total_completed == 0:
        raise RuntimeError("No messages were completed")

    print(
        f"[backpressure] PASS | completed={total_completed} "
        f"peak_consumers={peak_consumers}"
    )
