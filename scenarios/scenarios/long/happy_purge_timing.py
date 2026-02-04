from __future__ import annotations

import argparse
import json
import time
from typing import Dict

from lib.cleanup import clear_dynamo_table
from lib.types import ScenarioContext


def register_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--iterations",
        type=int,
        default=2,
        help="Number of purge cycles to run (more iterations = better chance of observing behavior)",
    )
    parser.add_argument(
        "--danger-count",
        type=int,
        default=10,
        help="Messages to send immediately after purge (per iteration)",
    )
    parser.add_argument(
        "--safe-delay",
        type=int,
        default=65,
        help="Seconds after purge before sending safe messages (should be >60)",
    )


def run(args: argparse.Namespace, ctx: ScenarioContext) -> None:
    """SQS purge timing — verify 60-second danger window behavior."""
    outputs = ctx.outputs
    queue_url = outputs["queue_url"]

    print("[purge-timing] Clearing DynamoDB tables ...")
    clear_dynamo_table(ctx.dynamo, outputs["message_status_table"])
    clear_dynamo_table(ctx.dynamo, outputs["message_side_effects_table"])

    iterations = args.iterations
    danger_count_per_iter = args.danger_count
    safe_window_delay = args.safe_delay

    total_danger_sent = 0
    total_danger_survived = 0
    total_safe_sent = 0
    total_safe_survived = 0

    for iteration in range(1, iterations + 1):
        print(f"\n[purge-timing] === Iteration {iteration}/{iterations} ===")

        seed_count = 10
        print(f"[purge-timing] Seeding queue with {seed_count} messages ...")
        for i in range(seed_count):
            ctx.sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({"id": f"seed-{iteration}-{i}", "phase": "seed"}),
            )
        time.sleep(2)

        print("[purge-timing] Initiating purge ...")
        ctx.sqs.purge_queue(QueueUrl=queue_url)
        purge_start = time.time()

        print(f"[purge-timing] Sending {danger_count_per_iter} messages IMMEDIATELY after purge ...")
        for i in range(danger_count_per_iter):
            ctx.sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({
                    "id": f"danger-{iteration}-{i}",
                    "phase": "danger-window",
                    "iteration": iteration,
                }),
            )
        total_danger_sent += danger_count_per_iter

        intervals = [5, 15, 30, 45]
        for delay in intervals:
            elapsed = time.time() - purge_start
            sleep_needed = delay - elapsed
            if sleep_needed > 0:
                time.sleep(sleep_needed)
            ctx.sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({
                    "id": f"danger-{iteration}-t{delay}",
                    "phase": "danger-window",
                    "iteration": iteration,
                    "delay": delay,
                }),
            )
            total_danger_sent += 1
            print(f"[purge-timing]   Sent message at t+{delay}s")

        elapsed = time.time() - purge_start
        remaining_wait = max(0, safe_window_delay - elapsed)
        if remaining_wait > 0:
            print(f"[purge-timing] Waiting {remaining_wait:.0f}s for purge window to close ...")
            time.sleep(remaining_wait)

        safe_count = 5
        print(f"[purge-timing] Sending {safe_count} messages after purge window ...")
        for i in range(safe_count):
            ctx.sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({
                    "id": f"safe-{iteration}-{i}",
                    "phase": "after-purge",
                    "iteration": iteration,
                }),
            )
        total_safe_sent += safe_count

        time.sleep(2)
        iter_danger_survived = 0
        iter_safe_survived = 0

        while True:
            resp = ctx.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=2,
            )
            messages = resp.get("Messages", [])
            if not messages:
                break
            for msg in messages:
                body = json.loads(msg["Body"])
                if body.get("phase") == "danger-window":
                    iter_danger_survived += 1
                elif body.get("phase") == "after-purge":
                    iter_safe_survived += 1
                ctx.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

        total_danger_survived += iter_danger_survived
        total_safe_survived += iter_safe_survived

        iter_danger_sent = danger_count_per_iter + len(intervals)
        print(
            f"[purge-timing] Iteration {iteration} results: "
            f"danger={iter_danger_survived}/{iter_danger_sent} survived, "
            f"safe={iter_safe_survived}/{safe_count} survived"
        )

        if iter_danger_survived < iter_danger_sent:
            print(
                f"[purge-timing] >>> OBSERVED MESSAGE LOSS: {iter_danger_sent - iter_danger_survived} messages deleted by purge"
            )

    print(f"\n[purge-timing] === SUMMARY ({iterations} iterations) ===")
    total_danger_lost = total_danger_sent - total_danger_survived
    total_safe_lost = total_safe_sent - total_safe_survived

    print(
        f"[purge-timing] Danger window: {total_danger_survived}/{total_danger_sent} survived ({total_danger_lost} lost)"
    )
    print(
        f"[purge-timing] Safe window:   {total_safe_survived}/{total_safe_sent} survived ({total_safe_lost} lost)"
    )

    if total_safe_lost > 0:
        raise RuntimeError(
            f"UNEXPECTED: Messages sent after 65s purge window were lost! "
            f"lost={total_safe_lost}/{total_safe_sent}. "
            f"This contradicts AWS documentation."
        )

    if total_danger_lost > 0:
        print(
            f"[purge-timing] PASS | Confirmed purge danger window behavior: "
            f"{total_danger_lost}/{total_danger_sent} messages lost during window. "
            f"All {total_safe_sent} messages after window survived."
        )
    else:
        print(
            f"[purge-timing] PASS (inconclusive for same-run) | No message loss observed in {iterations} iterations. "
            f"All {total_safe_sent} safe-window messages survived as expected."
        )

    print(f"\n[purge-timing] === Cross-run simulation ===")
    print("[purge-timing] This simulates what happens when scenarios run back-to-back:")
    print("[purge-timing]   Run A purges at end -> Run B starts immediately -> sends messages")

    print("[purge-timing] Seeding queue ...")
    for i in range(20):
        ctx.sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"id": f"cross-seed-{i}", "phase": "seed"}),
        )
    time.sleep(2)

    print("[purge-timing] Purging (simulating end of Run A) ...")
    ctx.sqs.purge_queue(QueueUrl=queue_url)

    delays_to_test = [0, 2, 5, 10, 15, 30, 45, 60, 65]
    cross_run_results: Dict[int, Dict[str, int]] = {}
    purge_start = time.time()

    for delay in delays_to_test:
        elapsed = time.time() - purge_start
        sleep_needed = delay - elapsed
        if sleep_needed > 0:
            time.sleep(sleep_needed)

        batch_size = 5
        for i in range(batch_size):
            ctx.sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({
                    "id": f"cross-{delay}s-{i}",
                    "phase": "cross-run",
                    "delay": delay,
                }),
            )
        print(f"[purge-timing] Sent {batch_size} messages at t+{delay}s")
        cross_run_results[delay] = {"sent": batch_size, "survived": 0}

    print("[purge-timing] Waiting 10s for messages to settle ...")
    time.sleep(10)

    print("[purge-timing] Draining queue to count survivors ...")
    while True:
        resp = ctx.sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )
        messages = resp.get("Messages", [])
        if not messages:
            break
        for msg in messages:
            body = json.loads(msg["Body"])
            if body.get("phase") == "cross-run":
                delay = body.get("delay", -1)
                if delay in cross_run_results:
                    cross_run_results[delay]["survived"] += 1
            ctx.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

    print("\n[purge-timing] Cross-run results by delay after purge:")
    print("[purge-timing]   Delay | Sent | Survived | Lost")
    print("[purge-timing]   ------|------|----------|-----")
    any_cross_run_loss = False
    for delay in delays_to_test:
        r = cross_run_results[delay]
        lost = r["sent"] - r["survived"]
        status = "<<<" if lost > 0 else ""
        print(f"[purge-timing]   {delay:4}s | {r['sent']:4} | {r['survived']:8} | {lost:4} {status}")
        if lost > 0:
            any_cross_run_loss = True

    if any_cross_run_loss:
        print("\n[purge-timing] CONFIRMED: Messages CAN be lost when sent during purge window!")
    else:
        print("\n[purge-timing] No losses observed in this controlled test.")

    print("\n[purge-timing] === CONCLUSIONS ===")
    print("[purge-timing] AWS docs say purge 'may take up to 60 seconds' - it's non-deterministic.")
    print("[purge-timing] In controlled tests, purges often complete quickly (no losses).")
    print("[purge-timing] But in practice (back-to-back scenario runs), we HAVE observed losses.")
    print("[purge-timing]")
    print("[purge-timing] Key factors that may affect timing:")
    print("[purge-timing]   - Queue depth at purge time")
    print("[purge-timing]   - AWS region load")
    print("[purge-timing]   - Time since last purge (60s cooldown between purges)")
    print("[purge-timing]   - Message visibility state")
    print("[purge-timing]")
    print("[purge-timing] RECOMMENDATION: Always wait 60+ seconds after any purge before sending")
    print("[purge-timing] messages to that queue. Our cleanup uses this approach to stay safe.")
