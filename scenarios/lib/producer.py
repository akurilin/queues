from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def run_producer_async(
    count: int,
    batch_size: int,
    region: str,
    queue_url: str,
    profile: str,
    rate: Optional[int] = None,
    poison_count: int = 0,
    poison_every: int = 0,
    quiet: bool = False,
) -> subprocess.Popen:
    """Start the producer script as a background subprocess. Returns the Popen handle."""
    cmd = [
        sys.executable,
        str(REPO_ROOT / "producer" / "produce.py"),
        "--queue-url",
        queue_url,
        "--region",
        region,
        "--n",
        str(count),
        "--batch-size",
        str(batch_size),
    ]
    if rate is not None:
        cmd.extend(["--rate", str(rate)])
    if poison_every > 0:
        cmd.extend(["--poison-every", str(poison_every)])
    elif poison_count > 0:
        cmd.extend(["--poison-count", str(poison_count)])
    if profile:
        cmd.extend(["--profile", profile])
    if not quiet:
        print(f"[producer] Starting producer in background (n={count}, rate={rate}) ...")
    kwargs: Dict = {}
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
    return subprocess.Popen(cmd, **kwargs)


def run_producer(
    count: int,
    batch_size: int,
    region: str,
    queue_url: str,
    profile: str,
    poison_count: int = 0,
    poison_every: int = 0,
) -> None:
    """Invoke the producer script to push messages."""
    cmd = [
        sys.executable,
        str(REPO_ROOT / "producer" / "produce.py"),
        "--queue-url",
        queue_url,
        "--region",
        region,
        "--n",
        str(count),
        "--batch-size",
        str(batch_size),
    ]
    if poison_every > 0:
        cmd.extend(["--poison-every", str(poison_every)])
    elif poison_count > 0:
        cmd.extend(["--poison-count", str(poison_count)])
    if profile:
        cmd.extend(["--profile", profile])
    subprocess.run(cmd, check=True)
