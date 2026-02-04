from __future__ import annotations

import os
import subprocess
import sys
from typing import Dict

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CONSUME_SCRIPT = REPO_ROOT / "consumer" / "consume.py"


def run_local_consumer(env: Dict[str, str], timeout: int) -> int:
    """Run consume.py as a blocking subprocess. Returns exit code."""
    full_env = {**os.environ, **env}
    print(f"[consumer] Running {CONSUME_SCRIPT} locally ...")
    result = subprocess.run(
        [sys.executable, str(CONSUME_SCRIPT)],
        env=full_env,
        timeout=timeout,
    )
    return result.returncode


def run_local_consumer_async(
    env: Dict[str, str], quiet: bool = False
) -> subprocess.Popen:
    """Start consume.py as a background subprocess. Returns the Popen handle."""
    full_env = {**os.environ, **env}
    if not quiet:
        print(f"[consumer] Starting {CONSUME_SCRIPT} in background ...")
    kwargs: Dict = {}
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
    return subprocess.Popen(
        [sys.executable, str(CONSUME_SCRIPT)],
        env=full_env,
        **kwargs,
    )
