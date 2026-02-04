from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Protocol

import argparse


@dataclass(frozen=True)
class ScenarioContext:
    outputs: Dict[str, str]
    region: str
    profile: str
    sqs: Any
    dynamo: Any


class ScenarioModule(Protocol):
    def register_args(self, parser: argparse.ArgumentParser) -> None:
        ...

    def run(self, args: argparse.Namespace, ctx: ScenarioContext) -> None:
        ...
