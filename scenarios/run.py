"""Scenario runner for queue behaviors against pre-provisioned infrastructure."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

from lib.aws import build_dynamo_resource, build_sqs_client
from lib.types import ScenarioContext, ScenarioModule
from validate_infra import validate_scenario_infra

SCENARIO_ROOT = Path(__file__).resolve().parent
REPO_ROOT = SCENARIO_ROOT.parent
TERRAFORM_DIR = REPO_ROOT / "terraform"
SCENARIOS_DIR = SCENARIO_ROOT / "scenarios"

if str(SCENARIO_ROOT) not in sys.path:
    sys.path.insert(0, str(SCENARIO_ROOT))

@dataclass(frozen=True)
class ScenarioInfo:
    cli_name: str
    stem: str
    category: str
    path: Path
    module: ScenarioModule


# ---------------------------------------------------------------------------
# Terraform outputs
# ---------------------------------------------------------------------------


def read_terraform_outputs() -> Dict:
    """Read outputs from the single terraform/ directory.

    Returns a dict with 'aws_region' and 'scenarios' (a map of scenario key
    to its resource references).
    """
    result = subprocess.run(
        ["terraform", f"-chdir={TERRAFORM_DIR}", "output", "-json"],
        capture_output=True,
        text=True,
        check=True,
    )
    raw = json.loads(result.stdout)
    return {k: v["value"] for k, v in raw.items()}


# ---------------------------------------------------------------------------
# Scenario discovery
# ---------------------------------------------------------------------------


def _cli_name_from_stem(stem: str) -> str:
    return stem.replace("_", "-")


def _help_from_module(module: ScenarioModule, fallback: str) -> str:
    doc = getattr(module, "run", None)
    if doc and getattr(doc, "__doc__"):
        return doc.__doc__.strip().splitlines()[0]
    return fallback


def _load_module(path: Path) -> ScenarioModule:
    module_name = f"scenario_{path.parent.name}_{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if not spec or not spec.loader:
        raise RuntimeError(f"Unable to load scenario module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[assignment]

    register_args = getattr(module, "register_args", None)
    run = getattr(module, "run", None)
    if not callable(register_args) or not callable(run):
        raise RuntimeError(
            f"Scenario module {path} must define register_args(parser) and run(args, ctx)"
        )

    return module  # type: ignore[return-value]


def discover_scenarios() -> List[ScenarioInfo]:
    scenarios: List[ScenarioInfo] = []
    for category in ("short", "long"):
        category_dir = SCENARIOS_DIR / category
        if not category_dir.exists():
            continue
        for path in sorted(category_dir.glob("*.py")):
            if path.name.startswith("_"):
                continue
            module = _load_module(path)
            stem = path.stem
            cli_name = _cli_name_from_stem(stem)
            scenarios.append(
                ScenarioInfo(
                    cli_name=cli_name,
                    stem=stem,
                    category=category,
                    path=path,
                    module=module,
                )
            )
    if not scenarios:
        raise RuntimeError("No scenarios discovered")
    return scenarios


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(scenarios: Iterable[ScenarioInfo]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run queue scenarios")
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available scenarios and exit",
    )
    subparsers = parser.add_subparsers(dest="scenario")

    for info in scenarios:
        help_text = _help_from_module(info.module, f"{info.cli_name} scenario")
        sub = subparsers.add_parser(info.cli_name, help=help_text)
        sub.set_defaults(_scenario=info.cli_name)
        info.module.register_args(sub)

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _terraform_key_for(stem: str, terraform_keys: set[str]) -> str:
    if stem in terraform_keys:
        return stem
    if "_" in stem:
        prefix = stem.split("_", 1)[0]
        if prefix in terraform_keys:
            return prefix
    raise RuntimeError(
        f"Scenario '{stem}' does not map to a terraform key. "
        f"Expected one of: {sorted(terraform_keys)}"
    )


def main() -> None:
    scenarios = discover_scenarios()
    scenario_map = {s.cli_name: s for s in scenarios}

    args = parse_args(scenarios)
    if getattr(args, "list", False):
        short = [s for s in scenarios if s.category == "short"]
        long = [s for s in scenarios if s.category == "long"]
        print("Short scenarios:")
        for info in short:
            print(f"  {info.cli_name}")
        print("Long scenarios:")
        for info in long:
            print(f"  {info.cli_name}")
        return

    if not args.scenario:
        raise SystemExit("Scenario name required. Use --list to see options.")
    scenario_name = getattr(args, "_scenario", None) or args.scenario
    info = scenario_map.get(scenario_name)
    if not info:
        raise SystemExit(f"Unknown scenario {scenario_name}")

    tf_outputs = read_terraform_outputs()
    region = tf_outputs["aws_region"]

    terraform_keys = set(tf_outputs["scenarios"].keys())
    tf_key = _terraform_key_for(info.stem, terraform_keys)
    scenario_resources = tf_outputs["scenarios"][tf_key]

    outputs = {
        "queue_url": scenario_resources["queue_url"],
        "dlq_arn": scenario_resources["dlq_arn"],
        "message_status_table": scenario_resources["message_status_table"],
        "message_side_effects_table": scenario_resources["message_side_effects_table"],
        "aws_region": region,
    }

    profile = os.environ.get("AWS_PROFILE", "")

    print(f"[run] Validating {scenario_name} infrastructure ...")
    validate_scenario_infra(outputs, region, profile)

    sqs = build_sqs_client(region, profile)
    dynamo = build_dynamo_resource(region, profile)
    ctx = ScenarioContext(outputs=outputs, region=region, profile=profile, sqs=sqs, dynamo=dynamo)

    info.module.run(args, ctx)


if __name__ == "__main__":
    main()
