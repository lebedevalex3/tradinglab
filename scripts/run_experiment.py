#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path

from tradinglab.experiments.runner import run_experiment


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TradingLab Experiment Runner (Milestone 1)")
    p.add_argument(
        "--config",
        type=str,
        default="configs/run.yml",
        help="Path to YAML config (default: configs/run.yml)",
    )
    p.add_argument(
        "--exp",
        type=str,
        default=None,
        help="Experiment name override (e.g., exp000_smoke)",
    )
    return p


def main() -> None:
    args = _build_arg_parser().parse_args()
    run_dir = run_experiment(Path(args.config), exp_override=args.exp)
    print(str(run_dir))


if __name__ == "__main__":
    main()
