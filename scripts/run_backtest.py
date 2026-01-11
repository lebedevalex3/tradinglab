# scripts/run_backtest.py
from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import yaml

# ---------- helpers ----------


def load_yaml(path: str | Path) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def get_git_commit() -> str:
    return os.getenv("GIT_COMMIT", "unknown")


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


# ---------- data structures ----------


@dataclass
class RunMeta:
    run_id: str
    created_at_utc: str
    git_commit: str
    strategy_version: str
    data_path: str
    params: dict


# ---------- main ----------


def main() -> None:
    parser = argparse.ArgumentParser(description="TradingLab smoke backtest")
    parser.add_argument("--config", required=True, help="Path to run config YAML")
    args = parser.parse_args()

    cfg = load_yaml(args.config)

    # paths
    data_path = Path(cfg["paths"]["data_path"])
    artifacts_dir = Path(cfg["paths"]["artifacts_dir"])

    # generate run id
    run_id = datetime.now(UTC).strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = artifacts_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # load data (smoke)
    df = pd.read_parquet(data_path)

    # empty trades (placeholder)
    trades = pd.DataFrame(columns=["timestamp", "side", "price", "qty"])

    # meta
    meta = RunMeta(
        run_id=run_id,
        created_at_utc=utc_now(),
        git_commit=get_git_commit(),
        strategy_version=cfg["strategy_version"],
        data_path=str(data_path),
        params=cfg.get("params", {}),
    )

    # write artifacts
    (run_dir / "meta.json").write_text(
        json.dumps(asdict(meta), indent=2),
        encoding="utf-8",
    )

    trades.to_parquet(run_dir / "trades.parquet", index=False)

    summary = {
        "rows_in_data": len(df),
        "trades": 0,
        "note": "Milestone 0 smoke run",
    }

    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    report = f"""# TradingLab — Run Report

Run ID: {run_id}
Strategy version: {meta.strategy_version}
Git commit: {meta.git_commit}

Data:
- path: {data_path}
- rows: {len(df)}

Trades: 0

Status: OK (smoke run)
"""
    (run_dir / "report.md").write_text(report, encoding="utf-8")

    print(f"OK: run created at {run_dir}")


if __name__ == "__main__":
    main()
