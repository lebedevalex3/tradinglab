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

# Run artifacts must follow docs/run-contract.md

REQUIRED_OHLCV_COLS = {"timestamp", "open", "high", "low", "close", "volume"}


# ---------- helpers ----------


def dump_yaml(obj: dict, path: Path) -> None:
    path.write_text(yaml.safe_dump(obj, sort_keys=False), encoding="utf-8")


def load_yaml(path: str | Path) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def get_git_commit() -> str:
    return os.getenv("GIT_COMMIT", "unknown")


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def ensure_config(cfg: dict) -> None:
    if "strategy_version" not in cfg:
        raise ValueError("Config missing key: strategy_version")
    if "paths" not in cfg or not isinstance(cfg["paths"], dict):
        raise ValueError("Config missing section: paths")
    if "data_path" not in cfg["paths"]:
        raise ValueError("Config missing key: paths.data_path")
    if "artifacts_dir" not in cfg["paths"]:
        raise ValueError("Config missing key: paths.artifacts_dir")


def validate_ohlcv(df: pd.DataFrame) -> None:
    cols = set(df.columns)
    missing = REQUIRED_OHLCV_COLS - cols
    if missing:
        raise ValueError(f"OHLCV data missing columns: {sorted(missing)}")
    if df["timestamp"].isna().any():
        raise ValueError("OHLCV data has NaN in timestamp")
    # минимально: проверка сортировки
    if not df["timestamp"].is_monotonic_increasing:
        raise ValueError("OHLCV timestamp is not monotonic increasing")


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
    ensure_config(cfg)

    # paths
    data_path = Path(cfg["paths"]["data_path"])
    artifacts_dir = Path(cfg["paths"]["artifacts_dir"])

    # generate run id
    run_id = datetime.now(UTC).strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = artifacts_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # Save config snapshot to run folder
    dump_yaml(cfg, run_dir / "config.yml")

    # load data (smoke)
    df = pd.read_parquet(data_path)
    validate_ohlcv(df)

    # empty trades (placeholder)
    trades = pd.DataFrame(columns=["timestamp", "side", "price", "qty"])
    trades.to_parquet(run_dir / "trades.parquet", index=False)

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
- columns_ok: {sorted(REQUIRED_OHLCV_COLS)}

Trades: 0

Status: OK (smoke run)
"""
    (run_dir / "report.md").write_text(report, encoding="utf-8")

    # Write pointer to latest run
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "latest.txt").write_text(run_id, encoding="utf-8")

    print(f"OK: run created at {run_dir}")


if __name__ == "__main__":
    main()
