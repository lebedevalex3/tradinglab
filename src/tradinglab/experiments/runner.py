# src/tradinglab/experiments/runner.py
from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import yaml  # PyYAML
except Exception as e:  # pragma: no cover
    yaml = None
    _YAML_IMPORT_ERROR = e

from tradinglab.experiments.base import RunContext
from tradinglab.experiments.registry import get_experiment
from tradinglab.reports.artifacts import (
    make_run_dir,
    write_metrics,
    write_report_md,
    write_resolved_config,
    write_results,
)


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _setup_logger(run_id: str) -> logging.Logger:
    logger = logging.getLogger(f"tradinglab.experiment.{run_id}")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers in repeated runs
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def _load_yaml(path: Path) -> dict[str, Any]:
    if yaml is None:  # pragma: no cover
        raise RuntimeError(
            "PyYAML is required to read configs. " f"Import error: {_YAML_IMPORT_ERROR}"
        )
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a YAML mapping (dict). Got: {type(data)}")
    return data


def _deep_get(cfg: Mapping[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = cfg
    for part in path.split("."):
        if not isinstance(cur, Mapping) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _resolve_config(raw_cfg: dict[str, Any], exp_override: str | None) -> dict[str, Any]:
    cfg = dict(raw_cfg)

    cfg.setdefault("experiment", {})
    cfg.setdefault("data", {})
    cfg.setdefault("artifacts", {})

    if exp_override:
        cfg["experiment"]["name"] = exp_override

    cfg["experiment"].setdefault("name", "exp000_smoke")
    cfg["data"].setdefault("sample_path", "data/sample/btcusdt_10m.parquet")
    cfg["artifacts"].setdefault("base_dir", "artifacts/runs")

    return cfg


def _load_sample_dataframe(sample_path: str | Path) -> pd.DataFrame:
    path = Path(sample_path)
    if not path.exists():
        raise FileNotFoundError(f"Sample parquet not found: {path}")

    df = pd.read_parquet(path)
    if df.empty:
        raise ValueError(f"Loaded dataframe is empty: {path}")
    return df


def _to_plain(obj: Any) -> Any:
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def run_experiment(config_path: Path, exp_override: str | None = None) -> Path:
    """
    Run one experiment and return the created run_dir path.
    """
    raw_cfg = _load_yaml(config_path)
    cfg = _resolve_config(raw_cfg, exp_override)

    exp_name: str = _deep_get(cfg, "experiment.name", "exp000_smoke")
    base_dir: str = _deep_get(cfg, "artifacts.base_dir", "artifacts/runs")
    sample_path: str = _deep_get(cfg, "data.sample_path", "data/sample/btcusdt_10m.parquet")

    run_id, run_dir = make_run_dir(base_dir=base_dir, exp_name=exp_name, now=_utcnow())
    logger = _setup_logger(run_id)
    started_at = _utcnow()

    logger.info("Starting experiment '%s'", exp_name)
    logger.info("Config: %s", config_path)
    logger.info("Sample: %s", sample_path)
    logger.info("Run dir: %s", run_dir)

    ctx = RunContext(
        run_id=run_id,
        run_dir=run_dir,
        config=cfg,
        logger=logger,
        started_at=started_at,
    )

    # Persist resolved config (defensive conversion)
    cfg_serializable: dict[str, Any] = {}
    for k, v in cfg.items():
        if isinstance(v, dict):
            cfg_serializable[k] = v
        else:
            cfg_serializable[k] = _to_plain(v)

    write_resolved_config(run_dir, cfg_serializable)

    df = _load_sample_dataframe(sample_path)

    exp_fn = get_experiment(exp_name)
    result = exp_fn(df, ctx)

    write_metrics(run_dir, dict(result.metrics))
    write_results(run_dir, result.results_df)
    write_report_md(run_dir, result.report_md)

    logger.info("Finished. Artifacts saved to: %s", run_dir)
    return run_dir
