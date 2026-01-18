# src/tradinglab/reports/artifacts.py
from __future__ import annotations

import json
import re
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


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _slugify(value: str) -> str:
    """
    Keep run_id filesystem-friendly.
    """
    value = value.strip().lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9._-]+", "", value)
    return value or "run"


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _json_default(obj: Any) -> Any:
    """
    Make metrics JSON-serializable in a predictable way.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    if is_dataclass(obj):
        return asdict(obj)
    # pandas / numpy scalars sometimes appear in metrics
    try:
        import numpy as np  # type: ignore

        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
    except Exception:
        pass

    return str(obj)


def make_run_dir(
    *,
    base_dir: str | Path = "artifacts/runs",
    exp_name: str = "exp",
    now: datetime | None = None,
) -> tuple[str, Path]:
    """
    Create a unique run directory under base_dir and return (run_id, run_dir).

    run_id format: YYYYMMDD_HHMMSS_<exp_name>
    """
    base = Path(base_dir)
    _ensure_dir(base)

    ts = (now or _utcnow()).strftime("%Y%m%d_%H%M%S")
    exp_slug = _slugify(exp_name)
    run_id = f"{ts}_{exp_slug}"
    run_dir = base / run_id

    # Handle collisions (rare, but possible in tests / rapid runs)
    if run_dir.exists():
        suffix = _utcnow().strftime("%f")  # microseconds
        run_id = f"{run_id}_{suffix}"
        run_dir = base / run_id

    _ensure_dir(run_dir)
    return run_id, run_dir


def write_resolved_config(run_dir: Path, cfg: Mapping[str, Any]) -> Path:
    """
    Write resolved config as YAML to: config.resolved.yml
    """
    if yaml is None:  # pragma: no cover
        raise RuntimeError(f"PyYAML is required to write config.resolved.yml. Import error: {_YAML_IMPORT_ERROR}")

    path = Path(run_dir) / "config.resolved.yml"
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            dict(cfg),
            f,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )
    return path


def write_metrics(run_dir: Path, metrics: Mapping[str, Any]) -> Path:
    """
    Write metrics JSON to: metrics.json
    """
    path = Path(run_dir) / "metrics.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(
            dict(metrics),
            f,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=_json_default,
        )
        f.write("\n")
    return path


def write_results(
    run_dir: Path,
    df: pd.DataFrame | None,
    *,
    prefer_parquet: bool = True,
) -> Path:
    """
    Write tabular results.

    Default target: results.parquet
    Fallback: results.csv (if parquet engine is unavailable)
    """
    if df is None:
        # Still create a placeholder file to keep artifact contract stable
        placeholder = Path(run_dir) / "results.empty"
        placeholder.write_text("", encoding="utf-8")
        return placeholder

    df_out = df.copy()

    if prefer_parquet:
        parquet_path = Path(run_dir) / "results.parquet"
        try:
            df_out.to_parquet(parquet_path, index=False)
            return parquet_path
        except Exception:
            # Fall back to CSV if parquet engine isn't available
            pass

    csv_path = Path(run_dir) / "results.csv"
    df_out.to_csv(csv_path, index=False)
    return csv_path


def write_report_md(run_dir: Path, text: str) -> Path:
    """
    Write markdown report to: report.md
    """
    path = Path(run_dir) / "report.md"
    path.write_text(text, encoding="utf-8")
    return path
