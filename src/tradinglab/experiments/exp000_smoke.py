# src/tradinglab/experiments/exp000_smoke.py
from __future__ import annotations

from typing import Any

import pandas as pd

from .base import ExperimentResult, RunContext


def _infer_time_bounds(df: pd.DataFrame) -> tuple[str | None, str | None]:
    """
    Try to infer start/end timestamps from either a 'timestamp' column or the index.
    Returns ISO8601 strings (or None if unavailable).
    """
    if df.empty:
        return None, None

    ts = None
    if "timestamp" in df.columns:
        ts = df["timestamp"]
    elif isinstance(df.index, pd.DatetimeIndex):
        ts = df.index

    if ts is None:
        return None, None

    # Ensure datetime-like
    try:
        ts_dt = pd.to_datetime(ts, utc=True, errors="coerce")
    except Exception:
        return None, None

    if ts_dt.isna().all():
        return None, None

    start = ts_dt.min()
    end = ts_dt.max()
    start_s = start.isoformat() if pd.notna(start) else None
    end_s = end.isoformat() if pd.notna(end) else None
    return start_s, end_s


def run(df: pd.DataFrame, ctx: RunContext) -> ExperimentResult:
    """
    Smoke experiment: validates the runner plumbing.
    Computes a few basic dataset/run metrics, emits a tiny results table
    and a short markdown report.
    """
    n_rows = int(len(df))
    n_cols = int(df.shape[1])

    start_ts, end_ts = _infer_time_bounds(df)

    metrics: dict[str, Any] = {
        "experiment": "exp000_smoke",
        "run_id": ctx.run_id,
        "n_rows": n_rows,
        "n_cols": n_cols,
        "start_ts": start_ts,
        "end_ts": end_ts,
    }

    # Single-row results table (handy for parquet/csv + quick inspection)
    results_df = pd.DataFrame([metrics])

    report_md = (
        "# Experiment: exp000_smoke\n\n"
        "Purpose: validate experiment runner and artifact pipeline.\n\n"
        "## Summary\n"
        f"- run_id: `{ctx.run_id}`\n"
        f"- rows: **{n_rows}**\n"
        f"- cols: **{n_cols}**\n"
        f"- start_ts: `{start_ts}`\n"
        f"- end_ts: `{end_ts}`\n"
    )

    # Optional: log something if logger exists and is configured
    try:
        ctx.logger.info("exp000_smoke finished: rows=%s cols=%s", n_rows, n_cols)
    except Exception:
        pass

    return ExperimentResult(metrics=metrics, results_df=results_df, report_md=report_md)
