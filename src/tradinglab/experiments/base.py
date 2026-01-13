# src/tradinglab/experiments/base.py
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True, slots=True)
class RunContext:
    """
    Immutable context passed to experiments.

    Attributes
    ----------
    run_id:
        Unique identifier of the run (e.g., "20260113_142233_exp000_smoke").
    run_dir:
        Directory where artifacts for this run should be written.
    config:
        Resolved configuration for the run (already merged/validated upstream).
    logger:
        Standard Python logger instance configured by the runner.
    started_at:
        Timestamp of when the run started (UTC recommended).
    """

    run_id: str
    run_dir: Path
    config: Mapping[str, Any]
    logger: Any  # logger-like (e.g., logging.Logger)
    started_at: datetime


@dataclass(frozen=True, slots=True)
class ExperimentResult:
    """
    Standardized output of an experiment.

    Attributes
    ----------
    metrics:
        Dictionary of scalar metrics to be serialized as JSON (JSON-serializable values).
    results_df:
        Optional tabular result to be persisted (parquet/csv).
    report_md:
        Markdown report content to be saved as report.md.
    """

    metrics: Mapping[str, Any]
    results_df: pd.DataFrame | None
    report_md: str
