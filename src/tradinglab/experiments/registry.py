# src/tradinglab/experiments/registry.py
from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from .base import ExperimentResult, RunContext
from .exp000_smoke import run as exp000_smoke_run
from .exp001_adx_bins import run as exp001_adx_bins_run

ExperimentFn = Callable[[pd.DataFrame, RunContext], ExperimentResult]

EXPERIMENTS: dict[str, ExperimentFn] = {
    "exp000_smoke": exp000_smoke_run,
    "exp001_adx_bins": exp001_adx_bins_run,
}


def get_experiment(name: str) -> ExperimentFn:
    """
    Return experiment function by name.
    Raises KeyError with a helpful message if not found.
    """
    if name not in EXPERIMENTS:
        available = ", ".join(sorted(EXPERIMENTS.keys())) or "<none>"
        raise KeyError(f"Unknown experiment '{name}'. Available: {available}")
    return EXPERIMENTS[name]
