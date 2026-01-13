# src/tradinglab/features/returns.py
from __future__ import annotations

from collections.abc import Sequence

import pandas as pd


def add_forward_returns(
    df: pd.DataFrame,
    *,
    horizons: Sequence[int] = (1, 3, 6, 12, 24),
    close_col: str = "close",
    prefix: str = "fwd_ret_",
) -> pd.DataFrame:
    """
    Adds forward simple returns:
        fwd_ret_h = close.shift(-h) / close - 1

    Parameters
    ----------
    horizons:
        Iterable of positive integers (bars ahead).
    """
    if close_col not in df.columns:
        raise KeyError(f"Missing required column '{close_col}'")

    if not horizons:
        raise ValueError("horizons must be non-empty")

    for h in horizons:
        if not isinstance(h, int) or h <= 0:
            raise ValueError(f"Invalid horizon: {h}. Must be positive int.")

    out = df.copy()
    close = pd.to_numeric(out[close_col], errors="coerce")

    for h in horizons:
        out[f"{prefix}{h}"] = close.shift(-h) / close - 1.0

    return out
