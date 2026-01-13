# src/tradinglab/features/adx.py
from __future__ import annotations

import numpy as np
import pandas as pd


def _wilder_rma(values: pd.Series, length: int) -> pd.Series:
    """
    Wilder's RMA (ta.rma-like):
    - first defined value at index length-1 is SMA of first `length` samples
    - then recursive: rma[i] = rma[i-1] + (x[i] - rma[i-1]) / length
    """
    if length <= 0:
        raise ValueError("length must be > 0")

    x = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    out = np.full_like(x, np.nan, dtype=float)

    if len(x) < length:
        return pd.Series(out, index=values.index, name=values.name)

    first = np.nanmean(x[:length])
    out[length - 1] = first

    for i in range(length, len(x)):
        prev = out[i - 1]
        if np.isnan(prev):
            out[i] = np.nan
        else:
            out[i] = prev + (x[i] - prev) / length

    return pd.Series(out, index=values.index, name=values.name)


def _true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)


def add_dmi_adx(
    df: pd.DataFrame,
    *,
    length: int = 14,
    adx_smoothing: int | None = None,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    prefix: str = "",
) -> pd.DataFrame:
    """
    Add DMI/ADX columns to df using Wilder smoothing.

    Adds:
      - {prefix}pdi
      - {prefix}mdi
      - {prefix}adx
      (and internal {prefix}dx optionally kept local)

    Parameters
    ----------
    length:
        DMI length (Wilder smoothing length for TR and DM).
    adx_smoothing:
        ADX smoothing length. If None, uses `length`.
        Mirrors TradingView ta.dmi(len, adxSmoothing) behavior.
    """
    if adx_smoothing is None:
        adx_smoothing = length
    if length <= 0 or adx_smoothing <= 0:
        raise ValueError("length and adx_smoothing must be > 0")

    for col in (high_col, low_col, close_col):
        if col not in df.columns:
            raise KeyError(f"Missing required column '{col}'")

    out = df.copy()

    high = pd.to_numeric(out[high_col], errors="coerce")
    low = pd.to_numeric(out[low_col], errors="coerce")
    close = pd.to_numeric(out[close_col], errors="coerce")

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    plus_dm = pd.Series(plus_dm, index=out.index)
    minus_dm = pd.Series(minus_dm, index=out.index)

    tr = _true_range(high, low, close)

    tr_rma = _wilder_rma(tr, length)
    plus_rma = _wilder_rma(plus_dm, length)
    minus_rma = _wilder_rma(minus_dm, length)

    # Avoid division by zero
    tr_rma_safe = tr_rma.replace(0.0, np.nan)

    pdi = 100.0 * (plus_rma / tr_rma_safe)
    mdi = 100.0 * (minus_rma / tr_rma_safe)

    denom = (pdi + mdi).replace(0.0, np.nan)
    dx = 100.0 * (pdi - mdi).abs() / denom

    adx = _wilder_rma(dx, adx_smoothing)

    out[f"{prefix}pdi"] = pdi
    out[f"{prefix}mdi"] = mdi
    out[f"{prefix}adx"] = adx

    return out
