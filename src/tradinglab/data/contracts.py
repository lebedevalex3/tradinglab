from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

REQUIRED_COLS = ("timestamp", "open", "high", "low", "close", "volume")


@dataclass(frozen=True, slots=True)
class OhlcvContract:
    required_cols: tuple[str, ...] = REQUIRED_COLS
    strict_ohlc_sanity: bool = True
    require_sorted: bool = True
    require_unique_ts: bool = True


class OhlcvValidationError(ValueError):
    pass


def _ensure_utc_timestamp(ts: pd.Series) -> pd.Series:
    # Numeric -> interpret as epoch milliseconds (your pipeline uses ms)
    if pd.api.types.is_numeric_dtype(ts):
        return pd.to_datetime(pd.to_numeric(ts, errors="coerce"), unit="ms", utc=True)

    # Datetime-like / strings -> parse to UTC (naive treated as UTC)
    return pd.to_datetime(ts, errors="coerce", utc=True)


def validate_ohlcv(
    df: pd.DataFrame,
    *,
    contract: OhlcvContract | None = None,
    strict: bool = True,
) -> pd.DataFrame:
    c = contract or OhlcvContract()

    if df is None or len(df) == 0:
        if strict:
            raise OhlcvValidationError("OHLCV is empty.")
        return pd.DataFrame(columns=list(c.required_cols))

    missing = [col for col in c.required_cols if col not in df.columns]
    if missing:
        if strict:
            raise OhlcvValidationError(f"Missing required columns: {missing}")
        return df.copy()

    out = df.copy()

    out["timestamp"] = _ensure_utc_timestamp(out["timestamp"])

    for col in ("open", "high", "low", "close", "volume"):
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["timestamp", "open", "high", "low", "close"])
    out = out.sort_values("timestamp").reset_index(drop=True)

    if c.require_unique_ts:
        out = out.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)

    if c.strict_ohlc_sanity:
        low = out["low"]
        high = out["high"]
        oc_min = out[["open", "close"]].min(axis=1)
        oc_max = out[["open", "close"]].max(axis=1)

        bad = (low > oc_min) | (high < oc_max) | (low > high)
        if strict and bool(bad.any()):
            raise OhlcvValidationError(f"OHLC sanity violated for {int(bad.sum())} rows.")

        neg_vol = out["volume"].notna() & (out["volume"] < 0)
        if strict and bool(neg_vol.any()):
            raise OhlcvValidationError(f"Negative volume for {int(neg_vol.sum())} rows.")

    if c.require_sorted and not out["timestamp"].is_monotonic_increasing:
        if strict:
            raise OhlcvValidationError("timestamp is not sorted ascending.")
    return out
