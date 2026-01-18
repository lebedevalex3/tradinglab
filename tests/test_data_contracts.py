from __future__ import annotations

import pandas as pd
import pytest

from tradinglab.data.contracts import OhlcvContract, OhlcvValidationError, validate_ohlcv


def test_validate_ohlcv_normalizes_sorts_and_dedups_keep_last():
    # duplicate timestamp at first bar, second row should win (keep="last")
    df = pd.DataFrame(
        {
            "timestamp": [1_700_000_000_000, 1_700_000_000_000, 1_700_000_900_000],
            "open": [10, 20, 30],
            "high": [21, 22, 31],
            "low": [9, 19, 29],
            "close": [20, 21, 30.5],
            "volume": [100, 101, 102],
        }
    )

    out = validate_ohlcv(df, contract=OhlcvContract(), strict=True)

    assert "timestamp" in out.columns
    assert str(out["timestamp"].dtype).startswith("datetime64[ns, UTC]")

    assert out["timestamp"].is_monotonic_increasing
    assert out["timestamp"].nunique() == len(out)

    # After dedup keep last, open at first timestamp should be 20 (second row)
    assert float(out.iloc[0]["open"]) == 20.0
    assert len(out) == 2


def test_validate_ohlcv_raises_on_missing_required_cols():
    df = pd.DataFrame({"timestamp": [1_700_000_000_000], "open": [1.0]})
    with pytest.raises(OhlcvValidationError):
        validate_ohlcv(df, contract=OhlcvContract(), strict=True)


def test_validate_ohlcv_raises_on_ohlc_sanity_violation():
    # high < max(open, close) => invalid
    df = pd.DataFrame(
        {
            "timestamp": [1_700_000_000_000],
            "open": [10.0],
            "high": [9.0],  # invalid
            "low": [8.0],
            "close": [9.5],
            "volume": [1.0],
        }
    )

    with pytest.raises(OhlcvValidationError):
        validate_ohlcv(df, contract=OhlcvContract(strict_ohlc_sanity=True), strict=True)


def test_validate_ohlcv_raises_on_negative_volume():
    df = pd.DataFrame(
        {
            "timestamp": [1_700_000_000_000],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [-1.0],  # invalid
        }
    )

    with pytest.raises(OhlcvValidationError):
        validate_ohlcv(df, contract=OhlcvContract(), strict=True)


def test_validate_ohlcv_non_strict_allows_empty_and_returns_schema():
    out = validate_ohlcv(pd.DataFrame(), contract=OhlcvContract(), strict=False)
    assert list(out.columns) == list(OhlcvContract().required_cols)
