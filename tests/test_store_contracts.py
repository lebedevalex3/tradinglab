from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tradinglab.data.contracts import OhlcvValidationError
from tradinglab.data.store import ParquetStore, RawPathSpec


def test_store_write_atomic_enforces_contract_and_normalizes(tmp_path: Path):
    store = ParquetStore(tmp_path)

    spec = RawPathSpec(exchange="binance", market_type="spot", symbol="BTC/USDT", timeframe="15m")
    path = store.path_for(spec)

    # intentionally unsorted + duplicated timestamp
    df = pd.DataFrame(
        {
            "timestamp": [1_700_000_900_000, 1_700_000_000_000, 1_700_000_000_000],
            "open": [30, 10, 20],
            "high": [31, 21, 22],
            "low": [29, 9, 19],
            "close": [30.5, 20, 21],
            "volume": [102, 100, 101],
        }
    )

    store.write_atomic(path, df)

    saved = pd.read_parquet(path)

    # after write, should be sorted+deduped by contract
    assert saved["timestamp"].is_monotonic_increasing
    assert saved["timestamp"].nunique() == len(saved)
    assert len(saved) == 2

    # keep="last" for duplicated timestamp => open at first timestamp should be 20
    assert float(saved.iloc[0]["open"]) == 20.0


def test_store_write_atomic_raises_on_invalid_data(tmp_path: Path):
    store = ParquetStore(tmp_path)

    spec = RawPathSpec(exchange="binance", market_type="spot", symbol="BTC/USDT", timeframe="15m")
    path = store.path_for(spec)

    # high < open/close => invalid
    df_bad = pd.DataFrame(
        {
            "timestamp": [1_700_000_000_000],
            "open": [10.0],
            "high": [9.0],
            "low": [8.0],
            "close": [9.5],
            "volume": [1.0],
        }
    )

    with pytest.raises(OhlcvValidationError):
        store.write_atomic(path, df_bad)


def test_store_read_ohlcv_normalizes_timestamp_dtype(tmp_path: Path):
    store = ParquetStore(tmp_path)

    spec = RawPathSpec(exchange="binance", market_type="spot", symbol="BTC/USDT", timeframe="15m")
    path = store.path_for(spec)

    # write parquet manually with timestamp as int ms
    raw = pd.DataFrame(
        {
            "timestamp": [1_700_000_000_000, 1_700_000_900_000],
            "open": [10, 11],
            "high": [12, 13],
            "low": [9, 10],
            "close": [11, 12],
            "volume": [100, 101],
        }
    )
    raw.to_parquet(path, index=False)

    df = store.read_ohlcv(path)
    assert df is not None
    assert str(df["timestamp"].dtype).startswith("datetime64[ns, UTC]")
    assert df["timestamp"].is_monotonic_increasing
