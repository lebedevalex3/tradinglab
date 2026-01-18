from __future__ import annotations

import pandas as pd

from tradinglab.data.timeframes import tf_to_ms


def compute_since_ms(start_ms: int, last_ts: pd.Timestamp, tf: str, overlap_bars: int) -> int:
    last_ms = int(last_ts.value // 1_000_000)  # ns -> ms
    overlap_ms = overlap_bars * tf_to_ms(tf)
    return max(start_ms, max(0, last_ms - overlap_ms))


def test_incremental_since_ms_with_overlap_respects_start():
    start_ms = 1_700_000_000_000

    # last_ts corresponds to start_ms + 1000 * 15m
    last_ms = start_ms + 1000 * tf_to_ms("15m")
    last_ts = pd.to_datetime(last_ms, unit="ms", utc=True)

    since = compute_since_ms(start_ms, last_ts, "15m", overlap_bars=200)

    # overlap=200 bars => subtract 200*15m
    expected = last_ms - 200 * tf_to_ms("15m")
    assert since == expected
    assert since >= start_ms


def test_incremental_since_ms_clamps_to_start_when_overlap_goes_before_start():
    start_ms = 1_700_000_000_000

    # last is only 50 bars after start, but overlap is 200 bars => would go before start
    last_ms = start_ms + 50 * tf_to_ms("15m")
    last_ts = pd.to_datetime(last_ms, unit="ms", utc=True)

    since = compute_since_ms(start_ms, last_ts, "15m", overlap_bars=200)

    assert since == start_ms


def test_incremental_since_ms_never_negative():
    start_ms = 0
    last_ms = 10_000  # 10 seconds in ms
    last_ts = pd.to_datetime(last_ms, unit="ms", utc=True)

    # absurd overlap to force negative
    since = compute_since_ms(start_ms, last_ts, "1m", overlap_bars=10_000)

    assert since == 0
