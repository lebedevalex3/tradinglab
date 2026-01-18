from __future__ import annotations

import pandas as pd

from tradinglab.data.timeframes import tf_to_ms


def test_expected_rows_formula_is_consistent():
    # 15m bars across 1 hour => 5 bars inclusive (00:00, 00:15, 00:30, 00:45, 01:00)
    step = tf_to_ms("15m")
    min_ms = 1_700_000_000_000
    max_ms = min_ms + 4 * step
    expected = ((max_ms - min_ms) // step) + 1
    assert expected == 5


def test_max_gap_bars_logic():
    # timestamps: 0, 15m, 60m => gap between 15m and 60m is 3 missing bars (30m,45m)
    tf = "15m"
    step = tf_to_ms(tf)
    base = 1_700_000_000_000
    ms = [base, base + step, base + 4 * step]
    ts = pd.to_datetime(ms, unit="ms", utc=True)

    diffs = pd.Series(ts.view("int64") // 1_000_000).diff().dropna()
    gap_bars = ((diffs // step) - 1).clip(lower=0)
    assert int(gap_bars.max()) == 2  # missing bars between 15m and 60m: 30m,45m => 2
