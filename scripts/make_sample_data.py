from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def main() -> None:
    out = Path("data/sample/btcusdt_10m.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)

    n = 2000  # 2000 свечей по 10m ≈ 13.9 дней
    start = pd.Timestamp("2024-01-01T00:00:00Z")
    idx = pd.date_range(start=start, periods=n, freq="10min")

    rng = np.random.default_rng(42)

    # простая синтетика: лог-норм случайный ход
    rets = rng.normal(loc=0.0, scale=0.002, size=n)  # ~0.2% std на бар
    price = 42000 * np.exp(np.cumsum(rets))

    close = price
    open_ = np.roll(close, 1)
    open_[0] = close[0]

    # high/low вокруг open/close + небольшой шум
    spread = np.abs(rng.normal(0, 0.0015, size=n)) * close
    high = np.maximum(open_, close) + spread
    low = np.minimum(open_, close) - spread

    volume = rng.lognormal(mean=8.0, sigma=0.4, size=n)

    df = pd.DataFrame(
        {
            "timestamp": idx,
            "open": open_.astype("float64"),
            "high": high.astype("float64"),
            "low": low.astype("float64"),
            "close": close.astype("float64"),
            "volume": volume.astype("float64"),
        }
    )

    # гарантируем тип timestamp и сортировку
    df = df.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)

    df.to_parquet(out, index=False)
    print(f"OK: wrote {len(df)} rows -> {out}")


if __name__ == "__main__":
    main()
