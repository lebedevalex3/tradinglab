from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True, slots=True)
class RawPathSpec:
    exchange: str
    market_type: str  # "spot" | "perp"
    symbol: str  # "BTC/USDT"
    timeframe: str  # "10m"

    def normalized_symbol(self) -> str:
        return self.symbol.replace("/", "")


class ParquetStore:
    def __init__(self, raw_dir: str | Path) -> None:
        self.raw_dir = Path(raw_dir)

    def path_for(self, spec: RawPathSpec) -> Path:
        p = self.raw_dir / spec.exchange / spec.market_type / spec.normalized_symbol()
        p.mkdir(parents=True, exist_ok=True)
        return p / f"{spec.timeframe}.parquet"

    def read(self, path: Path) -> pd.DataFrame | None:
        if not path.exists():
            return None
        return pd.read_parquet(path)

    def last_timestamp(self, df: pd.DataFrame | None) -> pd.Timestamp | None:
        if df is None or df.empty:
            return None
        ts = df["timestamp"]
        # expecting tz-aware UTC
        return pd.to_datetime(ts).max()

    def write_atomic(self, path: Path, df: pd.DataFrame) -> None:
        tmp = path.with_suffix(".tmp.parquet")
        df.to_parquet(tmp, index=False)
        tmp.replace(path)
