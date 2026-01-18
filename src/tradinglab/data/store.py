from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from tradinglab.data.contracts import OhlcvContract, validate_ohlcv


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

    def read_ohlcv(self, path: Path, *, contract: OhlcvContract | None = None) -> pd.DataFrame | None:
        df = self.read(path)
        if df is None or df.empty:
            return df
        return validate_ohlcv(df, contract=contract or OhlcvContract(), strict=False)

    def last_timestamp(self, df: pd.DataFrame | None) -> pd.Timestamp | None:
        if df is None or df.empty:
            return None
        ts = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        return ts.max()

    def write_atomic(self, path: Path, df: pd.DataFrame) -> None:
        # Enforce strict contract at write time
        clean = validate_ohlcv(df, contract=OhlcvContract(), strict=True)
        tmp = path.with_suffix(".tmp.parquet")
        clean.to_parquet(tmp, index=False)
        tmp.replace(path)
