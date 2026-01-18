# src/tradinglab/data/ohlcv_fetcher.py
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from tradinglab.data.contracts import OhlcvContract, validate_ohlcv
from tradinglab.data.timeframes import tf_to_ms

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    tqdm = None


def _to_utc_timestamp_ms(dt_str: str) -> int:
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def _tf_to_ms(tf: str) -> int:
    # backward-compatible wrapper
    return tf_to_ms(tf)


@dataclass(frozen=True, slots=True)
class FetchConfig:
    limit: int = 1000
    rate_limit_ms: int = 200
    retries: int = 5
    retry_backoff_ms: int = 500
    log_every: int = 1  # log every N chunks (if no tqdm)
    show_progress: bool = True  # enable tqdm if available


class OHLCVFetcher:
    def __init__(self, exchange: Any, cfg: FetchConfig, logger: Any) -> None:
        self.exchange = exchange
        self.cfg = cfg
        self.logger = logger

    def fetch_range(
        self,
        *,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int | None = None,
    ) -> pd.DataFrame:
        tf_ms = _tf_to_ms(timeframe)
        since = start_ms
        rows: list[list[Any]] = []
        chunk_i = 0

        # Estimate number of chunks for progress (rough; exchange may return fewer)
        total_chunks = None
        if end_ms is not None and self.cfg.limit > 0:
            est_bars = max(0, (end_ms - start_ms) // tf_ms)
            total_chunks = int(est_bars // self.cfg.limit + 1)

        pbar = None
        if self.cfg.show_progress and tqdm is not None:
            desc = f"{symbol} {timeframe}"
            pbar = tqdm(total=total_chunks, desc=desc, unit="chunk", dynamic_ncols=True)

        self.logger.info("fetch_range start: symbol=%s tf=%s since=%s end=%s limit=%s", symbol, timeframe, start_ms, end_ms, self.cfg.limit)

        while True:
            if end_ms is not None and since >= end_ms:
                self.logger.info("fetch_range stop: reached end_ms (%s >= %s)", since, end_ms)
                break

            chunk_i += 1
            t0 = time.time()
            batch = self._fetch_with_retries(symbol=symbol, timeframe=timeframe, since=since, limit=self.cfg.limit)
            dt = time.time() - t0

            if not batch:
                self.logger.info("fetch_range stop: empty batch returned (chunk=%s)", chunk_i)
                break

            rows.extend(batch)

            first_ts = batch[0][0]
            last_ts = batch[-1][0]
            batch_len = len(batch)

            # Progress update
            if pbar is not None:
                pbar.update(1)
                pbar.set_postfix(
                    {
                        "since": since,
                        "got": batch_len,
                        "last": last_ts,
                        "sec": f"{dt:.2f}",
                        "rows": len(rows),
                    }
                )
            else:
                if self.cfg.log_every > 0 and (chunk_i % self.cfg.log_every == 0):
                    self.logger.info("chunk=%s since=%s got=%s first_ts=%s last_ts=%s dt=%.2fs total_rows=%s", chunk_i, since, batch_len, first_ts, last_ts, dt, len(rows))

            next_since = int(last_ts + tf_ms)

            # Prevent infinite loop if exchange misbehaves
            if next_since <= since:
                self.logger.warning("fetch_range stop: next_since <= since (next=%s since=%s). Exchange returned non-increasing data.", next_since, since)
                break

            since = next_since
            time.sleep(self.cfg.rate_limit_ms / 1000.0)

        if pbar is not None:
            pbar.close()

        df = self._to_df(rows)
        self.logger.info("fetch_range done: rows=%s", len(df))
        return df

    def _fetch_with_retries(self, *, symbol: str, timeframe: str, since: int, limit: int):
        last_err: Exception | None = None
        for attempt in range(self.cfg.retries):
            try:
                return self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
            except Exception as e:
                last_err = e
                wait_ms = self.cfg.retry_backoff_ms * (attempt + 1)
                self.logger.warning("fetch_ohlcv failed (attempt %s/%s) since=%s tf=%s symbol=%s err=%s; retry in %sms", attempt + 1, self.cfg.retries, since, timeframe, symbol, repr(e), wait_ms)
                time.sleep(wait_ms / 1000.0)
        # If still failing:
        raise last_err  # type: ignore[misc]

    @staticmethod
    def _to_df(rows: list[list[Any]]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(rows, columns=["ts_ms", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        df = df.drop(columns=["ts_ms"])

        # Centralized normalization/contract
        return validate_ohlcv(df, contract=OhlcvContract(), strict=False)


@dataclass(frozen=True, slots=True)
class IncrementalConfig:
    overlap_bars: int = 200
    strict_validation: bool = True
    contract: OhlcvContract = OhlcvContract()


def _last_timestamp_ms(df: pd.DataFrame) -> int:
    # df["timestamp"] is expected tz-aware UTC datetime64
    ts = df["timestamp"].iloc[-1]
    return int(ts.value // 1_000_000)  # ns -> ms


def compute_incremental_since_ms(
    *,
    existing: pd.DataFrame | None,
    timeframe: str,
    overlap_bars: int,
    min_since_ms: int | None,
) -> int | None:
    if existing is None or len(existing) == 0:
        return min_since_ms

    tf_ms = tf_to_ms(timeframe)
    last_ms = _last_timestamp_ms(existing)
    overlap_ms = overlap_bars * tf_ms
    since = max(0, last_ms - overlap_ms)

    if min_since_ms is not None:
        since = max(min_since_ms, since)
    return since


def read_parquet_if_exists(path: Path, *, contract: OhlcvContract) -> pd.DataFrame | None:
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    return validate_ohlcv(df, contract=contract, strict=False)


def write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(path)


def merge_ohlcv(
    *,
    existing: pd.DataFrame | None,
    fresh: pd.DataFrame,
    contract: OhlcvContract,
    strict: bool,
) -> pd.DataFrame:
    if existing is None or len(existing) == 0:
        return validate_ohlcv(fresh, contract=contract, strict=strict)

    merged = pd.concat([existing, fresh], ignore_index=True)
    return validate_ohlcv(merged, contract=contract, strict=strict)


def incremental_update_parquet(
    *,
    fetcher: OHLCVFetcher,
    path: Path,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int | None = None,
    cfg: IncrementalConfig | None = None,
) -> pd.DataFrame:
    """
    Incremental updater:
      - loads existing parquet if present
      - computes since_ms = last_ts - overlap window (but not earlier than start_ms)
      - fetches range [since_ms, end_ms)
      - merges existing + fresh
      - validates strictly (optional)
      - writes atomically
    """
    cfg = cfg or IncrementalConfig()

    existing = read_parquet_if_exists(path, contract=cfg.contract)

    since_ms = compute_incremental_since_ms(
        existing=existing,
        timeframe=timeframe,
        overlap_bars=cfg.overlap_bars,
        min_since_ms=start_ms,
    )
    if since_ms is None:
        since_ms = start_ms

    fetcher.logger.info(
        "incremental_update: path=%s symbol=%s tf=%s since=%s end=%s overlap_bars=%s",
        str(path),
        symbol,
        timeframe,
        since_ms,
        end_ms,
        cfg.overlap_bars,
    )

    fresh = fetcher.fetch_range(
        symbol=symbol,
        timeframe=timeframe,
        start_ms=since_ms,
        end_ms=end_ms,
    )

    merged = merge_ohlcv(
        existing=existing,
        fresh=fresh,
        contract=cfg.contract,
        strict=cfg.strict_validation,
    )

    write_parquet_atomic(merged, path)
    return merged
