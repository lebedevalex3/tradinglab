# src/tradinglab/data/ohlcv_fetcher.py
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    tqdm = None


def _to_utc_timestamp_ms(dt_str: str) -> int:
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def _tf_to_ms(tf: str) -> int:
    n = int(tf[:-1])
    unit = tf[-1]
    if unit == "m":
        return n * 60_000
    if unit == "h":
        return n * 3_600_000
    if unit == "d":
        return n * 86_400_000
    raise ValueError(f"Unsupported timeframe: {tf}")


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

        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
        df = df.sort_values("timestamp")
        df = df.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
        return df
