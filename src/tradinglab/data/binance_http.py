# src/tradinglab/data/binance_http.py
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
import requests


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


def normalize_binance_symbol(symbol: str) -> str:
    # Accept "BTC/USDT" and "BTCUSDT"
    return symbol.replace("/", "").upper()


@dataclass(frozen=True, slots=True)
class BinanceHTTPConfig:
    base_urls: tuple[str, ...] = (
        "https://api.binance.com",
        "https://api1.binance.com",
        "https://api2.binance.com",
        "https://api3.binance.com",
    )
    timeout_s: int = 30
    limit: int = 1000
    rate_limit_ms: int = 200
    retries: int = 5
    retry_backoff_ms: int = 800


class BinanceKlinesFetcher:
    """
    Public OHLCV fetcher via Binance /api/v3/klines.
    Does NOT require exchangeInfo/markets.
    """

    def __init__(self, cfg: BinanceHTTPConfig, logger: Any) -> None:
        self.cfg = cfg
        self.logger = logger
        self.session = requests.Session()

    def fetch_range(
        self,
        *,
        symbol: str,  # "BTC/USDT" or "BTCUSDT"
        interval: str,  # "15m"
        start_ms: int,
        end_ms: int | None = None,
    ) -> pd.DataFrame:
        sym = normalize_binance_symbol(symbol)
        tf_ms = _tf_to_ms(interval)
        since = start_ms
        rows: list[list[Any]] = []
        chunk = 0

        self.logger.info("binance_http fetch_range start: symbol=%s interval=%s since=%s end=%s limit=%s", sym, interval, start_ms, end_ms, self.cfg.limit)

        while True:
            if end_ms is not None and since >= end_ms:
                self.logger.info("binance_http stop: reached end_ms (%s >= %s)", since, end_ms)
                break

            chunk += 1
            t0 = time.time()
            batch = self._fetch_klines_with_retries(sym, interval, since, end_ms)
            dt = time.time() - t0

            if not batch:
                self.logger.info("binance_http stop: empty batch returned (chunk=%s)", chunk)
                break

            rows.extend(batch)

            last_open_time = int(batch[-1][0])
            next_since = last_open_time + tf_ms

            self.logger.info("chunk=%s since=%s got=%s last_open=%s dt=%.2fs total_rows=%s", chunk, since, len(batch), last_open_time, dt, len(rows))

            if next_since <= since:
                self.logger.warning("binance_http stop: non-increasing next_since=%s since=%s", next_since, since)
                break

            since = next_since
            time.sleep(self.cfg.rate_limit_ms / 1000.0)

        df = self._to_df(rows)
        self.logger.info("binance_http fetch_range done: rows=%s", len(df))
        return df

    def _fetch_klines_with_retries(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int | None,
    ):
        last_err: Exception | None = None

        for attempt in range(1, self.cfg.retries + 1):
            for base in self.cfg.base_urls:
                url = f"{base}/api/v3/klines"
                params = {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": start_ms,
                    "limit": self.cfg.limit,
                }
                if end_ms is not None:
                    params["endTime"] = end_ms

                try:
                    r = self.session.get(url, params=params, timeout=self.cfg.timeout_s)
                    if r.status_code >= 400:
                        raise RuntimeError(f"HTTP {r.status_code} from {base}: {r.text[:200]}")
                    return r.json()
                except Exception as e:
                    last_err = e
                    # try next base_url

            wait_ms = self.cfg.retry_backoff_ms * attempt
            self.logger.warning("klines failed (attempt %s/%s) start=%s err=%s; retry in %sms", attempt, self.cfg.retries, start_ms, repr(last_err), wait_ms)
            if attempt == self.cfg.retries:
                break
            time.sleep(wait_ms / 1000.0)

        raise last_err  # type: ignore[misc]

    @staticmethod
    def _to_df(rows: list[list[Any]]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(
            rows,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "qav",
                "num_trades",
                "taker_base",
                "taker_quote",
                "ignore",
            ],
        )
        df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()

        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
        df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
        return df
