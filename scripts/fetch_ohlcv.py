# scripts/fetch_ohlcv.py
#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import yaml

# NEW: direct binance http provider
from tradinglab.data.binance_http import BinanceHTTPConfig, BinanceKlinesFetcher
from tradinglab.data.exchange_client import make_exchange
from tradinglab.data.ohlcv_fetcher import FetchConfig, OHLCVFetcher, _to_utc_timestamp_ms
from tradinglab.data.store import ParquetStore, RawPathSpec
from tradinglab.utils.logging import setup_logger


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch OHLCV into data/raw")
    parser.add_argument("--config", type=str, default="configs/run.yml")
    args = parser.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8")) or {}
    ds = cfg.get("data_source", {})
    st = cfg.get("storage", {})
    log_cfg = cfg.get("logging", {})

    logger = setup_logger("tradinglab.fetch", level=str(log_cfg.get("level", "INFO")))

    provider = str(ds.get("provider", "ccxt")).lower()

    exchange_id = ds.get("exchange", "binance")
    market_type = ds.get("market_type", "spot")

    symbols = ds.get("symbols", [])
    timeframes = ds.get("timeframes", [])
    start = ds["start"]
    end = ds.get("end")

    raw_dir = st.get("raw_dir", "data/raw")

    logger.info("Config loaded: %s", args.config)
    logger.info("Provider=%s", provider)
    logger.info("Exchange=%s market=%s symbols=%s tfs=%s", exchange_id, market_type, symbols, timeframes)
    logger.info("Raw dir: %s", raw_dir)

    start_ms = _to_utc_timestamp_ms(start)
    end_ms = _to_utc_timestamp_ms(end) if end else None

    store = ParquetStore(raw_dir)

    # ---------------------------
    # Provider: BINANCE HTTP
    # ---------------------------
    if provider == "binance_http":
        http_cfg = BinanceHTTPConfig(
            base_urls=tuple(
                ds.get(
                    "base_urls",
                    (
                        "https://api.binance.com",
                        "https://api1.binance.com",
                        "https://api2.binance.com",
                        "https://api3.binance.com",
                    ),
                )
            ),
            timeout_s=int(ds.get("timeout_s", 30)),
            limit=int(ds.get("limit", 1000)),
            rate_limit_ms=int(ds.get("rate_limit_ms", 200)),
            retries=int(ds.get("retries", 5)),
            retry_backoff_ms=int(ds.get("retry_backoff_ms", 800)),
        )
        fetcher = BinanceKlinesFetcher(http_cfg, logger=logger)

        for symbol in symbols:
            for tf in timeframes:
                spec = RawPathSpec(exchange=exchange_id, market_type=market_type, symbol=symbol, timeframe=tf)
                path = store.path_for(spec)

                existing = store.read(path)
                last_ts = store.last_timestamp(existing)

                since_ms = start_ms
                if last_ts is not None:
                    since_ms = int(last_ts.timestamp() * 1000)

                logger.info("=== %s %s ===", symbol, tf)
                logger.info("Existing: %s rows=%s last_ts=%s", path, 0 if existing is None else len(existing), last_ts)
                logger.info("Fetch since_ms=%s end_ms=%s limit=%s", since_ms, end_ms, http_cfg.limit)

                df_new = fetcher.fetch_range(symbol=symbol, interval=tf, start_ms=since_ms, end_ms=end_ms)

                if existing is None or existing.empty:
                    merged = df_new
                else:
                    merged = pd.concat([existing, df_new], ignore_index=True)
                    merged = merged.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)

                store.write_atomic(path, merged)
                logger.info("[OK] saved: %s rows=%s (new_rows=%s)", path, len(merged), len(df_new))

        return

    # ---------------------------
    # Provider: CCXT (default)
    # ---------------------------
    timeout_ms = int(ds.get("timeout_ms", 60000))
    load_markets = bool(ds.get("load_markets", False))  # for MVP keep False

    logger.info("CCXT: timeout_ms=%s load_markets=%s", timeout_ms, load_markets)

    ex = make_exchange(
        exchange_id,
        market_type,
        timeout_ms=timeout_ms,
        load_markets=load_markets,
        load_markets_retries=int(ds.get("load_markets_retries", 5)),
        retry_backoff_ms=int(ds.get("load_markets_backoff_ms", 800)),
        logger=logger,
    )

    fetcher = OHLCVFetcher(
        ex,
        FetchConfig(
            limit=int(ds.get("limit", 1000)),
            rate_limit_ms=int(ds.get("rate_limit_ms", 200)),
            retries=int(ds.get("retries", 5)),
            retry_backoff_ms=int(ds.get("retry_backoff_ms", 500)),
            show_progress=bool(ds.get("show_progress", True)),
            log_every=int(ds.get("log_every", 1)),
        ),
        logger=logger,
    )

    for symbol in symbols:
        for tf in timeframes:
            spec = RawPathSpec(exchange=exchange_id, market_type=market_type, symbol=symbol, timeframe=tf)
            path = store.path_for(spec)

            existing = store.read(path)
            last_ts = store.last_timestamp(existing)

            since_ms = start_ms
            if last_ts is not None:
                since_ms = int(last_ts.timestamp() * 1000)

            logger.info("=== %s %s ===", symbol, tf)
            logger.info("Existing: %s rows=%s last_ts=%s", path, 0 if existing is None else len(existing), last_ts)
            logger.info("Fetch since_ms=%s end_ms=%s", since_ms, end_ms)

            df_new = fetcher.fetch_range(symbol=symbol, timeframe=tf, start_ms=since_ms, end_ms=end_ms)

            if existing is None or existing.empty:
                merged = df_new
            else:
                merged = pd.concat([existing, df_new], ignore_index=True)
                merged = merged.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)

            store.write_atomic(path, merged)
            logger.info("[OK] saved: %s rows=%s (new_rows=%s)", path, len(merged), len(df_new))


if __name__ == "__main__":
    main()
