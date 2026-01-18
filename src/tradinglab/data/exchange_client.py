# src/tradinglab/data/exchange_client.py
from __future__ import annotations

import time
from typing import Any

import ccxt


def make_exchange(
    exchange_id: str,
    market_type: str,
    *,
    timeout_ms: int = 60000,
    load_markets: bool = True,
    load_markets_retries: int = 5,
    retry_backoff_ms: int = 800,
    logger: Any | None = None,
) -> Any:
    """
    Create and configure CCXT exchange client.

    Key goals for TradingLab:
    - reproducible defaults (timeout, rate limit)
    - ability to fully skip load_markets() (to avoid exchangeInfo dependency)
    - minimal side effects; do NOT override ex.urls for Binance
    """
    ex_cls = getattr(ccxt, exchange_id)

    # Map your "perp" to CCXT "swap" where applicable
    default_type = "swap" if market_type == "perp" else market_type

    ex = ex_cls(
        {
            "enableRateLimit": True,
            "timeout": timeout_ms,
            # Reduce CCXT "smart" behaviour that can trigger extra calls:
            # (supported options vary per exchange; safe to set when dict exists)
            "options": {
                "defaultType": default_type,
                # In CCXT, some exchanges may attempt currency/market lookups.
                # Keeping it explicit and lean helps.
                "adjustForTimeDifference": True,
            },
        }
    )

    # Some exchanges don't accept options in constructor as expected; ensure it exists
    if hasattr(ex, "options") and isinstance(ex.options, dict):
        ex.options.setdefault("defaultType", default_type)
        ex.options.setdefault("adjustForTimeDifference", True)

    if logger:
        logger.info(
            "CCXT exchange created: id=%s market_type=%s defaultType=%s timeout_ms=%s load_markets=%s",
            exchange_id,
            market_type,
            default_type,
            timeout_ms,
            load_markets,
        )

    if load_markets:
        _load_markets_with_retries(
            ex,
            retries=load_markets_retries,
            backoff_ms=retry_backoff_ms,
            logger=logger,
        )
    else:
        # Important: explicitly mark markets as "not loaded" but do not trigger loading here
        # Some CCXT methods may still trigger loading; we handle that at caller level.
        if logger:
            logger.info("Skipping load_markets() as requested (load_markets=false).")

    return ex


def _load_markets_with_retries(
    ex: Any,
    *,
    retries: int,
    backoff_ms: int,
    logger: Any | None = None,
) -> None:
    last_err: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            if logger:
                logger.info("load_markets attempt %s/%s ...", attempt, retries)

            ex.load_markets()

            if logger:
                try:
                    n = len(getattr(ex, "markets", {}) or {})
                except Exception:
                    n = -1
                logger.info("load_markets OK (markets=%s).", n)

            return

        except Exception as e:
            last_err = e
            wait_ms = backoff_ms * attempt

            if logger:
                logger.warning(
                    "load_markets failed (attempt %s/%s): %s; retry in %sms",
                    attempt,
                    retries,
                    repr(e),
                    wait_ms,
                )

            if attempt == retries:
                raise

            time.sleep(wait_ms / 1000.0)

    if last_err:
        raise last_err
