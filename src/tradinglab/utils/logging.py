# src/tradinglab/utils/logging.py
from __future__ import annotations

import logging


def setup_logger(name: str = "tradinglab", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        h = logging.StreamHandler()
        fmt = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        h.setFormatter(fmt)
        logger.addHandler(h)

    logger.propagate = False
    return logger
