from __future__ import annotations


def tf_to_ms(tf: str) -> int:
    n = int(tf[:-1])
    unit = tf[-1]
    if unit == "m":
        return n * 60_000
    if unit == "h":
        return n * 3_600_000
    if unit == "d":
        return n * 86_400_000
    raise ValueError(f"Unsupported timeframe: {tf!r}")
