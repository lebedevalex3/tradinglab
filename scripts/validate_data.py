#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from tradinglab.data.contracts import OhlcvContract, OhlcvValidationError, validate_ohlcv
from tradinglab.data.timeframes import tf_to_ms


@dataclass(frozen=True, slots=True)
class FileReport:
    path: str
    ok: bool
    rows: int
    min_ts: str | None
    max_ts: str | None
    expected_rows: int | None
    missing_rows: int | None
    missing_ratio: float | None
    max_gap_bars: int | None
    error: str | None


def _iso(ts: pd.Timestamp | None) -> str | None:
    if ts is None or pd.isna(ts):
        return None
    # ensure UTC ISO
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()


def _expected_rows(min_ts: pd.Timestamp, max_ts: pd.Timestamp, tf: str) -> int:
    step = tf_to_ms(tf)
    min_ms = int(min_ts.value // 1_000_000)
    max_ms = int(max_ts.value // 1_000_000)
    if max_ms < min_ms:
        return 0
    # inclusive range count on a grid
    return int((max_ms - min_ms) // step) + 1


def _max_gap_bars(ts: pd.Series, tf: str) -> int | None:
    if ts.empty:
        return None
    step_ms = tf_to_ms(tf)
    ms = (ts.view("int64") // 1_000_000).astype("int64")  # ns -> ms
    diffs = pd.Series(ms).diff().dropna()
    if diffs.empty:
        return 0
    # gap bars: (diff/step) - 1, clamp >=0
    gap_bars = ((diffs // step_ms) - 1).clip(lower=0)
    return int(gap_bars.max())


def _infer_tf_from_path(path: Path) -> str | None:
    # expecting "<tf>.parquet"
    name = path.name
    if not name.endswith(".parquet"):
        return None
    return name[: -len(".parquet")] or None


def iter_parquet_files(raw_dir: Path) -> Iterable[Path]:
    yield from raw_dir.rglob("*.parquet")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate OHLCV parquet files under data/raw")
    parser.add_argument("--raw-dir", type=str, default="data/raw")
    parser.add_argument("--strict", action="store_true", help="Fail on contract violations")
    parser.add_argument("--no-strict", dest="strict", action="store_false")
    parser.set_defaults(strict=True)

    parser.add_argument("--write-report", action="store_true", help="Write JSON report into artifacts/")
    parser.add_argument("--report-path", type=str, default="", help="Optional explicit report path")

    # Optional filters (substring match) to run faster on subsets
    parser.add_argument("--exchange", type=str, default="")
    parser.add_argument("--market", type=str, default="")
    parser.add_argument("--symbol", type=str, default="")
    parser.add_argument("--tf", type=str, default="")

    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    raw_dir = (root / args.raw_dir).resolve()

    contract = OhlcvContract()
    reports: list[FileReport] = []

    total = 0
    bad = 0

    for path in iter_parquet_files(raw_dir):
        rel = path.relative_to(root)
        srel = str(rel)

        # Simple path filters
        if args.exchange and f"/{args.exchange}/" not in f"/{srel}/":
            continue
        if args.market and f"/{args.market}/" not in f"/{srel}/":
            continue
        if args.symbol and f"/{args.symbol}/" not in f"/{srel}/":
            continue
        if args.tf and not path.name.startswith(args.tf + "."):
            continue

        total += 1
        tf = _infer_tf_from_path(path)

        try:
            df = pd.read_parquet(path)
            df = validate_ohlcv(df, contract=contract, strict=args.strict)

            rows = len(df)
            min_ts = df["timestamp"].min() if rows else None
            max_ts = df["timestamp"].max() if rows else None

            exp_rows = None
            miss_rows = None
            miss_ratio = None
            max_gap = None

            if rows and tf:
                exp_rows = _expected_rows(min_ts, max_ts, tf)  # type: ignore[arg-type]
                miss_rows = max(0, exp_rows - rows)
                miss_ratio = (miss_rows / exp_rows) if exp_rows else 0.0
                max_gap = _max_gap_bars(df["timestamp"], tf)

            reports.append(
                FileReport(
                    path=srel,
                    ok=True,
                    rows=rows,
                    min_ts=_iso(min_ts),
                    max_ts=_iso(max_ts),
                    expected_rows=exp_rows,
                    missing_rows=miss_rows,
                    missing_ratio=miss_ratio,
                    max_gap_bars=max_gap,
                    error=None,
                )
            )

        except (OhlcvValidationError, Exception) as e:
            bad += 1
            reports.append(
                FileReport(
                    path=srel,
                    ok=False,
                    rows=0,
                    min_ts=None,
                    max_ts=None,
                    expected_rows=None,
                    missing_rows=None,
                    missing_ratio=None,
                    max_gap_bars=None,
                    error=repr(e),
                )
            )
            print(f"[BAD] {srel}: {e}")

    print(f"Checked: {total}, bad: {bad}")

    if args.write_report:
        now = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        default_path = root / "artifacts" / "runs" / f"data_check_{now}.json"
        out_path = Path(args.report_path).resolve() if args.report_path else default_path
        out_path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "generated_at": datetime.now(UTC).isoformat(),
            "raw_dir": str(raw_dir),
            "strict": args.strict,
            "checked": total,
            "bad": bad,
            "files": [asdict(r) for r in reports],
        }
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"[OK] report saved: {out_path}")

    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
