#!/usr/bin/env python
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from tradinglab.reports.artifacts import (
    make_run_dir,
    write_metrics,
    write_report_md,
    write_resolved_config,
    write_results,
)


def main() -> None:
    print("== Writer smoke check ==")

    # 1. Create run dir
    run_id, run_dir = make_run_dir(
        base_dir="artifacts/runs",
        exp_name="writer_smoke_check",
        now=datetime.now(UTC),
    )
    print(f"[OK] run_id = {run_id}")
    print(f"[OK] run_dir = {run_dir}")

    # 2. Fake resolved config
    resolved_config = {
        "experiment": {"name": "writer_smoke_check"},
        "data": {"source": "sample"},
        "meta": {"created_at": datetime.now(UTC).isoformat()},
    }
    write_resolved_config(run_dir, resolved_config)
    print("[OK] config.resolved.yml written")

    # 3. Fake metrics
    metrics = {
        "status": "ok",
        "n_rows": 3,
        "example_float": 1.23,
        "example_bool": True,
    }
    write_metrics(run_dir, metrics)
    print("[OK] metrics.json written")

    # 4. Fake results dataframe
    df = pd.DataFrame(
        [
            {"col_a": 1, "col_b": 10.0},
            {"col_a": 2, "col_b": 20.0},
            {"col_a": 3, "col_b": 30.0},
        ]
    )
    results_path = write_results(run_dir, df)
    print(f"[OK] results written -> {results_path.name}")

    # 5. Fake markdown report
    report_md = (
        "# Writer Smoke Check\n\n"
        "This is a minimal report to verify artifact writing.\n\n"
        f"- run_id: `{run_id}`\n"
        f"- rows: {len(df)}\n"
    )
    write_report_md(run_dir, report_md)
    print("[OK] report.md written")

    # 6. Final verification
    expected_files = {
        "config.resolved.yml",
        "metrics.json",
        "report.md",
    }
    existing_files = {p.name for p in Path(run_dir).iterdir()}

    print("\n== Files in run_dir ==")
    for name in sorted(existing_files):
        print(" -", name)

    missing = expected_files - existing_files
    if missing:
        raise RuntimeError(f"Missing expected files: {missing}")

    print("\n[SUCCESS] Writer smoke check completed successfully.")


if __name__ == "__main__":
    main()
