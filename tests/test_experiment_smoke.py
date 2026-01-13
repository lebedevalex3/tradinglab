# tests/test_experiment_smoke.py
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

try:
    import yaml  # PyYAML
except Exception:  # pragma: no cover
    yaml = None

from tradinglab.experiments.runner import run_experiment


def _find_repo_root(start: Path) -> Path:
    """
    Find repository root by locating pyproject.toml.
    """
    cur = start.resolve()
    for parent in [cur] + list(cur.parents):
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("Cannot find repo root (pyproject.toml not found).")


@pytest.mark.skipif(yaml is None, reason="PyYAML is required for this test")
def test_experiment_runner_smoke(tmp_path: Path) -> None:
    repo_root = _find_repo_root(Path(__file__).resolve())
    sample_path = repo_root / "data" / "sample" / "btcusdt_10m.parquet"

    if not sample_path.exists():
        pytest.skip(f"Sample data not found: {sample_path}")

    # Write a minimal config that forces artifacts into tmp_path
    runs_dir = tmp_path / "runs"
    cfg = {
        "experiment": {"name": "exp000_smoke"},
        "data": {"sample_path": str(sample_path)},
        "artifacts": {"base_dir": str(runs_dir)},
    }

    config_path = tmp_path / "run.yml"
    config_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

    run_dir = run_experiment(config_path, exp_override="exp000_smoke")

    # Ensure output is inside tmp
    assert str(run_dir).startswith(str(runs_dir))

    # Artifact contract
    expected = {"config.resolved.yml", "metrics.json", "report.md"}
    existing = {p.name for p in run_dir.iterdir()}
    missing = expected - existing
    assert not missing, f"Missing expected artifacts: {missing}"

    # Results file can be parquet or csv
    has_parquet = (run_dir / "results.parquet").exists()
    has_csv = (run_dir / "results.csv").exists()
    assert has_parquet or has_csv, "Expected results.parquet or results.csv"

    # Validate metrics.json
    metrics = json.loads((run_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics.get("experiment") == "exp000_smoke"
    assert int(metrics.get("n_rows", 0)) > 0
    assert "run_id" in metrics

    # Validate results table content
    if has_parquet:
        df = pd.read_parquet(run_dir / "results.parquet")
    else:
        df = pd.read_csv(run_dir / "results.csv")

    assert len(df) >= 1
    assert "run_id" in df.columns
    assert "n_rows" in df.columns
