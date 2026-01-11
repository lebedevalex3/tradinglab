from __future__ import annotations

import json
import subprocess
from pathlib import Path


def test_smoke_backtest_creates_run(tmp_path: Path) -> None:
    # Запускаем smoke-run в отдельной temp-директории артефактов
    artifacts_dir = tmp_path / "runs"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    # Используем sample data из репо
    data_path = Path("data/sample/btcusdt_10m.parquet")
    assert data_path.exists(), "Sample data missing. Create data/sample/*.parquet first."

    cfg = tmp_path / "run.yml"
    cfg.write_text(
        "\n".join(
            [
                'strategy_version: "mr-envelope-v0.1.0"',
                "paths:",
                f'  data_path: "{data_path.as_posix()}"',
                f'  artifacts_dir: "{artifacts_dir.as_posix()}"',
                "params:",
                '  note: "pytest-smoke"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    # Запускаем скрипт как subprocess через poetry
    res = subprocess.run(
        ["poetry", "run", "python", "scripts/run_backtest.py", "--config", str(cfg)],
        capture_output=True,
        text=True,
        check=True,
    )

    # Должен появиться latest.txt
    latest = (artifacts_dir / "latest.txt").read_text(encoding="utf-8").strip()
    run_dir = artifacts_dir / latest
    assert run_dir.exists()

    meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["strategy_version"] == "mr-envelope-v0.1.0"

    assert (run_dir / "summary.json").exists()
    assert (run_dir / "report.md").exists()
    assert (run_dir / "trades.parquet").exists()

    assert "OK: run created at" in res.stdout
