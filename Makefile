.PHONY: setup lint format test backtest exp-smoke exp exp-help

# -------------------------
# Setup & Quality
# -------------------------

setup:
	poetry install
	poetry run pre-commit install

lint:
	poetry run ruff check .
	poetry run ruff format --check .

format:
	poetry run ruff check . --fix
	poetry run ruff format .

test:
	poetry run pytest -q

notebook:
	poetry run jupyter lab --notebook-dir research/notebooks

# -------------------------
# Backtests (existing)
# -------------------------

backtest:
	poetry run python scripts/run_backtest.py --config configs/run.yml

# -------------------------
# Experiments (Milestone 1)
# -------------------------

CONFIG ?= configs/run_exp.yml
EXP ?= exp001_adx_bins

# Default smoke experiment
exp-smoke:
	poetry run python scripts/run_experiment.py --config $(CONFIG) --exp exp000_smoke

# Run any experiment: make exp EXP=exp000_smoke
exp:
	poetry run python scripts/run_experiment.py --config $(CONFIG) --exp $(EXP)

fetch:
	python scripts/fetch_ohlcv.py --config configs/run.yml


# Help
exp-help:
	@echo "Experiment targets:"
	@echo "  make exp-smoke"
	@echo "  make exp EXP=exp000_smoke"
	@echo "  make exp CONFIG=configs/run.yml EXP=exp000_smoke"
