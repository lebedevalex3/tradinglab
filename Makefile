.PHONY: setup lint format test backtest

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

backtest:
	poetry run python scripts/run_backtest.py --config configs/run.yml