lint:
	poetry run ruff check .
	poetry run ruff format --check .

format:
	poetry run ruff check . --fix
	poetry run ruff format .