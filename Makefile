# Install all dependencies
install:
	poetry install --all-extras --with dev

# Automatically format and lint local python code
fmt:
	poetry run ruff format .
	poetry run ruff check . --fix
	make lint

# Check for lint errors
lint:
	poetry run ruff format --check .
	poetry run ruff check .
	make mypy

# Run type checking
mypy:
	poetry run mypy . --config-file setup.cfg

# Run unit tests
test:
	poetry run pytest -vv .
