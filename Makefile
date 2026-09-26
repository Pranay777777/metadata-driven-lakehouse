.DEFAULT_GOAL := help
.PHONY: help install up down test lint format typecheck security seed eval clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## Install the package with dev extras and git hooks
	pip install -e ".[dev]"
	pre-commit install

up:  ## Start the local stack
	docker compose up --build -d

down:  ## Stop the local stack
	docker compose down -v

test:  ## Run the test suite with coverage
	pytest

lint:  ## Lint and check formatting
	ruff check .
	ruff format --check .

format:  ## Auto-fix lint and format
	ruff check --fix .
	ruff format .

typecheck:  ## Run mypy in strict mode
	mypy

security:  ## Secret scan over full history plus dependency audit
	gitleaks detect --config .gitleaks.toml --redact --no-banner
	pip-audit --strict

seed:  ## Load sample data (implement per project)
	python -m lakehouse.seed

eval:  ## Run the evaluation harness (AI projects only)
	python -m lakehouse.eval

clean:  ## Remove caches and build artefacts
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage coverage.xml dist build
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
