.DEFAULT_GOAL := help
.PHONY: help install up down logs stack test lint format typecheck security seed classify register run demo dagster bench optimize vacuum replay eval clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## Install the package with dev extras and git hooks
	pip install -e ".[dev]"
	pre-commit install

up:  ## Start the local stack and wait for it to answer
	docker compose up -d --wait
	@$(MAKE) --no-print-directory stack

down:  ## Stop the local stack and delete its volumes
	docker compose down -v

logs:  ## Follow logs (SERVICE=marquez make logs for one service)
	docker compose logs -f $${SERVICE:-}

stack:  ## Check every service is actually answering
	python -m lakehouse.stack

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

seed:  ## Generate synthetic source data (ROWS=5000000 make seed)
	python -m lakehouse.seed --rows $${ROWS:-1000000}

classify:  ## Propose PII classifications for every registered object
	python -m lakehouse.privacy

register:  ## Write the demo catalog into the control plane
	python -m lakehouse.pipeline --register

run:  ## Run bronze -> silver -> gold over the seeded data
	python -m lakehouse.pipeline

demo:  ## Seed, register and run the whole platform end to end
	@$(MAKE) --no-print-directory seed
	@$(MAKE) --no-print-directory register
	@$(MAKE) --no-print-directory run

dagster:  ## Open the Dagster UI at http://localhost:3001
	dagster dev -m lakehouse.orchestration -p 3001

bench:  ## Benchmark the arrow and spark engines (ROWS=2000000 make bench)
	python scripts/benchmark_engines.py --rows $${ROWS:-1000000}

optimize:  ## Compact every table (ZORDER=col to also Z-order)
	python -m lakehouse.maintenance --compact $${ZORDER:+--zorder-by $$ZORDER}

vacuum:  ## Dry-run vacuum; APPLY=1 to delete for real
	python -m lakehouse.maintenance --vacuum $${APPLY:+--apply}

replay:  ## Rewind a source: make replay SOURCE=orders FROM=2026-03-01 [APPLY=1]
	python -m lakehouse.replay rewind --source $(SOURCE) --from $(FROM) $${APPLY:+--apply}

eval:  ## Run the evaluation harness (AI projects only)
	python -m lakehouse.eval

clean:  ## Remove caches and build artefacts
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage coverage.xml dist build
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
