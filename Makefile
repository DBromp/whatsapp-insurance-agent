# Convenience targets matching the namastexlabs/automagik-* ecosystem conventions.
# Windows: run from Git Bash, or use the equivalent pip/pytest commands directly.

.PHONY: help install install-uv dev test smoke lint format clean databricks-sync

help:
	@echo "Available targets:"
	@echo "  install       — install dependencies via pip"
	@echo "  install-uv    — install dependencies via uv (faster)"
	@echo "  dev           — run tests in watch mode"
	@echo "  test          — run all tests once"
	@echo "  smoke         — run local end-to-end smoke against test parquet"
	@echo "  lint          — ruff check"
	@echo "  format        — ruff format"
	@echo "  clean         — remove caches"
	@echo "  databricks-sync — sync repo to Databricks workspace (requires DATABRICKS_CONFIG_PROFILE)"

install:
	pip install -r requirements.txt

install-uv:
	uv pip install -r requirements.txt

dev:
	python -m pytest tests/ -v --color=yes -f

test:
	python -m pytest tests/ -v --color=yes

smoke:
	python scripts/smoke_bronze.py

lint:
	ruff check src/ agent/ tests/ scripts/

format:
	ruff format src/ agent/ tests/ scripts/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true

databricks-sync:
	databricks repos update --update-path "$(REPO_PATH)" --branch "$(BRANCH)"
