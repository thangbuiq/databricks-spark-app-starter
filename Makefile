.PHONY: help format lint check install dev-install clean

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	uv sync --no-dev

dev-install: ## Install development dependencies
	uv sync --extra dev

format: ## Format code with ruff (includes isort and formatting)
	uv run ruff check --fix .
	uv run ruff format .

lint: ## Run linting checks
	uv run ruff check .

check: lint ## Alias for lint

clean: ## Clean up build artifacts and cache
	rm -rf .ruff_cache/
	rm -rf __pycache__/
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete