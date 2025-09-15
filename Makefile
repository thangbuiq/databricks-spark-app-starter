.PHONY: install format whl

check-uv:
	@if ! command -v uv &> /dev/null; then \
		echo "uv could not be found, please install it first. Visit https://uv.io for more information."; \
		exit 1; \
	fi

install: check-uv
	@uv sync --all-groups --all-extras

format: check-uv
	@uv run ruff check --select I,F --fix .
	@uv run ruff format .

whl: format check-uv
	@uv build --verbose --wheel
