.PHONY: build run test lint lint-fix format

build:
	docker compose build

run:
	docker compose up

test:
	uv run --frozen pytest

lint:
	ruff check .

lint-fix:
	ruff check . --fix

format:
	ruff format .
