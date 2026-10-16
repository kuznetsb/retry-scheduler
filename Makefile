.PHONY: build run test lint lint-fix format

build:
	docker compose build

run:
	docker compose up

test:
	uv run --frozen pytest

lint:
	uv run --frozen ruff check .

lint-fix:
	uv run --frozen ruff check . --fix

format:
	uv run --frozen ruff format .
