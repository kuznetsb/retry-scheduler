.PHONY: lint lint-fix format

lint:
	ruff check .

lint-fix:
	ruff check . --fix

format:
	ruff format .
