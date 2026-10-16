.PHONY: install test lint typecheck format contracts run-all report serve up demo down clean

# Dev-only salt so the demo runs out of the box. Use a real secret anywhere else.
export LEVENTS_PII_SALT ?= local-dev-salt-not-secret

install:
	pip install -e ".[dev]"

test:
	pytest --cov-fail-under=80

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy src

format:
	ruff format .
	ruff check --fix .

contracts:
	levents check-contracts

# generate -> ingest through the API -> dbt build -> docs/results.md, chart, HTML reports
run-all:
	levents run-all

report:
	levents report

serve:
	levents serve

up:
	docker compose up -d --build postgres api

demo: up
	docker compose --profile demo run --rm pipeline

down:
	docker compose down -v

clean:
	rm -rf data reports dbt/target dbt/logs
