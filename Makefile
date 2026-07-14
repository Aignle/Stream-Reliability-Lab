PYTHON ?= python3

.PHONY: install browser-install run-api run-dashboard demo test test-unit test-integration test-e2e test-scenario lint format typecheck deps-check js-check check verify compose-up compose-down compose-check

install:
	$(PYTHON) -m pip install -e ".[dev]"

browser-install:
	$(PYTHON) -m playwright install chromium

run-api:
	$(PYTHON) -m uvicorn streamlab.main:app --host 127.0.0.1 --port 8000

run-dashboard:
	$(PYTHON) -m streamlit run src/streamlab/dashboard.py

demo:
	$(PYTHON) -m streamlab.simulator --scenario reconnect_burst --seed 20250314 --count 500 --rate 1000 --burst-rate 5000 --overlay-wait 120

test:
	$(PYTHON) -m pytest

test-unit:
	$(PYTHON) -m pytest tests/unit

test-integration:
	$(PYTHON) -m pytest tests/integration tests/dashboard

test-e2e:
	$(PYTHON) -m pytest -m e2e

test-scenario:
	$(PYTHON) -m pytest -m scenario

lint:
	$(PYTHON) -m ruff check .

format:
	$(PYTHON) -m ruff format --check .

typecheck:
	$(PYTHON) -m mypy src

deps-check:
	$(PYTHON) -m pip check

js-check:
	node --check src/streamlab/static/overlay.js

compose-up:
	docker compose up --build --wait

compose-down:
	docker compose down

compose-check:
	docker compose config --quiet

check: test lint format typecheck deps-check js-check

verify: check test-e2e test-scenario compose-check
