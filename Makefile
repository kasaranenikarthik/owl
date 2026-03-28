.PHONY: up down logs logs-ingest logs-inference logs-aggregator test lint clean

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f

logs-ingest:
	docker compose logs -f ingest

logs-inference:
	docker compose logs -f inference

logs-aggregator:
	docker compose logs -f aggregator

test:
	python scripts/smoke_test.py

lint:
	ruff check common/ schemas/ ingest/ inference/ aggregator/

clean:
	docker compose down -v --remove-orphans
	docker image prune -f
