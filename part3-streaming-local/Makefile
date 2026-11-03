# Convenience targets. `make help` lists them.
SHELL := /bin/bash
PY    := .venv/bin/python
DAY   ?= 2026-10-01
DATA  ?= data/events-$(DAY).jsonl

.PHONY: help venv data up down topic produce consume simulate console clean

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/' | column -t -s$$'\t'

venv: ## create the python venv and install the kafka client
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt

data: ## download one day of events from the public bucket
	mkdir -p data
	aws s3 cp s3://ucu-ooad-lab8-payments/payments/dt=$(DAY)/events.jsonl $(DATA) \
	    --no-sign-request --region us-east-1 --no-progress

up: ## start redpanda + console
	docker compose up -d
	@echo "waiting for the broker to report healthy..."
	@until docker compose exec -T redpanda rpk cluster health 2>/dev/null | grep -q 'Healthy:.*true'; do sleep 2; done
	@echo "ready — console at http://localhost:8080"

topic: ## create the payments topic with 3 partitions
	docker compose exec -T redpanda rpk topic create payments -p 3 -r 1 || true
	docker compose exec -T redpanda rpk topic describe payments

produce: ## replay one day into the topic (~20 payments/s). make produce ARGS="--fail-rate 0.30"
	$(PY) producer.py --file $(DATA) $(ARGS)

consume: ## run the windowed consumer. make consume ARGS="--grace 0"
	$(PY) consumer.py $(ARGS)

simulate: ## run the window logic over the file with no broker at all
	$(PY) simulate.py --file $(DATA) $(ARGS)

console: ## open the web console
	xdg-open http://localhost:8080

down: ## stop everything
	docker compose down

clean: ## stop and delete the broker's data volume
	docker compose down -v
