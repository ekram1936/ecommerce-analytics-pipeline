.PHONY: help install run stop test lint clean

help:
	@echo "Available commands:"
	@echo "  make install    - Install dependencies"
	@echo "  make run        - Start full pipeline (Docker + producer)"
	@echo "  make stop       - Stop all services"
	@echo "  make test       - Run all tests"
	@echo "  make lint       - Run code linting"
	@echo "  make clean      - Remove generated files"

install:
	pip install -r requirements.txt

infra-up:
	docker compose -f docker/docker-compose.yml up -d
	@echo "Waiting for Kafka to be ready..."
	sleep 10

infra-down:
	docker compose -f docker/docker-compose.yml down

run: infra-up
	python -m src.producer.olist_producer

stop: infra-down

test:
	pytest tests/ -v --tb=short

lint:
	ruff check src/ tests/
	black --check src/ tests/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete
	rm -rf delta-lake/
