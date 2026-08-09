.PHONY: help up down logs generate test clean status

# Default target
help:
	@echo ""
	@echo "PulseFlow - Real-Time Data Intelligence Platform"
	@echo "================================================"
	@echo ""
	@echo "Setup & Control:"
	@echo "  make up          Start all services (Kafka, Postgres, API, Consumer)"
	@echo "  make down        Stop and remove all containers"
	@echo "  make restart     Restart all services"
	@echo "  make status      Show container health status"
	@echo ""
	@echo "Data Generation:"
	@echo "  make generate    Generate 1M synthetic events (runs in Docker)"
	@echo "  make generate-local  Generate events using local Python"
	@echo ""
	@echo "Development:"
	@echo "  make logs        Tail logs from all services"
	@echo "  make logs-consumer  Tail consumer logs only"
	@echo "  make logs-api    Tail API logs only"
	@echo "  make test        Run test suite"
	@echo "  make test-cov    Run tests with coverage report"
	@echo ""
	@echo "Inspection:"
	@echo "  make health      Check pipeline health via API"
	@echo "  make funnel      Show conversion funnel"
	@echo "  make dlq         Show dead letter queue contents"
	@echo "  make psql        Open psql shell"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean       Remove containers and volumes (DELETES DATA)"
	@echo ""

up:
	docker compose up -d --build
	@echo ""
	@echo "Services starting..."
	@echo "  Kafka UI:  http://localhost:8080"
	@echo "  API:       http://localhost:8000"
	@echo "  API Docs:  http://localhost:8000/docs"
	@echo ""
	@echo "Run 'make logs' to watch the pipeline"

down:
	docker compose down

restart:
	docker compose restart

status:
	docker compose ps

generate:
	docker compose --profile generate run --rm generator

generate-small:
	TOTAL_EVENTS=50000 EVENTS_PER_SECOND=200 docker compose --profile generate run --rm generator

logs:
	docker compose logs -f --tail=50

logs-consumer:
	docker compose logs -f --tail=100 consumer

logs-api:
	docker compose logs -f --tail=50 api

logs-generator:
	docker compose logs -f --tail=50 generator

test:
	python -m pytest tests/ -v

test-cov:
	python -m pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html

health:
	@curl -s http://localhost:8000/health | python3 -m json.tool

funnel:
	@curl -s "http://localhost:8000/events/funnel?hours=24" | python3 -m json.tool

summary:
	@curl -s "http://localhost:8000/events/summary?hours=1" | python3 -m json.tool

dlq:
	@curl -s "http://localhost:8000/dlq?limit=10" | python3 -m json.tool

psql:
	docker exec -it pulseflow-postgres psql -U pulseflow -d pulseflow

clean:
	docker compose down -v
	@echo "All containers and volumes removed."

# Show event counts by type directly from Postgres
db-summary:
	docker exec -it pulseflow-postgres psql -U pulseflow -d pulseflow -c \
		"SELECT event_type, COUNT(*) as count, \
		 COUNT(DISTINCT user_id) as unique_users, \
		 ROUND(COUNT(*) FILTER (WHERE is_duplicate)::NUMERIC / COUNT(*) * 100, 2) as dupe_pct, \
		 ROUND(COUNT(*) FILTER (WHERE NOT is_valid)::NUMERIC / COUNT(*) * 100, 2) as invalid_pct \
		 FROM raw_events GROUP BY event_type ORDER BY count DESC;"
