.PHONY: help build airflow-init up down clean pull-models reset logs

COMPOSE := docker compose

OLLAMA_MODEL     ?= qwen2.5:3b
POSTGRES_USER    ?= rico
POSTGRES_DB      ?= rico
MINIO_ACCESS_KEY ?= minioadmin
MINIO_SECRET_KEY ?= minioadmin
MINIO_BUCKET     ?= rico-raw

help:
	@echo "Targets:"
	@echo "  build        build the Airflow Docker image (run once; subsequent runs use cache)"
	@echo "  up           start all services and initialize Airflow (first run ~5 min for image build)"
	@echo "  pull-models  pull qwen2.5:3b into Ollama (run once after first make up)"
	@echo "  down         stop services (volumes preserved)"
	@echo "  clean        stop services and wipe all volumes (full reset)"
	@echo "  reset        truncate all pipeline tables + clear MinIO bucket"
	@echo "  logs         tail compose logs for all services"
	@echo ""
	@echo "Airflow UI : http://localhost:8080  (admin / admin)"
	@echo "MinIO UI   : http://localhost:9001  (minioadmin / minioadmin)"

build:
	$(COMPOSE) build airflow-webserver

# Run Airflow DB migration + admin user creation (safe to re-run; migrate is idempotent).
airflow-init:
	$(COMPOSE) run --rm airflow-init

up:
	$(COMPOSE) up -d --wait postgres minio ollama
	$(COMPOSE) up -d minio-init ollama-init
	$(MAKE) airflow-init
	$(COMPOSE) up -d airflow-webserver airflow-scheduler
	@echo ""
	@echo "Stack ready."
	@echo "  Airflow UI : http://localhost:8080  (admin / admin)"
	@echo "  MinIO UI   : http://localhost:9001  (minioadmin / minioadmin)"

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

pull-models:
	$(COMPOSE) exec ollama ollama pull $(OLLAMA_MODEL)

# Truncate all pipeline state without re-pulling models or rebuilding volumes.
reset:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
	  "TRUNCATE TABLE pipeline_runs, audit_results, pipeline_metrics, \
	   screens_metadata, screens_embeddings, screens_review_queue, screens_eval \
	   RESTART IDENTITY CASCADE;"
	$(COMPOSE) exec minio mc alias set local http://minio:9000 $(MINIO_ACCESS_KEY) $(MINIO_SECRET_KEY) >/dev/null
	$(COMPOSE) exec minio mc rm --recursive --force local/$(MINIO_BUCKET)/ >/dev/null 2>&1 || true
	@echo "pipeline state truncated"

logs:
	$(COMPOSE) logs -f --tail=100
