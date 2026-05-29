# RICO Airflow Pipeline

This project turns the RICO notebook lab into a production-style Airflow DAG.
It ingests Android screen data from HuggingFace, stores raw assets in MinIO,
writes metadata and pgvector embeddings to Postgres, runs a local Ollama LLM
extractor, audits duplicate embeddings, evaluates recall@5, records metrics,
and sends Slack notifications.

The DAG is intentionally thin: orchestration lives in `dags/rico_pipeline.py`,
while business logic lives in `tasks/`.

## What Runs

The Airflow DAG is `rico_pipeline`.

Stage order:

```text
setup -> ingest -> parse -> [embed_image, embed_text, extract] -> load -> audit -> eval
```

The middle three tasks run in parallel after `parse`.

Main stores:

| Store | Purpose |
| --- | --- |
| Postgres + pgvector | metadata, embeddings, run traceability, audits, metrics, eval |
| MinIO | raw PNG and hierarchy JSON blobs |
| Ollama | local LLM extraction endpoint |
| Slack | run started, audit failed, and run finished notifications |

## Requirements

Install locally:

- Docker (Docker Desktop on Mac/Windows; Docker Engine + Compose plugin on Linux)
- Git
- Python 3.11, only needed for notebook/local development

You do not need local Postgres, MinIO, Ollama, or Airflow. Docker Compose runs
those services.

## Environment

The repo includes `.env.example`. Real secrets belong in `.env`, which is
gitignored.

Important variables:

| Variable | Meaning |
| --- | --- |
| `SLACK_WEBHOOK_URL` | Incoming Slack webhook URL. Required for the assignment notification requirement. |
| `GIT_SHA` | Commit hash stored in `pipeline_runs.git_sha`. |
| `OLLAMA_MODEL` | Defaults to `qwen2.5:3b`. |
| `MINIO_BUCKET` | Defaults to `rico-raw`. |

Set these in your shell before running `make up`, or add them to your `.env` file.

Linux / macOS:

```bash
export GIT_SHA=$(git rev-parse HEAD)
export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

Windows `cmd`:

```cmd
for /f %i in ('git rev-parse HEAD') do set GIT_SHA=%i
set SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

## Start The Stack

From the repo root:

```bash
make up
```

Or manually run all the services:

```bash
docker compose up -d --wait postgres minio ollama
docker compose up -d minio-init ollama-init
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Check containers:

```bash
docker compose ps
```

Airflow UI:

```text
http://localhost:8080
admin / admin
```

MinIO UI:

```text
http://localhost:9001
minioadmin / minioadmin
```

If you change mounted files or environment variables, recreate Airflow:

```bash
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

## Run The DAG

After `make up`, wait a minute or two before triggering the DAG. The `ollama-init`
container pulls the LLM model in the background and the `extract` task will fail
if the model is not ready yet. You can check progress with:

```bash
docker compose logs ollama-init
```

Once you see `ollama model ready`, open Airflow:

1. Open `rico_pipeline`.
2. Unpause the DAG if needed.
3. Trigger it manually.
4. Use `limit = 5` for development.

The `LIMIT=5` path uses curated screen IDs from `chosen_screens.txt`:

```text
2, 26, 37, 41, 50
```

These cover five apps and five categories, which makes the data-quality metrics
useful in a small demo.

## Expected Successful Run

For `LIMIT=5`, a healthy run should produce:

| Table | Expected rows |
| --- | --- |
| `pipeline_runs` | one row for the DAG run |
| `screens_metadata` | 5 rows |
| `screens_embeddings` | 10 rows, image + text per screen |
| `audit_results` | one passed duplicate-detection result |
| `pipeline_metrics` | health and data-quality metrics |
| `screens_eval` | one fingerprinted eval row for this dataset/model |

Use:

```bash
docker compose exec postgres psql -U rico -d rico
```

Useful checks:

```sql
SELECT run_id, dag_run_id, status, limit_param, git_sha, started_at, ended_at
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 5;

SELECT COUNT(*) AS metadata_rows FROM screens_metadata;
SELECT COUNT(*) AS embedding_rows FROM screens_embeddings;
SELECT COUNT(*) AS metrics_rows FROM pipeline_metrics;
SELECT COUNT(*) AS audit_rows FROM audit_results;
SELECT COUNT(*) AS eval_rows FROM screens_eval;
```

For the latest run:

```sql
SELECT screen_id, app_package, category
FROM screens_metadata
WHERE run_id = 'PASTE_RUN_UUID_HERE'
ORDER BY screen_id;
```

Expected for `LIMIT=5`: five distinct packages and categories.

## Idempotency

Re-triggering the DAG with the same `LIMIT=5` does not duplicate destination
data:

- `screens_metadata` upserts by `screen_id`.
- `screens_embeddings` upserts by `(screen_id, model_name, model_version, embedding_kind)`.
- `screens_eval` upserts by `(embedding_model_version, source_fingerprint)`.
- `screens_review_queue` upserts by `(screen_id, source_fingerprint, reason)`.
- `pipeline_runs`, `pipeline_metrics`, and `audit_results` are run history and
  are expected to grow per DAG run.

Check eval idempotency:

```sql
SELECT embedding_model_version, source_fingerprint, COUNT(*) AS n
FROM screens_eval
WHERE source_fingerprint IS NOT NULL
GROUP BY embedding_model_version, source_fingerprint
ORDER BY n DESC;
```

For repeated `LIMIT=5` runs with the same model/input, the grouped count stays at `1`.

## Traceability

Every destination row is tied back to a pipeline run and source input.

Core traceability columns:

| Table | Traceability |
| --- | --- |
| `pipeline_runs` | `run_id`, Airflow `dag_run_id`, timestamps, status, `git_sha`, model versions |
| `screens_metadata` | `run_id`, `source_fingerprint` of PNG bytes |
| `screens_embeddings` | `run_id`, `source_fingerprint` of embedding input |
| `screens_review_queue` | `run_id`, `source_fingerprint` of failed extraction input |
| `screens_eval` | `run_id`, `source_fingerprint` of evaluated text-vector set |

Check for missing traceability:

```sql
SELECT
  COUNT(*) AS metadata_rows,
  COUNT(*) FILTER (WHERE run_id IS NULL) AS missing_run_id,
  COUNT(*) FILTER (WHERE source_fingerprint IS NULL) AS missing_fingerprint
FROM screens_metadata;

SELECT
  COUNT(*) AS embedding_rows,
  COUNT(*) FILTER (WHERE run_id IS NULL) AS missing_run_id,
  COUNT(*) FILTER (WHERE source_fingerprint IS NULL) AS missing_fingerprint
FROM screens_embeddings;

SELECT
  COUNT(*) AS review_rows,
  COUNT(*) FILTER (WHERE run_id IS NULL) AS missing_run_id,
  COUNT(*) FILTER (WHERE source_fingerprint IS NULL) AS missing_fingerprint
FROM screens_review_queue;
```

## Audit Failure Drill

The required audit is duplicate detection. It checks:

- no duplicate logical embeddings for `(screen_id, model_version, embedding_kind)`
- no missing image/text embeddings for screens in the run

To prove the audit halts the DAG, insert one duplicate logical embedding:

```sql
INSERT INTO screens_embeddings
  (screen_id, model_name, model_version, embedding_kind, vector, run_id, source_fingerprint)
SELECT
  screen_id,
  model_name || '-duplicate',
  model_version,
  embedding_kind,
  vector,
  run_id,
  source_fingerprint
FROM screens_embeddings
WHERE embedding_kind = 'text'
LIMIT 1;
```

Confirm:

```sql
SELECT screen_id, model_version, embedding_kind, COUNT(*) AS n
FROM screens_embeddings
GROUP BY screen_id, model_version, embedding_kind
HAVING COUNT(*) > 1;
```

Trigger the DAG with `limit = 5`.

Expected:

- `audit` task fails.
- `eval_` is skipped.
- `pipeline_runs.status = 'paused-by-audit'`.
- `audit_results.passed = false`.
- Slack posts an audit-failed alert with duplicate keys and the Airflow task log URL.

Check:

```sql
SELECT run_id, dag_run_id, status, started_at, ended_at
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 1;

SELECT ar.audit_name, ar.passed, ar.details
FROM pipeline_runs pr
JOIN audit_results ar ON ar.run_id = pr.run_id
ORDER BY pr.started_at DESC
LIMIT 1;

SELECT COUNT(*) AS eval_rows_for_latest_run
FROM screens_eval
WHERE run_id = (
  SELECT run_id
  FROM pipeline_runs
  ORDER BY started_at DESC
  LIMIT 1
);
```

Clean up the deliberate duplicate:

```sql
DELETE FROM screens_embeddings
WHERE model_name LIKE '%-duplicate';
```

Then confirm the duplicate query returns zero rows.

## Metrics

Metrics are stored in `pipeline_metrics`.

```sql
SELECT metric_name, metric_value, metric_text
FROM pipeline_metrics
WHERE run_id = 'PASTE_RUN_UUID_HERE'
ORDER BY metric_name;
```

Pipeline health metrics:

| Metric | Meaning |
| --- | --- |
| `run.total_duration_s` | Total run duration in seconds |
| `run.final_status` | Final status as text: `succeeded`, `failed`, or `paused-by-audit` |
| `task.<task>.duration_s` | Task wall-clock duration |
| `task.<task>.retries` | Completed retry count for the task |
| `task.<task>.row_count_in` | Task input row/object count |
| `task.<task>.row_count_out` | Task output row/object count |
| `task.extract.row_count_queued` | Number of extraction failures routed to review |

Data-quality metrics:

| Metric | Meaning |
| --- | --- |
| `meta.row_count` | Number of metadata rows for the run |
| `meta.pct_extracted` | Percent of metadata rows with non-null extraction payload |
| `meta.pct_high_confidence` | Percent with `confidence >= 0.5` |
| `meta.pct_review_queue` | Percent routed to review queue |
| `meta.distinct_packages` | Distinct `app_package` count |
| `meta.distinct_categories` | Distinct category count |
| `emb.image.row_count`, `emb.text.row_count` | Embedding row counts by kind |
| `emb.<kind>.min_dims`, `max_dims`, `avg_dims` | Vector dimensionality checks |
| `emb.<kind>.dims_consistent` | `1` if min/max dimensions match |
| `emb.<kind>.pct_zero_norm` | Percent pure-zero vectors |

Model-version-specific embedding metrics are also stored, for example:

```text
emb.open-clip-ViT-B-32-laion2b-s34b-b79k.image.row_count
emb.sentence-transformers_all-MiniLM-L6-v2.text.row_count
```

The final Slack message includes a one-line summary such as:

```text
screens=5 extracted=100% high_conf=100% review_queue=0% packages=5 categories=5 image_rows=5 text_rows=5 dims_ok=true zero_norm=0%
```

## Slack Notifications

The pipeline posts three kinds of Slack messages:

| Moment | Contents |
| --- | --- |
| Run started | `run_id`, limit, trigger type |
| Audit failed | duplicate details, `run_id`, Airflow task log URL |
| Run finished | final status, duration, health/data-quality summary |

Slack failures are non-fatal. If `SLACK_WEBHOOK_URL` is missing or invalid, the
pipeline logs a warning or skips the post, but the DAG work continues.

Quick webhook smoke test:

```bash
docker compose exec airflow-scheduler python -c "import os, requests; r=requests.post(os.environ['SLACK_WEBHOOK_URL'], json={'text':'RICO pipeline webhook smoke test'}, timeout=10); print(r.status_code, r.text[:200])"
```

Expected:

```text
200 ok
```

## Reset And Cleanup

Truncate all pipeline tables and clear the MinIO bucket, keeping Docker volumes intact:

```bash
make reset
```

Stop services, preserving volumes:

```bash
docker compose down
```

Full wipe (removes all volumes — re-runs migrations on next `make up`):

```bash
make clean
```

Truncate pipeline state manually without using make:

```bash
docker compose exec postgres psql -U rico -d rico
```

```sql
TRUNCATE TABLE pipeline_runs, audit_results, pipeline_metrics,
screens_metadata, screens_embeddings, screens_review_queue, screens_eval
RESTART IDENTITY CASCADE;
```

## Troubleshooting

**DAG does not appear or has import errors.**

```bash
docker compose logs --tail=150 airflow-scheduler
docker compose exec airflow-scheduler airflow dags list-import-errors
```

**git_sha is unknown.**

Set `GIT_SHA` before recreating Airflow.

Linux / macOS:

```bash
export GIT_SHA=$(git rev-parse HEAD)
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

Windows `cmd`:

```cmd
for /f %i in ('git rev-parse HEAD') do set GIT_SHA=%i
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

**No Slack messages.**

Confirm Airflow sees the webhook:

```bash
docker compose exec airflow-scheduler python -c "import os; print(bool(os.getenv('SLACK_WEBHOOK_URL')))"
```

**Audit keeps failing.**

Run the duplicate query from the audit drill. If it returns rows, remove the
deliberate duplicate with:

```sql
DELETE FROM screens_embeddings
WHERE model_name LIKE '%-duplicate';
```

**relation "pipeline_runs" does not exist on first run.**

The Postgres init scripts only run on a completely empty volume. If the volume
already existed before the migrations were added, the tables will be missing.
Fix with a full wipe and restart:

```bash
make clean
make up
```

**First run is slow.**

The first run downloads or warms up Docker images, Ollama model data, CLIP, and
SBERT. Later runs are faster.

## Files

| File | Purpose |
| --- | --- |
| `dags/rico_pipeline.py` | Airflow DAG orchestration |
| `tasks/` | Pipeline task implementations |
| `migrations/001_init.sql` | Base Postgres tables and pgvector |
| `migrations/002_traceability.sql` | Traceability, metrics, audit, idempotency additions |
| `docker-compose.yml` | Local stack |
| `Dockerfile.airflow` | Airflow image with pipeline dependencies |
| `requirements-airflow.txt` | Packages installed in the Airflow image |
| `chosen_screens.txt` | Curated diverse demo screens |
| `pyproject.toml` | Package metadata for the `tasks/` module |
| `.env.example` | Safe environment template |
