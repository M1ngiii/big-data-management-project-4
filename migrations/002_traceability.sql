-- Adds traceability, observability, and audit tables required by the
-- homework project. Runs after 001_init.sql on a fresh volume; can also
-- be applied manually to an existing instance with:
--   docker compose exec postgres psql -U rico -d rico -f /docker-entrypoint-initdb.d/002_traceability.sql

\c rico

-- ── New tables ────────────────────────────────────────────────────────────────

-- One row per DAG run. Created at run start, updated at run end.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID PRIMARY KEY,
    dag_run_id      TEXT NOT NULL UNIQUE,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'succeeded', 'failed', 'paused-by-audit')),
    limit_param     INTEGER NOT NULL,
    git_sha         TEXT,
    clip_version    TEXT,
    sbert_version   TEXT,
    llm_model       TEXT,
    prompt_version  TEXT
);

-- One row per metric per run. Numeric metrics use metric_value;
-- text/JSON metrics use metric_text.
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id            BIGSERIAL PRIMARY KEY,
    run_id        UUID NOT NULL REFERENCES pipeline_runs(run_id),
    metric_name   TEXT NOT NULL,
    metric_value  DOUBLE PRECISION,
    metric_text   TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, metric_name)
);

-- Audit results — one row per audit check per run.
CREATE TABLE IF NOT EXISTS audit_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES pipeline_runs(run_id),
    audit_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Extend existing tables ────────────────────────────────────────────────────

-- screens_metadata: which run wrote this row, and what were the source bytes?
ALTER TABLE screens_metadata
    ADD COLUMN IF NOT EXISTS run_id             UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

-- screens_embeddings: same traceability columns.
ALTER TABLE screens_embeddings
    ADD COLUMN IF NOT EXISTS run_id             UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

-- screens_review_queue: tag every queued row to the run that produced it.
ALTER TABLE screens_review_queue
    ADD COLUMN IF NOT EXISTS run_id             UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

-- screens_eval: tag eval results to the run/input that computed them.
ALTER TABLE screens_eval
    ADD COLUMN IF NOT EXISTS run_id UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS screens_eval_model_source_uq
    ON screens_eval (embedding_model_version, source_fingerprint)
    WHERE source_fingerprint IS NOT NULL;
