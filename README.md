# RICO Pipeline Lab

A 90-minute Jupyter lab that re-derives the production RICO pipeline from primitives. You'll ingest 5 screens from HuggingFace, embed them with CLIP and SBERT, run an LLM extractor, search across the triad, and compute an honest recall@k — talking directly to a Postgres + pgvector + MinIO + Ollama stack you bring up locally with one command.

The notebook is the lecture's hands-on counterpart. It optimizes for **readability, linearity, and conceptual clarity** — not for production-credibility. No tests, no hexagonal architecture, no idempotency. One file, top to bottom, run all.

This `lab/` folder is **self-contained** — it ships its own `docker-compose.yml`, `Makefile`, and Postgres migrations. You don't need anything outside this directory to run the lab.

## Prerequisites

You need:

- **Docker Desktop** (or any Docker daemon).
- **Python 3.11** on your laptop.
- ~3 GB free disk for model weights and the HuggingFace dataset shard.

## Quickstart

From inside `week07/`:

```bash
make up           # starts Postgres+pgvector, MinIO, Ollama (waits until healthy)
make pull-models  # pulls qwen2.5:3b into Ollama (one-time, ~1.9 GB)

python3.11 -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
jupyter lab notebook.ipynb
```

Then `Run → Run All Cells`. The notebook walks through Sections 0–8 (Setup → Ingest → Parse → Image embeddings → Text embeddings → LLM extraction → Search → Eval → Where next).

When you're done, `make down` (preserves data) or `make clean` (full wipe).

## Connection details

The notebook hardcodes these — they match `docker-compose.yml` here in `lab/`:

| Service  | Endpoint                  | Credentials              |
|----------|---------------------------|--------------------------|
| Postgres | `localhost:5432/rico`     | `rico` / `rico`          |
| MinIO    | `http://localhost:9000`   | `minioadmin` / `minioadmin` (bucket: `rico-raw`) |
| Ollama   | `http://localhost:11434`  | model: `qwen2.5:3b`      |

The MinIO web console is at <http://localhost:9001> if you want to browse the bucket. If you override credentials via `.env` (see `.env.example`), edit the constants in the notebook's Section 0 imports cell to match.

## Time budget

| Run            | Wall time | What dominates                              |
|----------------|-----------|----------------------------------------------|
| First run      | 4–8 min   | CLIP weights (~600 MB) + Ollama model warm-up + 5 sequential LLM calls |
| Second run     | ~90 s     | 5 sequential Ollama LLM calls (~12–20 s each) |

The slow cell every time is **Section 5 (LLM extraction)** — five `qwen2.5:3b` calls in series at CPU speed. Production solves this with concurrency; the notebook doesn't.

## Re-running

The notebook is **non-idempotent on purpose**. Re-running it without restarting the kernel will hit a primary-key violation on `INSERT INTO screens_metadata` (Section 1) or `INSERT INTO screens_embeddings` (Sections 3, 4). To start fresh:

1. **Restart the kernel** (`Kernel → Restart Kernel`).
2. **Truncate state**:
   ```bash
   make reset    # truncates the four tables and clears the MinIO bucket
   ```
3. `Run All Cells` again.

If something is deeply wrong, `make clean && make up && make pull-models` resets everything, including Ollama's model cache.

## Troubleshooting

**`psycopg.OperationalError: connection refused` (Section 0).**
Postgres container isn't running or hasn't finished starting. Run `docker compose ps` from `lab/` — wait for `(healthy)`.

**`AssertionError: bucket 'rico-raw' missing` (Section 0).**
The `minio-init` container creates the bucket on first `make up`. If `make clean` was run recently, run `make up` again to re-trigger init.

**`AssertionError: model 'qwen2.5:3b' not pulled` (Section 0).**
Run `make pull-models`.

**`json.JSONDecodeError` in Section 5.**
The LLM occasionally returns invalid JSON. That's the failure mode production code handles by routing to a review queue. The notebook just crashes — re-run the cell and the next attempt usually parses cleanly. If it fails repeatedly, restart the kernel and re-run from Section 0.

**`UniqueViolation` (any INSERT cell).**
You ran the section twice without truncating. See "Re-running" above.

**CLIP first-load takes forever.**
Open-clip downloads ~600 MB to `~/.cache/huggingface` on first use. There's no progress bar in nbconvert; check `du -sh ~/.cache/huggingface/hub` from another terminal to confirm progress.

## What's in this directory

| File                     | Purpose                                                  |
|--------------------------|----------------------------------------------------------|
| `notebook.ipynb`         | The lab itself — 43 cells, ~5 min E2E.                   |
| `requirements.txt`       | Laptop-side pip deps (talks to docker-compose services). |
| `chosen_screens.txt`     | The 5 screen IDs the notebook ingests (5 categories).    |
| `docker-compose.yml`     | Lab-only stack: Postgres+pgvector, MinIO, Ollama.        |
| `Makefile`               | `up`, `down`, `clean`, `pull-models`, `reset`.           |
| `migrations/001_init.sql`| Postgres bootstrap — creates `pgvector` + the four tables. |
| `.env.example`           | Optional connection overrides. Defaults are fine.        |
| `README.md`              | This file.                                               |
