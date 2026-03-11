# Movie Database Service

## Quick Reference

- Run: `make run` (or `uv run fastapi dev main.py`)
- Test: `make test` (or `uv run pytest -q`)
- Bench: `make bench` (requires `movies.csv` in project root)
- Swagger: `http://localhost:8000/docs`
- Frontend: `http://localhost:8000`

## Stack

- Python 3.13, FastAPI, DuckDB (embedded OLAP), Pydantic
- CSV ingestion: stdlib `csv` for header validation + DuckDB `read_csv` for bulk load
- Runtime dependencies: `duckdb`, `python-multipart`

## Runtime Configuration

- `MOVIES_DB_PATH`: DuckDB file path (default `movies.duckdb`)
- `MOVIES_MAX_QUERY_LIMIT`: max allowed `limit` for `/movies` and `/movies/query` (default `50000`)
- `MOVIES_TASK_POLL_INTERVAL_SECONDS`: SSE poll interval for `/tasks/{id}/events` (default `0.2`)

## API Surface

- `POST /datasets`: upload CSV, returns `202` + `task_id`
- `GET /tasks/{id}/events`: SSE task progress stream
- `GET /movies`: filtered query (returns `200` rows, or `202` + `task_id` when query exceeds threshold)
- `POST /movies/query`: always async query (`202` + `task_id`)
- `GET /tasks/{id}/results`: fetch completed async query results
- `GET /datasets/download`: gzip download (`200` direct, or `202` + `task_id` when export prep exceeds threshold)
- `GET /tasks/{id}/download`: fetch completed async export artifact
- `GET /health`: health check
- `GET /sample-data`: sample `movies.csv`

## Project Structure

```
main.py              FastAPI app, lifespan, frontend/static routes
app/
  db.py              DuckDB init + per-operation connection factory + task registry
  model.py           Pydantic response models
  movies.py          Endpoint handlers, ingestion/query/export task flows
frontend/
  index.html         Main dashboard UI
  design.html        Design rationale page
  interview.html     Interview prep page
  plan.html          Build-plan artifact page
tests/
  conftest.py        Test fixtures (in-memory DB overrides)
  test_movies.py     33 tests across upload/query/download/task flows
DESIGN.md            Design choices and tradeoffs
plan.md              AI planning workfile
AI_WORKFILES.md      AI artifact index
benchmark.py         Benchmark harness
Makefile             Dev workflow shortcuts
```

## Core Design Choices

- DuckDB for out-of-core analytical workloads and efficient scan/filter execution
- Streaming upload (1 MB chunks) and streaming download (64 KB chunks)
- Staging table + atomic swap for safe full-dataset replacement
- SSE progress model for ingestion, long queries, and long export preparation
- Auto async handoff for long requests (>2s threshold)
- In-memory task registry with lock, TTL/max-count pruning, and export artifact cleanup
