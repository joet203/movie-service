# Plan: Movie Database Service (Final State)

## Goal

Deliver a FastAPI service that satisfies all assessment requirements with clear, reviewable code and tests.

## Requirement Mapping

1. Upload `movies.csv`:
   - `POST /datasets` returns `202` + `task_id`
   - Ingestion runs in background task
2. Download full dataset as gzipped CSV:
   - `GET /datasets/download` for direct fast path
   - Long-running exports return `202` + `task_id`; artifact fetched via `GET /tasks/{task_id}/download`
3. Query by year range and genre:
   - `GET /movies?start_year=&end_year=&genre=`
4. Real-time progress for requests >2s:
   - `GET /tasks/{task_id}/events` SSE for upload, query, and export tasks
   - `/movies` and `/datasets/download` automatically hand off to task mode when threshold is exceeded

## Architecture

- FastAPI with lifespan startup/shutdown
- DuckDB on-disk database (`movies.duckdb`)
- Per-operation DuckDB connections (no shared singleton connection object)
- In-memory task registry with:
  - thread-safe updates
  - TTL/max-count pruning
  - artifact cleanup for expired/deleted tasks

## Execution Plan (Implemented)

1. Implement upload + background ingestion with staging-table atomic swap.
2. Add SSE task status stream.
3. Implement query filtering/sorting/pagination with parameterized SQL.
4. Add long-query task handoff (`202` after 2s threshold).
5. Implement gzip dataset export.
6. Add long-export task handoff (`202` after 2s threshold) + task artifact download endpoint.
7. Add task pruning and shutdown cleanup.
8. Add tests for core endpoints, long-request handoff, pruning, and DB connection safety.
9. Document endpoint behavior and AI workfiles for reviewer clarity.

## Verification Commands

```bash
uv sync
uv run pytest -q
uv run fastapi dev main.py
```

## Review Aids

- `README.md` for quick run/use instructions
- `DESIGN.md` for rationale and trade-offs
- `AI_WORKFILES.md` for AI artifact manifest
