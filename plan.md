# Plan: Movie Database Service (Extended Branch)

## Goal

Ship a FastAPI + DuckDB service that satisfies the assessment requirements, stays responsive on larger datasets, and remains easy for reviewers to run and inspect.

## Requirement Mapping

1. Upload `movies.csv`
   - `POST /datasets` returns `202` + `task_id`
   - ingestion runs in background with SSE progress

2. Download full dataset as gzipped CSV
   - `GET /datasets/download` returns a gzip stream for fast exports
   - if export prep exceeds ~2s, returns `202` + `task_id`
   - completed async export is fetched from `GET /tasks/{task_id}/download`

3. Query by date range and genre
   - `GET /movies?start_year=&end_year=&genre=`
   - supports sorting and pagination

4. Progress updates for long requests (>2s)
   - `GET /tasks/{task_id}/events` provides SSE status/progress
   - upload always task-based
   - `/movies` auto-hands off to task mode when query exceeds threshold
   - `/datasets/download` auto-hands off to task mode when export prep exceeds threshold

## Architecture (Implemented)

- FastAPI app with lifespan startup/shutdown
- DuckDB on-disk database (`movies.duckdb` by default; override with `MOVIES_DB_PATH`)
- Per-operation DuckDB connections (no shared global connection object)
- In-memory task registry with lock-protected updates
- Task pruning by TTL and max-count
- Temp artifact cleanup for export tasks (on task removal and shutdown)
- Frontend dashboard served at `/` for upload/query/download workflows

## Execution Steps Completed

1. Implemented streaming upload + atomic staging-table swap ingestion
2. Added SSE endpoint for task progress updates
3. Implemented query filtering/sorting/pagination with parameterized SQL
4. Added auto async handoff for long queries (`/movies` -> `202` task)
5. Added explicit async query endpoint (`POST /movies/query`) + results endpoint
6. Added gzip export with auto async handoff for long export prep
7. Added async export artifact endpoint (`GET /tasks/{task_id}/download`)
8. Added task pruning and cleanup hooks
9. Added tests for query/download handoff, task endpoints, and DB connection safety
10. Updated docs to match actual API behavior and runtime config

## Verification

```bash
uv sync
uv run pytest -q
make bench
uv run fastapi dev main.py
```

## Reviewer Aids

- `README.md` — setup, run commands, endpoint behavior
- `DESIGN.md` — design choices and tradeoffs
- `frontend/index.html` — interactive demo surface
