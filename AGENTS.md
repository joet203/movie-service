# Movie Database Service - Agent Context

## Assignment Requirements

Build a FastAPI movie database service with:

1. CSV upload endpoint for `movies.csv`
2. Full dataset download as gzipped CSV
3. Query endpoint by year range and genre
4. Real-time progress updates for requests that run longer than 2 seconds

## Implemented API Surface

- `POST /datasets`
  - Uploads CSV as a background task
  - Returns `202` + `task_id`
- `GET /tasks/{task_id}/events`
  - SSE stream for task status/progress (`pending`, `processing`, `completed`, `error`)
- `GET /movies`
  - Query filters: `start_year`, `end_year`, `genre`
  - Optional: `sort_by`, `sort_order`, `limit`, `offset`
  - Returns `200` with rows for fast requests
  - Returns `202` + `task_id` when query exceeds ~2 seconds
- `GET /tasks/{task_id}/results`
  - Returns rows for completed long-running query task
- `GET /datasets/download`
  - Returns `200` gzip stream for fast exports
  - Returns `202` + `task_id` when export exceeds ~2 seconds
- `GET /tasks/{task_id}/download`
  - Returns gzip artifact for completed long-running export task

## Current Project Structure

```
main.py            # FastAPI app + lifespan startup/shutdown
app/
  db.py            # DuckDB path/config, per-operation connections, task registry
  model.py         # Pydantic response models
  movies.py        # Upload/query/download handlers + background task logic
tests/
  conftest.py      # Isolated test DB fixture
  test_movies.py   # Endpoint behavior tests
  test_db.py       # DB connection safety tests
requirements.md    # Original assessment requirements
README.md          # Run instructions + endpoint summary
DESIGN.md          # Architecture and trade-offs
plan.md            # Implementation plan
```

## Runtime Notes

- Python: `>=3.13,<3.14`
- Dependency manager: `uv`
- Run app: `uv run fastapi dev main.py`
- Run tests: `uv run pytest -q`

## Delivery Notes

- Keep implementation readable and dependency-light.
- Prefer deterministic progress phases over artificial delays.
- Keep task memory bounded and clean temporary artifacts on completion/shutdown.
