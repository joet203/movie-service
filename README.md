# Movie Database Service (Submission Version)

Time-boxed implementation for the assessment requirements.

## Requirements Coverage

- Upload `movies.csv`: `POST /datasets`
- Download full dataset as gzipped CSV: `GET /datasets/download`
- Query by date range and genre: `GET /movies?start_year=&end_year=&genre=`
- Real-time progress updates for long-running operations: `GET /tasks/{task_id}/events` (SSE)

## Setup

```bash
uv sync
```

## Run

```bash
uv run fastapi dev main.py
```

- Swagger: `http://127.0.0.1:8000/docs`

## Test

```bash
uv run pytest -q
```

## API Summary

- `POST /datasets`
  - Always returns `202` with `task_id`
  - Ingestion runs in background; progress over SSE

- `GET /movies`
  - Returns `200` with movie rows when query finishes quickly
  - Returns `202` with `task_id` when query exceeds ~2 seconds
  - Long-query results are available at `GET /tasks/{task_id}/results`

- `GET /datasets/download`
  - Returns `200` with `movies.csv.gz` when export finishes quickly
  - Returns `202` with `task_id` when export preparation exceeds ~2 seconds
  - Long-export artifact is available at `GET /tasks/{task_id}/download`

- `GET /tasks/{task_id}/events`
  - Server-Sent Events stream with `status`, `progress`, and optional `error`

## Query Pagination and Limits

- Query params: `limit` and `offset`
- Default `limit`: `1000`
- Max `limit` is configurable via `MOVIES_MAX_QUERY_LIMIT` (default `50000`)

Example:

```bash
MOVIES_MAX_QUERY_LIMIT=200000 uv run fastapi dev main.py
```

## Quick Verify

```bash
# Upload dataset
curl -X POST -F "file=@movies.csv" http://127.0.0.1:8000/datasets

# Query by year range + genre
curl "http://127.0.0.1:8000/movies?start_year=2020&end_year=2023&genre=Action"

# Download gzipped dataset
curl -o movies.csv.gz http://127.0.0.1:8000/datasets/download
```

## Included Docs

- `DESIGN.md` — design decisions and trade-offs
- `PERFORMANCE.md` — CPU/memory efficiency notes
- `plan.md` — AI workfile

## Note on Extended Version

The extended version (separate zip) adds a frontend GUI at `/` and richer progress visualization across upload/query/download flows to make interactive testing and review easier.
