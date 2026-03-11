# Movie Database Service

## Quick Reference

- **Run**: `make run` (or `uv run fastapi dev main.py`)
- **Test**: `make test` (or `uv run pytest tests/ -v`)
- **Bench**: `make bench` (requires `movies.csv` in project root)
- **Swagger**: `localhost:8000/docs`
- **Frontend**: `localhost:8000`

## Stack

- Python 3.13, FastAPI, DuckDB (embedded OLAP), Pydantic
- No pandas/Polars — stdlib `csv` for header validation, DuckDB native `read_csv` for ingestion
- 2 runtime deps: `duckdb`, `python-multipart`

## Project Structure

```
main.py              FastAPI app, lifespan, CORS, serves frontend
app/
  db.py              DuckDB connection singleton + task state dict
  model.py           Pydantic response models (Movie, TaskResponse, TaskStatus)
  movies.py          All route handlers + background ingestion
frontend/
  index.html         Single-page dashboard UI
tests/
  conftest.py        Test fixtures (in-memory DuckDB override)
  test_movies.py     15 tests across all endpoints
DESIGN.md            Architecture and design decisions
INTERVIEW.md         Interview prep Q&A
benchmark.py         Performance benchmark script
Makefile             Dev workflow (run, test, bench, clean)
```

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/datasets` | Upload CSV → 202 + task_id |
| `GET` | `/tasks/{id}/events` | SSE progress stream |
| `GET` | `/movies` | Query with filters, sorting, pagination |
| `GET` | `/datasets/download` | Download gzipped CSV |
| `GET` | `/health` | Health check |
| `GET` | `/sample-data` | Download sample movies.csv |

### Query params for `/movies`

`start_year`, `end_year`, `genre` (ILIKE), `sort_by`, `sort_order` (asc/desc), `limit` (default 1000, max 10000), `offset`. Returns `X-Total-Count` header.

## Key Design Decisions

- DuckDB native `read_csv` with `TRY_CAST` for ingestion (170x faster than Python csv.reader)
- Staging table + atomic swap for safe data replacement
- SSE for progress updates (unidirectional, simpler than WebSockets)
- Streaming everywhere: upload (1MB chunks), download (64KB chunks via DuckDB gzip export)
- 10 GB upload limit, LIKE wildcard escaping, error sanitization

## Branches

- `master` — full version with frontend dashboard
- `submission` — barebones version (no frontend, same API)
