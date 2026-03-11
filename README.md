# Create a Movie Database Service

### Problem Statement

(Expected time commitment: 2-ish hours)

Use AI to build a FastAPI service that serves as a simple movie database, with the following requirements:

- Input a movie database CSV from the file `movies.csv`.
- Support downloading the whole database as a gzipped CSV file.
- Support query requests by date range and genre.
- Provide progress updates for long running requests > 2 seconds

Your assignment will be graded on meeting the above requirements, overall performance and responsiveness of your service, efficient management of CPU and memory, and graceful error handling. Outside of the above requirements, you have flexibility in implementation details. Make reasonable design choices and be prepared to explain them.

The included `movies.csv` is small by modern standards, for testing and distribution, but design your solution as if it needed to handle much larger datasets.

Please return your solution as a zipfile to the same email (i.e. recruiter) that sent it to you.

### API Surface

You should implement endpoints for:

- Submitting the original datafile
- Downloading the entire dataset
- Query endpoint for at least date ranges and genre(s), returning a list of movies
- Endpoint for real-time updates

### Deliverables

Your submission should include:

- All of your AI tool's workfiles
- A working FastAPI application
- A brief explanation of your design choices
- Instructions on how to build and run your application

# Code Template Instructions

### Installation

Install `uv` and run `uv sync`

### Run

Run the following command at root directory (this will also install dependency)

```bash
uv run fastapi dev main.py
```

### Add dependencies

If you need any other packages, just use `uv` to add them

```bash
uv add some_other_package
```

### Useful info

Go to `localhost:8000/docs` for the `Swagger` UI

---

# Developer Guide

## Quick Start

```bash
# Install dependencies
uv sync

# Start the server (cleans up old DB files automatically)
make run

# Or use a custom port if 8000 is taken
make run PORT=8001
```

Open `http://localhost:8000` for the frontend UI, or `http://localhost:8000/docs` for Swagger.

## Common Commands

```bash
make run        # Clean start (kills old server, removes DB, starts fresh)
make test       # Run all 33 tests
make bench      # Run benchmark suite (starts/stops server automatically)
make stop       # Stop the running server
make clean      # Stop server + delete DuckDB files
make help       # Show all available commands
```

**Without make** (Windows or if `make` isn't installed):

```bash
# Remove old DB files, then start
rm -f movies.duckdb movies.duckdb.wal   # del on Windows
uv run fastapi dev main.py

# Run tests
uv run pytest tests/ -v
```

## Runtime Configuration

- `MOVIES_DB_PATH`: DuckDB file path (default: `movies.duckdb`)
- `MOVIES_MAX_QUERY_LIMIT`: maximum allowed `limit` for `/movies` and `/movies/query` (default: `50000`)
- `MOVIES_TASK_POLL_INTERVAL_SECONDS`: SSE task polling interval for `/tasks/{id}/events` (default: `0.2`)

### Persistence Note

- The service uses an on-disk DuckDB file, so uploaded data persists across restarts by default.
- For a clean demo state, use `make run` (it removes old DB files before starting), or manually delete `movies.duckdb` and `movies.duckdb.wal` before launch.

## Demo / Progress Simulation

- Frontend includes a **Demo Settings** card (`Simulate Slow Operations` + `Phase Delay (ms)`).
- This appends `debug_phase_delay_ms` to upload/query/download requests so you can force task handoff and observe SSE progress bars.
- API also accepts this optional query param directly on:
  - `POST /datasets`
  - `GET /movies`
  - `POST /movies/query`
  - `GET /datasets/download`

## Usage Walkthrough

1. **Start the server**: `make run`
2. **Open the UI**: `http://localhost:8000`
3. **Upload data**: Click the upload area or drag `movies.csv` onto it. A sample CSV download link is provided in the UI.
4. **Query movies**: Use the search form to filter by year range, single-genre text, and/or match-all genre selection (AND)
5. **Download dataset**: Click the download button to export as gzipped CSV

### API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `POST` | `/datasets` | Upload CSV (returns 202 + task ID) |
| `GET` | `/tasks/{id}/events` | SSE progress stream |
| `GET` | `/movies` | Query movies (returns `200` with rows, or `202` + task ID if query exceeds ~2s) |
| `POST` | `/movies/query` | Async query (returns 202 + task ID) |
| `GET` | `/tasks/{id}/results` | Fetch async query results |
| `GET` | `/datasets/download` | Download gzipped CSV (returns `200` directly, or `202` + task ID if export prep exceeds ~2s) |
| `GET` | `/tasks/{id}/download` | Fetch completed async export artifact |
| `GET` | `/` | Frontend UI |
| `GET` | `/sample-data` | Download sample movies.csv |

#### Query Parameters for `/movies`

| Param | Type | Default | Description |
|---|---|---|---|
| `start_year` | int | — | Filter movies from this year |
| `end_year` | int | — | Filter movies up to this year |
| `genre` | string | — | Case-insensitive genre substring match |
| `genres_all` | string | — | Comma-separated genres; all listed genres must be present (AND). Example: `Action,Crime` |
| `sort_by` | string | `movie_name` | Sort column (`movie_name`, `year`, `genres`, `rating`) |
| `sort_order` | string | `asc` | Sort direction (`asc` or `desc`) |
| `limit` | int | 10,000 | Results per page (max from `MOVIES_MAX_QUERY_LIMIT`, default `50,000`) |
| `offset` | int | 0 | Pagination offset |

The response includes an `X-Total-Count` header with the total number of matching rows (before pagination).

### curl Examples

```bash
# Upload
curl -X POST -F "file=@movies.csv" http://localhost:8000/datasets

# Poll progress (replace TASK_ID)
curl -N http://localhost:8000/tasks/TASK_ID/events

# Query (with sorting, pagination, and match-all genres)
curl -v "http://localhost:8000/movies?start_year=2020&end_year=2023&genres_all=Action,Crime&sort_by=rating&sort_order=desc&limit=50"

# Async query (for large result sets — returns task ID, poll via SSE)
curl -X POST "http://localhost:8000/movies/query?genre=Action"
curl -N http://localhost:8000/tasks/TASK_ID/events
curl http://localhost:8000/tasks/TASK_ID/results

# Download
curl -o movies.csv.gz http://localhost:8000/datasets/download

# Slow download path (if /datasets/download returns 202)
curl -N http://localhost:8000/tasks/TASK_ID/events
curl -o movies.csv.gz http://localhost:8000/tasks/TASK_ID/download
```

## Project Structure

```
main.py              Entry point, lifespan, serves frontend
app/
  db.py              DuckDB connection factory + task state
  model.py           Pydantic response models
  movies.py          All route handlers + background ingestion
frontend/
  index.html         Single-page dashboard UI
tests/
  conftest.py        Test fixtures (in-memory DuckDB)
  test_movies.py     33 tests across all endpoints
DESIGN.md            Architecture and design decisions
AI_WORKFILES.md      AI tool workfiles and artifacts summary
benchmark.py         Performance benchmark script
Makefile             Dev workflow shortcuts
```

## Key Design Decisions

See [DESIGN.md](DESIGN.md) for detailed rationale. Summary:

- **DuckDB** for out-of-core OLAP storage (handles datasets larger than RAM)
- **DuckDB native CSV reader** for ingestion (170x faster than Python-side parsing)
- **Staging table + atomic swap** for safe data replacement
- **SSE** for real-time progress updates
- **Streaming responses** for memory-efficient downloads
- **Minimal dependencies** — 2 runtime deps: `duckdb` + `python-multipart`

## AI Workfiles

See [AI_WORKFILES.md](AI_WORKFILES.md) for the included planning/design artifacts.
