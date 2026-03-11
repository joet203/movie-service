# Plan: Movie Database Service — FastAPI + DuckDB (stdlib only)

## Context

Assessment for a Senior SWE role. Build a FastAPI movie database service that handles large datasets with minimal CPU/memory overhead. The provided `movies.csv` has ~367K rows (16MB) but the design must scale to much larger datasets.

Data shape: `movie_name VARCHAR, year INTEGER (nullable), genres VARCHAR, rating DOUBLE (nullable)`. Genres are comma-separated strings. "Date range" is implemented as `start_year`/`end_year` since data only has year.

**Design philosophy:** Senior-level restraint — no dependency bloat. Standard library + DuckDB's native executemany API handles everything. No artificial delays or sleeps anywhere in the codebase.

---

## Tech Stack

- **FastAPI** + **Pydantic** (provided template)
- **DuckDB** — persistent on-disk analytical DB, out-of-core processing, native executemany for bulk insert
- **Python stdlib** — `csv.reader` for parsing, `tempfile` for disk streaming, `asyncio.to_thread` for offloading
- **Python 3.13** (pin `>=3.13,<3.14` — pydantic-core doesn't support 3.14 yet)

```bash
uv add duckdb python-multipart
```

---

## Project Structure

```
main.py                  # FastAPI app, lifespan, includes router
app/
  __init__.py            # (empty)
  db.py                  # DuckDB connection singleton + task state dict
  model.py               # Pydantic models (request/response)
  movies.py              # All route handlers + background ingestion
tests/
  __init__.py            # (empty)
  conftest.py            # pytest fixtures: in-memory DuckDB override, test client
  test_movies.py         # Full test suite for all endpoints
DESIGN.md                # Design choices explanation
```

---

## File-by-File Plan

### 1. `pyproject.toml` — Update

- Change `requires-python` to `">=3.13,<3.14"`
- Dependencies: `fastapi[standard]`, `pydantic`, `duckdb`, `python-multipart`
- Dev dependencies: `pytest`, `httpx`

### 2. `main.py` — App Entry Point

- `asynccontextmanager` lifespan:
  - Call `db.init_db()` to create persistent DuckDB connection + movies table
  - On shutdown: call `db.close_db()`
- Create `FastAPI(lifespan=lifespan)`
- Include `movies_router`

### 3. `app/db.py` — Database & Task State

**DuckDB connection (module-level singleton):**
- `_conn: duckdb.DuckDBConnection | None = None`
- `init_db()` — connects to `movies.duckdb`, runs `CREATE TABLE IF NOT EXISTS movies (movie_name VARCHAR, year INTEGER, genres VARCHAR, rating DOUBLE)`
- `get_db() -> duckdb.DuckDBConnection` — returns `_conn`, raises RuntimeError if not initialized
- `close_db()` — closes connection, sets `_conn = None`

**Task state (module-level dict):**
- `tasks: dict[str, dict] = {}` — maps `task_id` → `{"status": "pending"|"processing"|"completed"|"error", "progress": int (0-100), "error": str|None}`
- Simple dict is fine — single-process FastAPI, no need for Redis/external store

### 4. `app/model.py` — Pydantic Models

**Response models:**
- `Movie(BaseModel)`: `movie_name: str, year: int | None, genres: str, rating: float | None`
- `TaskResponse(BaseModel)`: `task_id: str`
- `TaskStatus(BaseModel)`: `status: str, progress: int, error: str | None = None`
- `HealthResponse(BaseModel)`: `status: str`

### 5. `app/movies.py` — Route Handlers

#### `GET /health`
- Return `{"status": "ok"}`

#### `POST /datasets` → `TaskResponse` (202)
1. Accept `file: UploadFile`
2. **Stream to temp file** — read `UploadFile` in 1MB chunks, write to `tempfile.NamedTemporaryFile(delete=False, suffix='.csv')`. Never hold full file in memory.
3. Generate `task_id = str(uuid4())`
4. Store in `tasks[task_id]` with status `"pending"`, progress `0`, error `None`
5. Kick off background work: use `asyncio.to_thread(_ingest_csv, task_id, temp_path)` wrapped in a `BackgroundTasks` async wrapper, or directly schedule via `asyncio.get_event_loop().run_in_executor`.
6. Return `TaskResponse(task_id=task_id)` with `status_code=202`

#### Background: `_ingest_csv(task_id: str, temp_path: str)` — runs in thread via `asyncio.to_thread`
This is a **synchronous** function executed off the event loop. No sleeps, no artificial delays.

1. **Set status** → `"processing"`

2. **Step 1 — Count rows (fast pass):**
   - Open `temp_path`, iterate lines with `for _ in f`, count them, subtract 1 for header.
   - This is a fast sequential read — no CSV parsing, just `\n` counting.
   - Store as `total_rows`.

3. **Step 2 — Atomic table replace via staging:**
   - `DROP TABLE IF EXISTS movies_staging`
   - `CREATE TABLE movies_staging (movie_name VARCHAR, year INTEGER, genres VARCHAR, rating DOUBLE)`
   - Ingest all rows into `movies_staging` (steps 3-5 below target this table)
   - After ingestion completes: `BEGIN TRANSACTION; DROP TABLE IF EXISTS movies; ALTER TABLE movies_staging RENAME TO movies; COMMIT;`
   - This guarantees queries against `movies` never see partial data — the swap is atomic.

4. **Step 3 — Parse and batch insert with `csv.reader` + `executemany`:**
   - Open `temp_path` with `csv.reader(f)`
   - Skip header row (`next(reader)`)
   - **Validate header** matches expected columns `[movie_name, year, genres, rating]`; if not, set task to error and abort
   - Accumulate parsed rows into a batch list
   - For each row:
     - Skip rows with wrong column count
     - Parse `year` as int (or None), `rating` as float (or None) — skip malformed values
     - Append tuple `(movie_name, year, genres, rating)` to batch

5. **Step 4 — Progress updates every 50,000 rows:**
   - Every 50,000 rows: `conn.executemany("INSERT INTO movies_staging VALUES (?, ?, ?, ?)", batch)`, clear batch, update progress
   - Flush remaining rows after loop ends
   - No sleeps — progress updates happen naturally at the speed of ingestion

6. **Step 5 — Finalize:**
   - Execute atomic swap: `BEGIN TRANSACTION; DROP TABLE IF EXISTS movies; ALTER TABLE movies_staging RENAME TO movies; COMMIT;`
   - `os.unlink(temp_path)` — clean up temp file
   - Set progress → 100, status → `"completed"`

8. **Error handling:**
   - Wrap entire function in `try/except Exception`
   - On failure: `DROP TABLE IF EXISTS movies_staging` (clean up partial state), status → `"error"`, error → `str(e)`
   - `finally`: ensure `os.unlink(temp_path)` runs regardless

#### `GET /tasks/{task_id}/events` → SSE StreamingResponse
1. Look up `task_id` in `tasks` dict. If not found → `HTTPException(404)`
2. Return `StreamingResponse(event_generator(), media_type="text/event-stream")`
3. `event_generator()`: async generator that:
   - Yields `data: {"status": ..., "progress": ..., "error": ...}\n\n`
   - Polls the `tasks` dict — yields whenever progress has changed since last yield
   - Uses `asyncio.sleep(0.5)` **only** as a non-blocking poll interval (this is NOT an artificial delay — it's the standard SSE polling pattern to avoid busy-waiting; the async sleep yields control back to the event loop)
   - Stops when status is `"completed"` or `"error"` (yields final event then breaks)

#### `GET /movies` → `list[Movie]`
1. Optional query params: `start_year: int | None = None`, `end_year: int | None = None`, `genre: str | None = None`
2. **Validate**: if both `start_year` and `end_year` provided, check `start_year <= end_year` → `HTTPException(400)` if not
3. Build SQL dynamically:
   ```sql
   SELECT movie_name, year, genres, rating FROM movies WHERE 1=1
   ```
   - If `start_year`: append `AND year >= ?` with param
   - If `end_year`: append `AND year <= ?` with param
   - If `genre`: append `AND genres LIKE ?` with param `f"%{genre}%"` (substring match)
3. Use parameterized queries (no SQL injection)
4. `conn.execute(sql, params).fetchall()` → convert to list of `Movie` dicts using column names
5. Return the list

#### `GET /datasets/download` → StreamingResponse (gzipped CSV)
1. Check table has data: `SELECT COUNT(*) FROM movies`. If 0 → `HTTPException(404, "No data")`
2. Write to temp file: `COPY movies TO '<temp_path>.csv.gz' (FORMAT CSV, HEADER, COMPRESSION 'gzip')` — DuckDB handles gzip compression natively and efficiently
3. Stream the `.csv.gz` file in 64KB chunks via a generator passed to `StreamingResponse`
4. Headers:
   - `media_type="application/gzip"`
   - `Content-Disposition: attachment; filename="movies.csv.gz"`
5. Generator cleans up temp file after yielding all chunks (delete in `finally` block)

### 6. `tests/conftest.py` — Test Fixtures

- `@pytest.fixture` that:
  - Patches `db._conn` with `duckdb.connect(":memory:")`
  - Creates the movies table in memory
  - Provides `TestClient(app)`
  - Tears down after test (closes in-memory connection)
- Small test CSV fixture as a `StringIO` or temp file (5-10 rows with edge cases: missing year, missing rating, multiple genres)

### 7. `tests/test_movies.py` — Full Test Suite

- **test_health**: GET /health → 200 + `{"status": "ok"}`
- **test_upload_csv**: POST /datasets with test CSV → 202 + task_id returned
- **test_upload_and_poll**: Upload, then poll SSE until completed, verify progress reaches 100%
- **test_query_no_filters**: GET /movies returns all uploaded rows
- **test_query_by_year_range**: GET /movies?start_year=2020&end_year=2023 → only matching years
- **test_query_by_genre**: GET /movies?genre=Action → only rows containing "Action" in genres
- **test_query_combined**: year range + genre filter together
- **test_query_empty_result**: filters that match nothing → empty list `[]`
- **test_download**: GET /datasets/download → decompress gzip, verify CSV content matches
- **test_task_not_found**: GET /tasks/bad-id/events → 404
- **test_upload_invalid_file**: Upload malformed content → error state in task
- **test_query_invalid_year_range**: start_year > end_year → 400
- **test_upload_bad_headers**: CSV with wrong column names → error state in task

### 8. `DESIGN.md` — Design Choices

Brief document covering:
- Why DuckDB (out-of-core analytical engine, zero-config, executemany for O(1) memory bulk insert)
- Why stdlib-only for CSV parsing (no dependency bloat, `csv.reader` handles quoted fields correctly, senior-level restraint)
- Memory management: streaming upload (1MB chunks), executemany (no SQL parsing overhead), streaming download (DuckDB native gzip + chunked file read)
- SSE design: dict-based task state, real progress from row counts, no artificial delays
- Atomic data replacement via staging table + rename (no partial state visible to queries)
- Two-pass ingestion trade-off: counting lines first doubles file I/O but enables real progress tracking — acceptable because the count pass is a fast sequential scan (no parsing) and OS page cache makes the second pass essentially free
- Trade-offs: single-process task state (no horizontal scaling), task history not persisted

---

## Dependency Commands

```bash
# Pin Python version in pyproject.toml: requires-python = ">=3.13,<3.14"

uv add duckdb python-multipart
uv add --dev pytest httpx
```

---

## Submission Checklist

- [ ] Working FastAPI application
- [ ] `DESIGN.md` with design choices explanation
- [ ] `plan.md` (AI workfile — this plan)
- [ ] Claude Code conversation/session logs (AI workfiles)
- [ ] Build/run instructions in README
- [ ] Package as zipfile for recruiter

---

## Verification Steps

1. `uv sync` — install all deps
2. `uv run fastapi dev main.py` — start dev server
3. Upload: `curl -X POST -F "file=@movies.csv" http://localhost:8000/datasets`
4. Poll SSE: `curl -N http://localhost:8000/tasks/{task_id}/events`
5. Query: `curl "http://localhost:8000/movies?start_year=2020&end_year=2023&genre=Action"`
6. Download: `curl -o movies.csv.gz http://localhost:8000/datasets/download`
7. Verify gzip: `gunzip -t movies.csv.gz && zcat movies.csv.gz | head`
8. Tests: `uv run pytest tests/ -v`
