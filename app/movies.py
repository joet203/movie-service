from __future__ import annotations

import asyncio
import csv
import logging
import os
import tempfile
import time
from typing import Callable, Literal
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Response, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from app import db
from app.model import HealthResponse, Movie, TaskResponse, TaskStatus

logger = logging.getLogger(__name__)

router = APIRouter()

EXPECTED_HEADERS = ["movie_name", "year", "genres", "rating"]
CHUNK_SIZE = 1024 * 1024  # 1 MB for upload streaming
DOWNLOAD_CHUNK = 65_536  # 64 KB for download streaming
MAX_UPLOAD_BYTES = 10_000_000_000  # 10 GB upload limit
DEFAULT_QUERY_LIMIT = 1000
DEFAULT_MAX_QUERY_LIMIT = 50_000
MAX_QUERY_LIMIT_ENV = "MOVIES_MAX_QUERY_LIMIT"
SSE_QUERY_THRESHOLD = 2.0  # seconds
SSE_DOWNLOAD_THRESHOLD = 2.0  # seconds
TASK_TTL_SECONDS = 15 * 60
TASK_MAX_COUNT = 500
TERMINAL_STATES = {"completed", "error"}
VALID_SORT_COLUMNS = {"movie_name", "year", "genres", "rating"}


def _sanitize_error(e: Exception) -> str:
    """Return a safe error message — expose CSV format issues, hide internals."""
    msg = str(e)
    if (
        "header" in msg.lower()
        or "csv" in msg.lower()
        or "empty" in msg.lower()
        or "no data" in msg.lower()
    ):
        return msg
    return "Operation failed — check input and try again"


def get_max_query_limit() -> int:
    raw = os.getenv(MAX_QUERY_LIMIT_ENV)
    if raw is None:
        return DEFAULT_MAX_QUERY_LIMIT
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "Invalid %s value %r; using default %s",
            MAX_QUERY_LIMIT_ENV,
            raw,
            DEFAULT_MAX_QUERY_LIMIT,
        )
        return DEFAULT_MAX_QUERY_LIMIT
    if value < 1:
        logger.warning(
            "Invalid %s value %r; must be >= 1, using default %s",
            MAX_QUERY_LIMIT_ENV,
            raw,
            DEFAULT_MAX_QUERY_LIMIT,
        )
        return DEFAULT_MAX_QUERY_LIMIT
    return value


def _escape_like(value: str) -> str:
    """Escape SQL LIKE wildcards in user input."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _validate_year_range(start_year: int | None, end_year: int | None) -> None:
    if start_year is not None and end_year is not None and start_year > end_year:
        raise HTTPException(
            status_code=400,
            detail="start_year must be less than or equal to end_year",
        )


def _validate_limit(limit: int) -> None:
    max_limit = get_max_query_limit()
    if limit > max_limit:
        raise HTTPException(
            status_code=422,
            detail=f"limit must be less than or equal to {max_limit}",
        )


def _build_where_clause(
    start_year: int | None,
    end_year: int | None,
    genre: str | None,
) -> tuple[str, list]:
    where = "WHERE 1=1"
    params: list = []

    if start_year is not None:
        where += " AND year >= ?"
        params.append(start_year)
    if end_year is not None:
        where += " AND year <= ?"
        params.append(end_year)
    if genre is not None:
        where += " AND genres ILIKE ? ESCAPE '\\'"
        params.append(f"%{_escape_like(genre)}%")

    return where, params


def _new_task_id() -> str:
    _prune_tasks()
    task_id = str(uuid4())
    db.create_task(task_id)
    _prune_tasks()
    return task_id


def _cleanup_task_artifacts(task: dict | None) -> None:
    if task is None:
        return
    result = task.get("result")
    if not isinstance(result, dict):
        return
    export_path = result.get("export_path")
    if isinstance(export_path, str):
        try:
            os.unlink(export_path)
        except OSError:
            pass


def _prune_tasks() -> None:
    removed = db.prune_tasks(max_tasks=TASK_MAX_COUNT, ttl_seconds=TASK_TTL_SECONDS)
    for task in removed:
        _cleanup_task_artifacts(task)


def cleanup_all_task_artifacts() -> None:
    """Best-effort cleanup for any task-owned temp files (called at shutdown)."""
    for task in list(db.tasks.values()):
        _cleanup_task_artifacts(task)


def _execute_query(
    start_year: int | None,
    end_year: int | None,
    genre: str | None,
    sort_by: str,
    sort_order: Literal["asc", "desc"],
    limit: int,
    offset: int,
    on_progress: Callable[[int], None] | None = None,
) -> dict:
    normalized_sort = sort_by if sort_by in VALID_SORT_COLUMNS else "movie_name"
    direction = "ASC" if sort_order == "asc" else "DESC"
    where, params = _build_where_clause(start_year, end_year, genre)

    conn = db.get_db()
    try:
        total = conn.execute(
            f"SELECT COUNT(*) FROM movies {where}", params
        ).fetchone()[0]
        if on_progress is not None:
            on_progress(25)

        sql = (
            f"SELECT movie_name, year, genres, rating FROM movies {where}"
            f" ORDER BY {normalized_sort} {direction} NULLS LAST"
            f" LIMIT ? OFFSET ?"
        )
        result = conn.execute(sql, params + [limit, offset])
        columns = [desc[0] for desc in result.description]

        t0 = time.monotonic()
        rows = result.fetchall()
        query_time = round(time.monotonic() - t0, 3)
        if on_progress is not None:
            on_progress(85)
    finally:
        conn.close()

    movies = [dict(zip(columns, row)) for row in rows]
    return {"total": total, "movies": movies, "query_time": query_time}


def _run_query(
    task_id: str,
    start_year: int | None,
    end_year: int | None,
    genre: str | None,
    sort_by: str,
    sort_order: Literal["asc", "desc"],
    limit: int,
    offset: int,
) -> None:
    db.set_task(task_id, status="processing", progress=5, error=None)
    try:
        result = _execute_query(
            start_year=start_year,
            end_year=end_year,
            genre=genre,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset,
            on_progress=lambda progress: db.set_task(task_id, progress=progress),
        )
        db.set_task(
            task_id,
            status="completed",
            progress=100,
            result=result,
            error=None,
        )
    except Exception as e:
        logger.exception("Query failed for task %s", task_id)
        db.set_task(task_id, status="error", progress=100, error=_sanitize_error(e))


def _run_export(task_id: str) -> None:
    """Build gzip export in background and attach path to task result."""
    db.set_task(task_id, status="processing", progress=5, error=None)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv.gz")
    tmp_path = tmp.name
    tmp.close()
    conn = db.get_db()

    try:
        count = conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
        if count == 0:
            raise ValueError("No data available for download")

        db.set_task(task_id, progress=25)
        conn.execute(
            f"COPY movies TO '{tmp_path}' (FORMAT CSV, HEADER, COMPRESSION 'gzip')"
        )
        db.set_task(task_id, progress=90)
        db.set_task(
            task_id,
            status="completed",
            progress=100,
            result={"export_path": tmp_path},
            error=None,
        )
    except Exception as e:
        logger.exception("Export failed for task %s", task_id)
        db.set_task(task_id, status="error", progress=100, error=_sanitize_error(e))
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/health")
async def health() -> HealthResponse:
    return HealthResponse(status="ok")


# ---------------------------------------------------------------------------
# Upload / Ingest
# ---------------------------------------------------------------------------


@router.post("/datasets", status_code=202)
async def upload_dataset(
    file: UploadFile, background_tasks: BackgroundTasks
) -> TaskResponse:
    # Stream upload to a temp file — never hold full CSV in memory
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    total_bytes = 0
    try:
        while chunk := await file.read(CHUNK_SIZE):
            total_bytes += len(chunk)
            if total_bytes > MAX_UPLOAD_BYTES:
                tmp.close()
                os.unlink(tmp.name)
                raise HTTPException(
                    status_code=413,
                    detail=f"File exceeds {MAX_UPLOAD_BYTES // 1_000_000_000} GB limit",
                )
            tmp.write(chunk)
    finally:
        tmp.close()

    task_id = _new_task_id()

    # BackgroundTasks runs after the response is sent.
    # Starlette runs sync functions in a thread pool automatically.
    background_tasks.add_task(_ingest_csv, task_id, tmp.name)

    return TaskResponse(task_id=task_id)


def _ingest_csv(task_id: str, temp_path: str) -> None:
    """Synchronous ingestion — runs in a thread via BackgroundTasks."""
    db.set_task(task_id, status="processing", progress=5, error=None)
    conn = db.get_db()

    try:
        # Step 1: validate header (quick first-line read, no full parse)
        with open(temp_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                raise ValueError("CSV file is empty")
            normalized = [h.strip().lower() for h in header]
            if normalized != EXPECTED_HEADERS:
                raise ValueError(
                    f"Invalid CSV headers: expected {EXPECTED_HEADERS}, "
                    f"got {normalized}"
                )

        db.set_task(task_id, progress=25)

        # Safety: temp paths from tempfile should never contain quotes,
        # but assert to prevent SQL injection via path manipulation.
        if "'" in temp_path:
            raise ValueError("Invalid temp file path")

        # Step 2: DuckDB native CSV read — bypasses Python-side parsing entirely.
        # TRY_CAST gracefully handles dirty data (e.g., "III" in year column).
        # strict_mode=false tolerates quoting irregularities in the CSV.
        conn.execute("DROP TABLE IF EXISTS movies_staging")
        conn.execute(
            f"""CREATE TABLE movies_staging AS
            SELECT
                movie_name,
                TRY_CAST(year AS INTEGER) AS year,
                genres,
                TRY_CAST(rating AS DOUBLE) AS rating
            FROM read_csv('{temp_path}',
                columns={{'movie_name': 'VARCHAR', 'year': 'VARCHAR',
                          'genres': 'VARCHAR', 'rating': 'VARCHAR'}},
                header=true, auto_detect=false, strict_mode=false)"""
        )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM movies_staging"
        ).fetchone()[0]
        if row_count == 0:
            raise ValueError("CSV file contains only a header (no data rows)")

        db.set_task(task_id, progress=75)

        # Step 3: atomic swap — readers never see partial data
        conn.execute("BEGIN TRANSACTION")
        conn.execute("DROP TABLE IF EXISTS movies")
        conn.execute("ALTER TABLE movies_staging RENAME TO movies")
        conn.execute("COMMIT")

        db.set_task(task_id, status="completed", progress=100)

    except Exception as e:
        logger.exception("Ingestion failed for task %s", task_id)
        db.set_task(task_id, status="error", progress=100, error=_sanitize_error(e))
        try:
            conn.execute("DROP TABLE IF EXISTS movies_staging")
        except Exception:
            pass
    finally:
        conn.close()
        try:
            os.unlink(temp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# SSE Progress
# ---------------------------------------------------------------------------


@router.get("/tasks/{task_id}/events")
async def task_events(task_id: str) -> StreamingResponse:
    _prune_tasks()
    if not db.task_exists(task_id):
        raise HTTPException(status_code=404, detail="Task not found")
    return StreamingResponse(
        _event_generator(task_id),
        media_type="text/event-stream",
    )


async def _event_generator(task_id: str):
    """Yield SSE events until the task completes or errors."""
    last_progress = -1
    while True:
        task = db.get_task(task_id)
        if task is None:
            break

        status = task["status"]
        progress = task["progress"]

        # Only emit when progress actually changes (or terminal state)
        if progress != last_progress or status in TERMINAL_STATES:
            event = TaskStatus(
                status=status, progress=progress, error=task.get("error")
            )
            yield f"data: {event.model_dump_json()}\n\n"
            last_progress = progress

        if status in TERMINAL_STATES:
            break

        # Non-blocking poll interval — yields control to the event loop
        await asyncio.sleep(0.5)


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


@router.get(
    "/movies",
    responses={
        202: {
            "description": "Query is still running; poll task endpoints for progress/results",
            "model": TaskResponse,
        }
    },
)
async def query_movies(
    response: Response,
    start_year: int | None = None,
    end_year: int | None = None,
    genre: str | None = None,
    sort_by: str = Query(default="movie_name"),
    sort_order: Literal["asc", "desc"] = "asc",
    limit: int = Query(default=DEFAULT_QUERY_LIMIT, ge=1),
    offset: int = Query(default=0, ge=0),
) -> list[Movie]:
    _validate_year_range(start_year, end_year)
    _validate_limit(limit)

    # Run query in background and wait up to 2 seconds.
    # If it exceeds threshold, return task_id and continue via SSE/results endpoints.
    task_id = _new_task_id()
    task = asyncio.create_task(
        asyncio.to_thread(
            _run_query,
            task_id,
            start_year,
            end_year,
            genre,
            sort_by,
            sort_order,
            limit,
            offset,
        )
    )
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=SSE_QUERY_THRESHOLD)
    except asyncio.TimeoutError:
        return JSONResponse(status_code=202, content={"task_id": task_id})

    completed = db.get_task(task_id)
    if completed is None:
        raise HTTPException(status_code=500, detail="Query task missing")
    if completed["status"] == "error":
        _cleanup_task_artifacts(db.delete_task(task_id))
        raise HTTPException(status_code=500, detail=completed.get("error"))

    result = completed.get("result")
    if result is None:
        _cleanup_task_artifacts(db.delete_task(task_id))
        raise HTTPException(status_code=500, detail="No query result available")

    response.headers["X-Total-Count"] = str(result["total"])
    _cleanup_task_artifacts(db.delete_task(task_id))
    return [Movie(**row) for row in result["movies"]]


@router.get("/tasks/{task_id}/results")
def get_task_results(task_id: str, response: Response):
    _prune_tasks()
    task = db.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    if task["status"] != "completed":
        raise HTTPException(
            status_code=409,
            detail=f"Task is {task['status']}, not completed",
        )

    result = task.get("result")
    if result is None:
        raise HTTPException(status_code=404, detail="No results available")

    response.headers["X-Total-Count"] = str(result["total"])
    return result["movies"]


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


@router.get(
    "/datasets/download",
    responses={
        200: {
            "description": "Gzipped CSV file",
            "content": {"application/gzip": {}},
        },
        202: {
            "description": "Export is still running; poll task endpoints for progress/download",
            "model": TaskResponse,
        },
    },
)
async def download_dataset() -> StreamingResponse:
    # Same threshold pattern as /movies:
    # return direct payload for quick exports, task flow for slow exports.
    task_id = _new_task_id()
    task = asyncio.create_task(asyncio.to_thread(_run_export, task_id))
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=SSE_DOWNLOAD_THRESHOLD)
    except asyncio.TimeoutError:
        return JSONResponse(status_code=202, content={"task_id": task_id})

    completed = db.get_task(task_id)
    if completed is None:
        raise HTTPException(status_code=500, detail="Export task missing")
    if completed["status"] == "error":
        detail = completed.get("error") or "Export failed"
        _cleanup_task_artifacts(db.delete_task(task_id))
        if detail == "No data available for download":
            raise HTTPException(status_code=404, detail=detail)
        raise HTTPException(status_code=500, detail=detail)

    result = completed.get("result")
    if not isinstance(result, dict) or "export_path" not in result:
        _cleanup_task_artifacts(db.delete_task(task_id))
        raise HTTPException(status_code=500, detail="Export artifact missing")

    return StreamingResponse(
        _file_streamer(result["export_path"], task_id=task_id),
        media_type="application/gzip",
        headers={"Content-Disposition": 'attachment; filename="movies.csv.gz"'},
    )


@router.get("/tasks/{task_id}/download")
def download_task_file(task_id: str) -> StreamingResponse:
    _prune_tasks()
    task = db.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    if task["status"] != "completed":
        raise HTTPException(
            status_code=409,
            detail=f"Task is {task['status']}, not completed",
        )

    result = task.get("result")
    if not isinstance(result, dict) or "export_path" not in result:
        raise HTTPException(status_code=404, detail="No download artifact available")

    return StreamingResponse(
        _file_streamer(result["export_path"], task_id=task_id),
        media_type="application/gzip",
        headers={"Content-Disposition": 'attachment; filename="movies.csv.gz"'},
    )


def _file_streamer(path: str, task_id: str | None = None):
    """Yield file in chunks, then delete it."""
    try:
        with open(path, "rb") as f:
            while chunk := f.read(DOWNLOAD_CHUNK):
                yield chunk
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
        if task_id is not None:
            _cleanup_task_artifacts(db.delete_task(task_id))
