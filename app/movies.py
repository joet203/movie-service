from __future__ import annotations

import asyncio
import csv
import json
import os
import tempfile
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from app import db
from app.model import HealthResponse, Movie, TaskResponse, TaskStatus

router = APIRouter()

EXPECTED_HEADERS = ["movie_name", "year", "genres", "rating"]
CHUNK_SIZE = 1024 * 1024  # 1 MB for upload streaming
BATCH_SIZE = 50_000       # rows between progress updates
DOWNLOAD_CHUNK = 65_536   # 64 KB for download streaming


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
    try:
        while chunk := await file.read(CHUNK_SIZE):
            tmp.write(chunk)
    finally:
        tmp.close()

    task_id = str(uuid4())
    db.tasks[task_id] = {"status": "pending", "progress": 0, "error": None}

    # BackgroundTasks runs after the response is sent.
    # Starlette runs sync functions in a thread pool automatically.
    background_tasks.add_task(_ingest_csv, task_id, tmp.name)

    return TaskResponse(task_id=task_id)


def _ingest_csv(task_id: str, temp_path: str) -> None:
    """Synchronous ingestion — runs in a thread via asyncio.to_thread."""
    conn = db.get_db()
    task = db.tasks[task_id]
    task["status"] = "processing"

    try:
        # Step 1: fast line count for progress denominator
        with open(temp_path, "r", encoding="utf-8", errors="replace") as f:
            total_rows = sum(1 for _ in f) - 1  # subtract header
        if total_rows <= 0:
            raise ValueError("CSV file is empty or contains only a header")

        # Step 2: prepare staging table for atomic swap
        conn.execute("DROP TABLE IF EXISTS movies_staging")
        conn.execute(
            "CREATE TABLE movies_staging ("
            "movie_name VARCHAR, year INTEGER, genres VARCHAR, rating DOUBLE)"
        )

        # Step 3: parse + batch insert via executemany
        rows_done = 0
        batch: list[tuple] = []

        with open(temp_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            header = next(reader)

            # Validate header
            normalized = [h.strip().lower() for h in header]
            if normalized != EXPECTED_HEADERS:
                raise ValueError(
                    f"Invalid CSV headers: expected {EXPECTED_HEADERS}, got {normalized}"
                )

            for row in reader:
                if len(row) != 4:
                    continue  # skip malformed rows

                movie_name = row[0]
                try:
                    year = int(row[1]) if row[1].strip() else None
                except ValueError:
                    year = None
                genres = row[2]
                try:
                    rating = float(row[3]) if row[3].strip() else None
                except ValueError:
                    rating = None

                batch.append((movie_name, year, genres, rating))
                rows_done += 1

                if rows_done % BATCH_SIZE == 0:
                    conn.executemany(
                        "INSERT INTO movies_staging VALUES (?, ?, ?, ?)", batch
                    )
                    batch.clear()
                    task["progress"] = int(rows_done / total_rows * 100)

        # Flush remaining rows
        if batch:
            conn.executemany(
                "INSERT INTO movies_staging VALUES (?, ?, ?, ?)", batch
            )

        # Step 4: atomic swap — readers never see partial data
        conn.execute("BEGIN TRANSACTION")
        conn.execute("DROP TABLE IF EXISTS movies")
        conn.execute("ALTER TABLE movies_staging RENAME TO movies")
        conn.execute("COMMIT")

        task["progress"] = 100
        task["status"] = "completed"

    except Exception as e:
        task["status"] = "error"
        task["error"] = str(e)
        try:
            conn.execute("DROP TABLE IF EXISTS movies_staging")
        except Exception:
            pass
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# SSE Progress
# ---------------------------------------------------------------------------

@router.get("/tasks/{task_id}/events")
async def task_events(task_id: str) -> StreamingResponse:
    if task_id not in db.tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    return StreamingResponse(
        _event_generator(task_id),
        media_type="text/event-stream",
    )


async def _event_generator(task_id: str):
    """Yield SSE events until the task completes or errors."""
    last_progress = -1
    while True:
        task = db.tasks[task_id]
        status = task["status"]
        progress = task["progress"]

        # Only emit when progress actually changes (or terminal state)
        if progress != last_progress or status in ("completed", "error"):
            event = TaskStatus(
                status=status, progress=progress, error=task.get("error")
            )
            yield f"data: {event.model_dump_json()}\n\n"
            last_progress = progress

        if status in ("completed", "error"):
            break

        # Non-blocking poll interval — yields control to the event loop
        await asyncio.sleep(0.5)


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

@router.get("/movies")
async def query_movies(
    start_year: int | None = None,
    end_year: int | None = None,
    genre: str | None = None,
) -> list[Movie]:
    if start_year is not None and end_year is not None and start_year > end_year:
        raise HTTPException(
            status_code=400,
            detail="start_year must be less than or equal to end_year",
        )

    conn = db.get_db()
    sql = "SELECT movie_name, year, genres, rating FROM movies WHERE 1=1"
    params: list = []

    if start_year is not None:
        sql += " AND year >= ?"
        params.append(start_year)
    if end_year is not None:
        sql += " AND year <= ?"
        params.append(end_year)
    if genre is not None:
        sql += " AND genres LIKE ?"
        params.append(f"%{genre}%")

    result = conn.execute(sql, params)
    columns = [desc[0] for desc in result.description]
    return [Movie(**dict(zip(columns, row))) for row in result.fetchall()]


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

@router.get("/datasets/download")
async def download_dataset() -> StreamingResponse:
    conn = db.get_db()
    count = conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
    if count == 0:
        raise HTTPException(status_code=404, detail="No data available for download")

    # DuckDB writes gzip-compressed CSV natively — efficient C++ path
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv.gz")
    tmp.close()
    conn.execute(
        f"COPY movies TO '{tmp.name}' (FORMAT CSV, HEADER, COMPRESSION 'gzip')"
    )

    return StreamingResponse(
        _file_streamer(tmp.name),
        media_type="application/gzip",
        headers={"Content-Disposition": 'attachment; filename="movies.csv.gz"'},
    )


def _file_streamer(path: str):
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
