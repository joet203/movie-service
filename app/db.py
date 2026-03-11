from __future__ import annotations

import os
import threading
import time
from typing import Any

import duckdb

_db_path: str | None = None
_tasks_lock = threading.Lock()

# In-memory task state: task_id -> {"status", "progress", "error", ...}
tasks: dict[str, dict[str, Any]] = {}

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS movies (
    movie_name VARCHAR,
    year INTEGER,
    genres VARCHAR,
    rating DOUBLE
)
"""


def init_db(path: str | None = None) -> None:
    global _db_path
    if path is None:
        path = os.getenv("MOVIES_DB_PATH", "movies.duckdb")
    _db_path = path
    with duckdb.connect(path) as conn:
        conn.execute(TABLE_DDL)


def get_db() -> duckdb.DuckDBPyConnection:
    if _db_path is None:
        raise RuntimeError("Database not initialized — call init_db() first")
    return duckdb.connect(_db_path)


def close_db() -> None:
    global _db_path
    _db_path = None
    clear_tasks()


def clear_tasks() -> None:
    with _tasks_lock:
        tasks.clear()


def create_task(task_id: str) -> None:
    now = time.monotonic()
    with _tasks_lock:
        tasks[task_id] = {
            "status": "pending",
            "progress": 0,
            "error": None,
            "created_at": now,
            "updated_at": now,
        }


def set_task(task_id: str, **updates: Any) -> None:
    with _tasks_lock:
        task = tasks.get(task_id)
        if task is None:
            return
        task.update(updates)
        task["updated_at"] = time.monotonic()


def get_task(task_id: str) -> dict[str, Any] | None:
    with _tasks_lock:
        task = tasks.get(task_id)
        return task.copy() if task is not None else None


def task_exists(task_id: str) -> bool:
    with _tasks_lock:
        return task_id in tasks


def delete_task(task_id: str) -> dict[str, Any] | None:
    with _tasks_lock:
        return tasks.pop(task_id, None)


def list_tasks() -> list[dict[str, Any]]:
    with _tasks_lock:
        return [task.copy() for task in tasks.values()]


def prune_tasks(max_tasks: int, ttl_seconds: float) -> list[dict[str, Any]]:
    now = time.monotonic()
    removed: list[dict[str, Any]] = []
    with _tasks_lock:
        expired = [
            task_id
            for task_id, task in tasks.items()
            if task.get("status") in {"completed", "error"}
            and now - float(task.get("updated_at", now)) > ttl_seconds
        ]
        for task_id in expired:
            task = tasks.pop(task_id, None)
            if task is not None:
                removed.append(task)

        if len(tasks) <= max_tasks:
            return removed

        done_items = sorted(
            (
                (task_id, task)
                for task_id, task in tasks.items()
                if task.get("status") in {"completed", "error"}
            ),
            key=lambda item: float(item[1].get("updated_at", 0.0)),
        )
        while len(tasks) > max_tasks and done_items:
            task_id, _ = done_items.pop(0)
            task = tasks.pop(task_id, None)
            if task is not None:
                removed.append(task)
    return removed
