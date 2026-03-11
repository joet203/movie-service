from __future__ import annotations

import duckdb

_conn: duckdb.DuckDBConnection | None = None

# In-memory task state: task_id -> {"status", "progress", "error"}
# Safe for single-process FastAPI; GIL protects dict mutations.
tasks: dict[str, dict] = {}

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS movies (
    movie_name VARCHAR,
    year INTEGER,
    genres VARCHAR,
    rating DOUBLE
)
"""


def init_db(path: str = "movies.duckdb") -> None:
    global _conn
    _conn = duckdb.connect(path)
    _conn.execute(TABLE_DDL)


def get_db() -> duckdb.DuckDBConnection:
    if _conn is None:
        raise RuntimeError("Database not initialized — call init_db() first")
    return _conn


def close_db() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None
