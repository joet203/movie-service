import io

import pytest
from fastapi.testclient import TestClient

from app import db
from main import app

TEST_CSV = """\
movie_name,year,genres,rating
The Godfather,1972,"Crime, Drama",9.2
Pulp Fiction,1994,"Crime, Drama",8.9
The Dark Knight,2008,"Action, Crime, Drama",9.0
Glass Onion,2022,"Comedy, Crime, Drama",7.2
Fast X,2023,"Action, Crime, Mystery",
Killers of the Flower Moon,,"Crime, Drama, History",
"""


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """TestClient with isolated DuckDB file per test."""
    db_path = tmp_path / "test.duckdb"
    monkeypatch.setenv("MOVIES_DB_PATH", str(db_path))
    db.clear_tasks()
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c
    db.clear_tasks()


@pytest.fixture()
def csv_file():
    """Return test CSV content as bytes for upload."""
    return TEST_CSV.encode("utf-8")


@pytest.fixture()
def populated_db(client, csv_file):
    """Upload test CSV — BackgroundTasks runs synchronously in TestClient."""
    resp = client.post(
        "/datasets", files={"file": ("movies.csv", io.BytesIO(csv_file), "text/csv")}
    )
    task_id = resp.json()["task_id"]
    assert db.tasks[task_id]["status"] == "completed", (
        f"Ingestion failed: {db.tasks[task_id].get('error')}"
    )
    return task_id
