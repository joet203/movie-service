import io

import duckdb
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
def client():
    """TestClient with isolated in-memory database.

    Enter TestClient first (triggers lifespan/init_db), then swap
    the connection to an in-memory DB so tests are fully isolated.
    """
    with TestClient(app, raise_server_exceptions=False) as c:
        # Swap to in-memory DB after lifespan has run
        original = db._conn
        conn = duckdb.connect(":memory:")
        conn.execute(db.TABLE_DDL)
        db._conn = conn
        yield c
        # Restore original so lifespan's close_db() closes the right one
        db._conn = original
        conn.close()


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
