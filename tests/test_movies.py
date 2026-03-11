import gzip
import io
import json
import time

from app import db
from app import movies


class TestHealth:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


class TestUpload:
    def test_upload_returns_202(self, client, csv_file):
        resp = client.post(
            "/datasets",
            files={"file": ("movies.csv", io.BytesIO(csv_file), "text/csv")},
        )
        assert resp.status_code == 202
        assert "task_id" in resp.json()

    def test_upload_completes(self, client, csv_file):
        resp = client.post(
            "/datasets",
            files={"file": ("movies.csv", io.BytesIO(csv_file), "text/csv")},
        )
        task_id = resp.json()["task_id"]
        assert db.tasks[task_id]["status"] == "completed"
        assert db.tasks[task_id]["progress"] == 100

    def test_upload_and_sse(self, client, csv_file):
        resp = client.post(
            "/datasets",
            files={"file": ("movies.csv", io.BytesIO(csv_file), "text/csv")},
        )
        task_id = resp.json()["task_id"]

        # Task already completed (TestClient runs background synchronously)
        resp = client.get(f"/tasks/{task_id}/events")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")

        events = []
        for line in resp.text.strip().split("\n"):
            if line.startswith("data: "):
                events.append(json.loads(line[6:]))

        assert events[-1]["status"] == "completed"
        assert events[-1]["progress"] == 100

    def test_upload_bad_headers(self, client):
        bad_csv = b"name,date,type,score\nFoo,2020,Action,5.0\n"
        resp = client.post(
            "/datasets",
            files={"file": ("bad.csv", io.BytesIO(bad_csv), "text/csv")},
        )
        task_id = resp.json()["task_id"]
        assert db.tasks[task_id]["status"] == "error"
        assert "headers" in db.tasks[task_id]["error"].lower()

    def test_upload_invalid_file(self, client):
        garbage = b"not,a,valid\n\x00\x01\x02\x03"
        resp = client.post(
            "/datasets",
            files={"file": ("garbage.csv", io.BytesIO(garbage), "text/csv")},
        )
        task_id = resp.json()["task_id"]
        assert db.tasks[task_id]["status"] == "error"


class TestSSE:
    def test_task_not_found(self, client):
        resp = client.get("/tasks/nonexistent-id/events")
        assert resp.status_code == 404


class TestQuery:
    def test_query_no_filters(self, client, populated_db):
        resp = client.get("/movies")
        assert resp.status_code == 200
        movies = resp.json()
        assert len(movies) == 6

    def test_query_by_year_range(self, client, populated_db):
        resp = client.get("/movies", params={"start_year": 2000, "end_year": 2023})
        assert resp.status_code == 200
        movies = resp.json()
        assert len(movies) > 0
        for m in movies:
            assert m["year"] is not None
            assert 2000 <= m["year"] <= 2023

    def test_query_by_genre(self, client, populated_db):
        resp = client.get("/movies", params={"genre": "Action"})
        assert resp.status_code == 200
        movies = resp.json()
        assert len(movies) > 0
        for m in movies:
            assert "Action" in m["genres"]

    def test_query_combined(self, client, populated_db):
        resp = client.get(
            "/movies", params={"start_year": 2020, "end_year": 2023, "genre": "Comedy"}
        )
        assert resp.status_code == 200
        movies = resp.json()
        for m in movies:
            assert m["year"] is not None
            assert 2020 <= m["year"] <= 2023
            assert "Comedy" in m["genres"]

    def test_query_empty_result(self, client, populated_db):
        resp = client.get(
            "/movies", params={"start_year": 1800, "end_year": 1850}
        )
        assert resp.status_code == 200
        assert resp.json() == []

    def test_query_invalid_year_range(self, client, populated_db):
        resp = client.get(
            "/movies", params={"start_year": 2023, "end_year": 2000}
        )
        assert resp.status_code == 400

    def test_query_auto_async_when_slow(self, client, populated_db, monkeypatch):
        original_run_query = movies._run_query

        def slow_run_query(*args, **kwargs):
            time.sleep(0.05)
            return original_run_query(*args, **kwargs)

        monkeypatch.setattr(movies, "_run_query", slow_run_query)
        monkeypatch.setattr(movies, "SSE_QUERY_THRESHOLD", 0.001)

        resp = client.get("/movies", params={"genre": "Action"})
        assert resp.status_code == 202
        task_id = resp.json()["task_id"]

        # Poll for completion
        for _ in range(20):
            rr = client.get(f"/tasks/{task_id}/results")
            if rr.status_code == 200:
                break
            assert rr.status_code == 409
            time.sleep(0.01)

        assert rr.status_code == 200
        movies_result = rr.json()
        assert len(movies_result) > 0
        for m in movies_result:
            assert "Action" in m["genres"]


class TestDownload:
    def test_download(self, client, populated_db):
        resp = client.get("/datasets/download")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/gzip"
        assert "movies.csv.gz" in resp.headers.get("content-disposition", "")

        csv_text = gzip.decompress(resp.content).decode("utf-8")
        lines = csv_text.strip().split("\n")
        assert lines[0] == "movie_name,year,genres,rating"
        assert len(lines) == 7  # header + 6 data rows

    def test_download_empty_db(self, client):
        resp = client.get("/datasets/download")
        assert resp.status_code == 404


class TestTasks:
    def test_task_results_not_found(self, client):
        resp = client.get("/tasks/fake-id/results")
        assert resp.status_code == 404

    def test_task_pruning_keeps_registry_bounded(
        self, client, populated_db, csv_file, monkeypatch
    ):
        monkeypatch.setattr(movies, "TASK_MAX_COUNT", 2)
        monkeypatch.setattr(movies, "TASK_TTL_SECONDS", 9999.0)

        task_ids = []
        for _ in range(3):
            resp = client.post(
                "/datasets",
                files={"file": ("movies.csv", io.BytesIO(csv_file), "text/csv")},
            )
            assert resp.status_code == 202
            task_ids.append(resp.json()["task_id"])

        assert task_ids[0] not in db.tasks
        assert task_ids[1] in db.tasks
        assert task_ids[2] in db.tasks
