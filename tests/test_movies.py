import gzip
import io
import json

from app import db


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
