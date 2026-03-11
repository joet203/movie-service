from app import db


def test_get_db_returns_independent_connections(monkeypatch, tmp_path):
    db_path = tmp_path / "conn-test.duckdb"
    monkeypatch.setenv("MOVIES_DB_PATH", str(db_path))

    db.init_db()
    conn1 = db.get_db()
    conn2 = db.get_db()
    try:
        assert conn1 is not conn2
        conn1.execute(
            "INSERT INTO movies (movie_name, year, genres, rating) VALUES (?, ?, ?, ?)",
            ["A", 2020, "Action", 8.0],
        )
        row_count = conn2.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
        assert row_count == 1
    finally:
        conn1.close()
        conn2.close()
        db.close_db()
