"""Benchmark script for the Movie Database Service.

Measures wall-clock time and peak memory for:
  1. CSV ingestion (upload + background processing)
  2. Filtered query (year range + genre)
  3. Full-table query (no filters)
  4. Gzipped CSV download

Usage:
    # Start the server first:
    uv run fastapi dev main.py

    # Then in another terminal:
    uv run python benchmark.py [path_to_csv]
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import httpx

BASE_URL = "http://localhost:8000"
DEFAULT_CSV = "movies.csv"


def server_rss_mb() -> float | None:
    """Get RSS of the FastAPI server process via ps."""
    try:
        out = subprocess.check_output(
            ["lsof", "-ti", ":8000"], text=True
        ).strip()
        pids = out.split("\n")
        max_rss = 0.0
        for pid in pids:
            ps_out = subprocess.check_output(
                ["ps", "-o", "rss=", "-p", pid.strip()], text=True
            ).strip()
            if ps_out:
                max_rss = max(max_rss, int(ps_out) / 1024)
        return max_rss if max_rss > 0 else None
    except Exception:
        return None


def bench_upload(client: httpx.Client, csv_path: Path) -> str:
    size_mb = csv_path.stat().st_size / (1024 * 1024)
    print(f"\n--- Upload & Ingest ({csv_path.name}, {size_mb:.1f} MB) ---")

    start = time.perf_counter()
    with open(csv_path, "rb") as f:
        resp = client.post(
            f"{BASE_URL}/datasets",
            files={"file": (csv_path.name, f, "text/csv")},
            timeout=120,
        )
    upload_elapsed = time.perf_counter() - start
    assert resp.status_code == 202, f"Upload failed: {resp.status_code}"
    task_id = resp.json()["task_id"]
    print(f"  Upload accepted: {upload_elapsed:.2f}s (task {task_id[:8]}...)")

    # Poll SSE until ingestion completes
    start = time.perf_counter()
    with client.stream(
        "GET", f"{BASE_URL}/tasks/{task_id}/events", timeout=300
    ) as stream:
        for line in stream.iter_lines():
            if line.startswith("data: "):
                event = json.loads(line[6:])
                if event["status"] == "completed":
                    break
                if event["status"] == "error":
                    print(f"  ERROR: {event['error']}")
                    sys.exit(1)
    ingest_elapsed = time.perf_counter() - start

    total = upload_elapsed + ingest_elapsed
    print(f"  Ingestion: {ingest_elapsed:.2f}s")
    print(f"  Total upload→complete: {total:.2f}s")
    return task_id


def bench_query_filtered(client: httpx.Client) -> None:
    print("\n--- Query: Action movies 2020-2023 ---")
    start = time.perf_counter()
    resp = client.get(
        f"{BASE_URL}/movies",
        params={"start_year": 2020, "end_year": 2023, "genre": "Action"},
        timeout=30,
    )
    elapsed = time.perf_counter() - start
    assert resp.status_code == 200
    count = len(resp.json())
    print(f"  {count} results in {elapsed:.3f}s")


def bench_query_all(client: httpx.Client) -> None:
    print("\n--- Query: All movies (no filters) ---")
    start = time.perf_counter()
    resp = client.get(f"{BASE_URL}/movies", timeout=60)
    elapsed = time.perf_counter() - start
    assert resp.status_code == 200
    count = len(resp.json())
    print(f"  {count} results in {elapsed:.3f}s")


def bench_download(client: httpx.Client) -> None:
    print("\n--- Download (gzipped CSV) ---")
    start = time.perf_counter()
    resp = client.get(f"{BASE_URL}/datasets/download", timeout=60)
    elapsed = time.perf_counter() - start
    assert resp.status_code == 200
    size_mb = len(resp.content) / (1024 * 1024)
    print(f"  {size_mb:.1f} MB downloaded in {elapsed:.3f}s")


def main():
    csv_path = Path(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CSV)
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    # Verify server is running
    client = httpx.Client()
    try:
        client.get(f"{BASE_URL}/health", timeout=5)
    except httpx.ConnectError:
        print(f"Server not reachable at {BASE_URL}")
        print("Start it first: uv run fastapi dev main.py")
        sys.exit(1)

    server_mem_before = server_rss_mb()

    bench_upload(client, csv_path)
    bench_query_filtered(client)
    bench_query_all(client)
    bench_download(client)

    server_mem_after = server_rss_mb()

    print(f"\n--- Server Memory (FastAPI process RSS) ---")
    if server_mem_before is not None:
        print(f"  Before: {server_mem_before:.1f} MB")
    if server_mem_after is not None:
        print(f"  After:  {server_mem_after:.1f} MB")
    if server_mem_before and server_mem_after:
        print(f"  Delta:  {server_mem_after - server_mem_before:.1f} MB")
    print()

    client.close()


if __name__ == "__main__":
    main()
