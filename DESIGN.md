# Design Choices

## Overview

Uploads are streamed to disk in chunks to avoid buffering the full file in application memory. Querying and storage are handled with DuckDB because it supports efficient analytical filtering on columnar data and can operate out-of-core on datasets larger than available RAM.

Long-running ingestion is decoupled from the request/response cycle using background task execution plus in-memory task status tracking. The endpoint returns `202 Accepted` immediately with a task ID. Progress updates are exposed via Server-Sent Events (SSE).

Data replacement uses a staging table pattern — new data is ingested into `movies_staging`, then atomically swapped into `movies` via a transaction. This ensures queries always see a complete dataset, never partial ingestion state.

Large downloads are returned with `StreamingResponse` over a DuckDB-generated gzip file, so the response is streamed incrementally in 64 KB chunks rather than materialized in memory.

For this take-home, in-process task state (a Python dict) was chosen for simplicity. In production, job execution and status state would be externalized (e.g., Redis, a task queue) to support horizontal scaling and persistence across restarts.

---

## Why DuckDB?

DuckDB is an embedded OLAP (Online Analytical Processing) database that runs in-process — no separate server needed. Unlike transactional databases (PostgreSQL, MySQL) which are optimized for many small reads and writes, OLAP engines are designed for scanning and filtering large volumes of data efficiently.

- **Out-of-core processing**: Automatically spills to disk when data exceeds available RAM. Handles datasets far larger than memory without any configuration.
- **Columnar storage**: Analytical queries (filters, scans) are inherently faster with columnar layout. DuckDB pushes filter predicates down to the scan layer.
- **ACID transactions**: Enables the atomic staging table swap during ingestion — queries never see partial data.
- **Native gzip export**: `COPY ... TO ... COMPRESSION 'gzip'` handles CSV formatting and compression in a single C++ pass.

## Ingestion: DuckDB Native CSV Reader

CSV ingestion uses DuckDB's built-in `read_csv` function rather than Python-side parsing. This was an intentional optimization — an initial implementation using Python's `csv.reader` with `executemany` batching took **92 seconds** for the 367K-row dataset. Switching to DuckDB's native C++ CSV reader reduced this to **0.5 seconds** (170x faster).

- `TRY_CAST` handles dirty data gracefully (e.g., `"III"` in the year column becomes `NULL` instead of failing)
- `strict_mode=false` tolerates quoting irregularities in the CSV
- Header validation is still performed via a quick `csv.reader` first-line check before DuckDB ingests
- The staging table pattern is preserved for atomic replacement

The Python `csv.reader` is still used for header validation — stdlib for correctness, DuckDB for performance.

## Memory Management

Every operation avoids loading full datasets into memory:

- **Upload**: `UploadFile` streamed to a temp file in 1 MB chunks.
- **Ingestion**: DuckDB reads the CSV directly from disk in its C++ engine. No Python-side row iteration.
- **Query**: DuckDB pushes `WHERE` filters to the scan layer. Only matching rows are materialized.
- **Download**: DuckDB writes a gzip-compressed CSV to a temp file natively. The file is streamed to the client in 64 KB chunks, then deleted.

## Background Tasks + SSE

CSV ingestion runs as a `BackgroundTasks` function. Starlette runs synchronous background tasks in a thread pool, keeping the event loop free. The SSE endpoint polls task state and emits events when status changes.

SSE is used for ingestion because it's the only operation that can exceed the 2-second threshold on large files. Queries are bounded by the 10K row pagination limit (0.03–0.83s even for the full dataset). Downloads stream natively via chunked HTTP response — the client receives data incrementally as DuckDB writes it, so the streaming itself serves as progress. If queries or downloads ever became long-running (e.g., unbounded result sets or multi-GB exports), the same SSE pattern could be applied by wrapping them in a background task.

## Atomic Data Replacement

Each upload replaces the entire dataset atomically using a staging table:

1. Ingest into `movies_staging`
2. `BEGIN TRANSACTION; DROP TABLE IF EXISTS movies; ALTER TABLE movies_staging RENAME TO movies; COMMIT;`

Queries always see either the complete old dataset or the complete new one. If ingestion fails, the staging table is dropped and the original `movies` table is preserved.

## SSE over WebSockets

Server-Sent Events was chosen over WebSockets because progress reporting is unidirectional (server to client). SSE uses standard HTTP with no upgrade handshake, and has native browser support via the `EventSource` API.

## Year vs Date Range

The CSV data only contains a `year` column, not a full date. The query API uses `start_year`/`end_year` as an honest interpretation of the "date range" requirement.

## Genre Filtering: LIKE vs Normalization

Genres are stored as a single comma-separated string rather than normalized into a junction table:

- DuckDB is an OLAP engine where denormalized data is idiomatic
- `ILIKE '%Action%'` (case-insensitive) is pushed down to DuckDB's scan layer and is efficient on columnar data
- A junction table would add join complexity with no performance benefit
- Trade-off: substring matching could theoretically produce false positives (e.g., "Drama" matching "Melodrama"), but the dataset's genre vocabulary doesn't contain such conflicts

## Task State: In-Memory Dict

Task progress is tracked in a plain Python dict:

- Single-process FastAPI — no cross-process state needed
- Dict mutations are GIL-safe between the background thread and SSE generator
- Task state is ephemeral by nature

In production, this would be replaced with Redis or a database-backed solution for horizontal scaling and persistence across restarts.

---

## Benchmarks

Measured on Apple M1, 367,314-row CSV (15.8 MB), Python 3.13, DuckDB 1.3:

| Operation | Time | Notes |
|---|---|---|
| Upload + Ingest | 0.52s | DuckDB native `read_csv` with atomic staging swap |
| Query (filtered) | 0.03s | 8,377 Action movies, 2020–2023 |
| Query (all rows) | 0.83s | 367,314 rows serialized to JSON |
| Download (gzip) | 0.49s | 4.9 MB gzipped CSV via `StreamingResponse` |

Server memory (RSS): ~85 MB idle → ~288 MB after all operations (DuckDB buffer pool + query materialization).

Run benchmarks: `uv run python benchmark.py movies.csv` (requires server running on port 8000).
