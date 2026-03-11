# Design Choices

## Overview

Uploads are streamed to disk in chunks to avoid buffering the full file in application memory. Querying and storage are handled with DuckDB because it supports efficient analytical filtering on columnar data and can operate out-of-core on datasets larger than available RAM.

Long-running ingestion is decoupled from the request/response cycle using background task execution plus in-memory task status tracking. The endpoint returns `202 Accepted` immediately with a task ID. Real-time progress updates are exposed via Server-Sent Events (SSE), driven by actual row counts rather than estimates.

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

## Why stdlib-only for CSV parsing?

No Polars, no pandas. The Python standard library's `csv.reader` handles everything we need:

- Correctly parses quoted fields containing commas (e.g., `"Action, Crime, Drama"`)
- Streams row-by-row with constant memory overhead
- Zero additional dependencies

Adding a heavy dependency for simple CSV iteration would be over-engineering.

## Memory Management

Every operation avoids loading full datasets into memory:

- **Upload**: `UploadFile` streamed to a temp file in 1 MB chunks.
- **Ingestion**: `csv.reader` iterates row-by-row. Rows are batched into 50K-row groups and flushed to DuckDB via `executemany`. Peak memory is proportional to batch size, not file size.
- **Query**: DuckDB pushes `WHERE` filters to the scan layer. Only matching rows are materialized.
- **Download**: DuckDB writes a gzip-compressed CSV to a temp file natively. The file is streamed to the client in 64 KB chunks, then deleted.

## Ingestion: Background Task + Real Progress

CSV ingestion runs as a `BackgroundTasks` function. Starlette runs synchronous background tasks in a thread pool, keeping the event loop free.

Progress tracking is real, not simulated:
1. A fast first pass counts total lines (sequential read, no parsing — OS page cache makes the second pass essentially free).
2. Every 50,000 rows, the progress percentage is updated in a shared dict.
3. The SSE endpoint polls this dict and emits events only when progress has changed.

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
- `LIKE '%Action%'` is pushed down to DuckDB's scan layer and is efficient on columnar data
- A junction table would add join complexity with no performance benefit
- Trade-off: substring matching could theoretically produce false positives (e.g., "Drama" matching "Melodrama"), but the dataset's genre vocabulary doesn't contain such conflicts

## Task State: In-Memory Dict

Task progress is tracked in a plain Python dict:

- Single-process FastAPI — no cross-process state needed
- Dict mutations are GIL-safe between the background thread and SSE generator
- Task state is ephemeral by nature

In production, this would be replaced with Redis or a database-backed solution for horizontal scaling and persistence across restarts.
