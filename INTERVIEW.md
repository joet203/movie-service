# Interview Prep — Movie Database Service

Quick reference for discussing design decisions, trade-offs, and scaling.

---

## Architecture at a Glance

```
Client → POST /datasets (file upload)
           → Stream to temp file (1 MB chunks)
           → 202 Accepted + task_id
           → BackgroundTask: DuckDB native read_csv → staging table → atomic swap
           → SSE events: pending → processing → completed

Client → GET /movies?start_year=X&end_year=Y&genre=Z
           → DuckDB parameterized query with predicate pushdown

Client → GET /datasets/download
           → DuckDB COPY TO gzip → stream 64 KB chunks
```

---

## Anticipated Questions & Answers

### "Why DuckDB instead of PostgreSQL/SQLite?"

DuckDB is an embedded OLAP engine — optimized for scanning and filtering large datasets, not transactional workloads. Key advantages:

- **Out-of-core**: Spills to disk when data exceeds RAM. Handles datasets far larger than memory.
- **Columnar storage**: Filter predicates are pushed down to the scan layer. Only relevant columns are read.
- **Native CSV read**: `read_csv` parses CSV in C++, orders of magnitude faster than Python-side parsing.
- **No server**: Runs in-process, zero configuration.

SQLite is row-oriented (OLTP) — great for transactions, poor for analytical scans. PostgreSQL would work but adds operational complexity for a service that runs on a single machine.

### "Walk me through the ingestion pipeline."

1. Upload streams to a temp file in 1 MB chunks (never holds full CSV in memory).
2. Header validation via `csv.reader` (just reads first line).
3. DuckDB's `read_csv` ingests the file natively — `TRY_CAST` handles dirty data (non-numeric years, etc.).
4. Data goes into `movies_staging` table.
5. Atomic swap: `BEGIN; DROP movies; RENAME staging → movies; COMMIT;`
6. Temp file deleted.

If ingestion fails at any point, the staging table is dropped and the original `movies` table is untouched.

### "Why not use pandas or Polars?"

Deliberate restraint. The task is CSV → database → query. Adding a 200+ MB dependency for something DuckDB handles natively would be over-engineering. Python's `csv.reader` is used only for header validation (one line). DuckDB does the heavy lifting.

### "Your initial implementation was 92s. How did you optimize to 0.5s?"

The first version used Python's `csv.reader` to iterate row-by-row, parse types manually, accumulate 50K-row batches, and call `executemany`. This round-trips every row through Python.

The optimized version uses DuckDB's `read_csv` — the CSV is parsed in DuckDB's C++ engine, never touching Python. `TRY_CAST` replaces manual type parsing. Same correctness, 170x faster.

### "How does the progress tracking work?"

The upload endpoint returns `202 Accepted` with a `task_id`. Progress is tracked in a Python dict (GIL-safe for single-process). The SSE endpoint (`/tasks/{id}/events`) polls this dict and emits events when state changes.

With the DuckDB native reader, ingestion is sub-second for the test dataset, so progress goes 0 → 100 almost instantly. For truly massive files (GB+), the DuckDB operation would take longer and the SSE mechanism still reports the transition. In a production system with longer ingestion, you'd add more granular progress hooks.

### "What about concurrent uploads?"

DuckDB is single-writer. If two uploads arrive simultaneously, the second would fail to acquire the write lock. In production:

- Use a task queue (Celery, Dramatiq) to serialize ingestion jobs
- Return 409 Conflict if an ingestion is already in progress
- Or queue uploads and process sequentially

### "The in-memory task dict doesn't survive restarts."

Correct. This is a conscious trade-off for a single-process take-home. In production:

- Task state → Redis or database-backed store
- Job execution → Celery/Dramatiq with a message broker
- Horizontal scaling → multiple workers sharing external state

### "How would you scale this to 100M+ rows?"

DuckDB handles this out of the box — out-of-core processing spills to disk. But the API layer would need changes:

- **Pagination**: The `/movies` endpoint currently returns all matching rows. Add `limit`/`offset` or cursor-based pagination.
- **Streaming responses**: For very large result sets, stream JSON lines instead of building a full list.
- **Ingestion**: DuckDB's `read_csv` scales linearly. Even at 100M rows it would be minutes, not hours.
- **Download**: Already streaming — no memory concerns.

### "Why SSE instead of WebSockets?"

Progress reporting is unidirectional (server → client). SSE is simpler:

- No upgrade handshake
- Standard HTTP
- Native browser support via `EventSource` API
- Auto-reconnection built in

WebSockets would be overkill for one-way status updates.

### "Why `LIKE '%Action%'` instead of normalizing genres?"

DuckDB is an OLAP engine where denormalized data is idiomatic. The `LIKE` predicate is pushed down to the scan layer and runs in C++. A junction table would add join complexity with no performance benefit — the filtered query returns 8,377 rows in 0.03s.

Trade-off: substring matching could produce false positives (`"Drama"` matching `"Melodrama"`), but the dataset's genre vocabulary doesn't have such conflicts. If it did, you'd split on commas and use `list_contains`.

### "How did you handle dirty data in the CSV?"

The CSV has entries like `"III"` in the year column and irregular quoting (`"""Giliap"""`). DuckDB's `TRY_CAST` converts unparseable values to `NULL` instead of failing. `strict_mode=false` tolerates quoting issues. This matches the behavior of the original Python-side parsing (which had try/except blocks) but runs in C++.

### "What would you add with more time?"

- **Pagination** on the query endpoint
- **Input validation** on genre (whitelist of valid genres)
- **Rate limiting** on upload endpoint
- **Persistent task state** (Redis)
- **OpenTelemetry** instrumentation
- **CI/CD** pipeline with automated testing
- **Docker** containerization for consistent deployment
- **API versioning** for backwards compatibility

### "How did you use AI tools?"

Claude Code (CLI) was the primary tool. Used it for:

- Architecture planning (interactive Q&A before writing code)
- Implementation (generated code with review at each step)
- Debugging (DuckDB API changes, test fixture ordering)
- Optimization (identified DuckDB native CSV reader opportunity)
- Documentation (DESIGN.md, this interview prep)

All AI workfiles (plan.md, plan.html) are included in the submission.

---

## Key Numbers

| Metric | Value |
|---|---|
| CSV rows | 367,314 |
| CSV size | 15.8 MB |
| Upload + ingest | 0.52s |
| Filtered query | 0.03s |
| Full query (367K rows) | 0.83s |
| Download (gzip) | 0.49s |
| Server idle RSS | ~85 MB |
| Test suite | 15 tests, 0.17s |
| Dependencies | 2 (duckdb, python-multipart) |
