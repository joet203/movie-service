# CPU and Memory Efficiency Notes

## Goal

Keep the service responsive and memory-safe while handling datasets that can be much larger than the sample CSV.

## Efficiency Choices Implemented

1. Stream upload body to disk (`POST /datasets`)
- CSV is read in chunks and written to a temp file.
- Avoids holding full upload payloads in process memory.

2. Use DuckDB native CSV ingestion
- Ingestion uses DuckDB `read_csv(...)` and typed `TRY_CAST` in SQL.
- This is significantly more CPU-efficient than Python row-by-row parsing for large files.

3. Atomic staging-table swap
- Data loads into `movies_staging` and is atomically swapped into `movies`.
- Prevents partial-state query complexity and rollback overhead.

4. Per-operation DuckDB connections
- Avoids unsafe shared connection contention across request/background threads.
- Improves stability under concurrent load.

5. Bounded query payloads
- Query endpoint enforces `limit` (default 1000, max 10000).
- Prevents accidental huge in-memory JSON responses.

6. Streaming download responses
- Gzip files are streamed in chunks (64KB) instead of fully buffered in memory.

7. Long-request async handoff (>2s)
- `/movies` and `/datasets/download` return `202 + task_id` when work exceeds threshold.
- Keeps HTTP request paths responsive under heavy workloads.

8. Task registry pruning
- Completed/error tasks are pruned by TTL and max task count.
- Prevents unbounded in-memory task growth.

9. Export artifact cleanup
- Temporary gzip artifacts are deleted on download completion, task eviction, and app shutdown.
- Prevents disk buildup from stale task files.

## Practical Resource Behavior

- CPU spikes are expected during CSV ingest and gzip export (compute-heavy phases).
- Memory can still rise significantly on large datasets because DuckDB uses internal buffers/caches.
- Current behavior is efficient for this assessment scope, but extremely tight memory environments may require extra caps.

## If Tighter Resource Limits Are Required

Potential hardening options:

- Set DuckDB memory cap at startup (for example `PRAGMA memory_limit='1GB'`).
- Set DuckDB thread cap (for example `PRAGMA threads=2`) on constrained CPUs.
- Reduce `MAX_QUERY_LIMIT` for stricter response-size control.
