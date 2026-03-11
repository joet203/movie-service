# Design Choices

## Objectives

- Meet the required API surface with clear behavior.
- Handle datasets larger than memory.
- Keep CPU/memory usage predictable for long-running operations.
- Keep implementation readable and dependency-light.

## Core Decisions

## 1) DuckDB for storage and analytics

DuckDB is used as an embedded analytical database because it is strong at large scans/filtering and supports out-of-core execution. This is a good fit for CSV ingest, filtered movie queries, and whole-table export.

Key benefits used in this project:

- Native CSV ingestion via `read_csv(...)`
- ACID transactions for atomic table swap
- Native gzip CSV export via `COPY ... COMPRESSION 'gzip'`

## 2) Atomic ingestion with staging table

Uploads are written to a temp CSV on disk, validated, then ingested into `movies_staging`. On success:

1. `DROP TABLE IF EXISTS movies`
2. `ALTER TABLE movies_staging RENAME TO movies`

inside a transaction. This prevents partially loaded datasets from becoming visible.

## 3) Progress model for long requests (>2s)

A shared task model is used for upload, query, and export:

- task created with `task_id`
- status transitions: `pending -> processing -> completed/error`
- phase-based progress percentages (deterministic milestones)
- real-time events streamed with SSE (`/tasks/{task_id}/events`)

For synchronous UX:

- `GET /movies` and `GET /datasets/download` attempt fast completion.
- If work exceeds the 2-second threshold, endpoint returns `202` + `task_id`.
- Client follows with SSE and fetches final result/artifact from task endpoints.

This avoids guessing runtime up front while satisfying the requirement for long-running request progress.

## 4) Thread-safe task registry with bounded growth

Task state is stored in-process (dict + lock) for simplicity in a single-process service. To prevent unbounded memory growth:

- completed/error tasks are pruned by TTL
- registry is capped by max task count
- export artifacts are deleted when tasks are consumed/pruned/shutdown

## 5) Per-operation DB connections

The app stores a DB path, not a shared connection object. Each operation opens/closes its own DuckDB connection. This avoids shared-connection concurrency hazards and keeps behavior easier to reason about under parallel requests.

## 6) Memory behavior

- Upload: streamed to disk in chunks (no full file buffering)
- Ingestion: DuckDB reads CSV from file
- Download: gzip artifact streamed in chunks
- Query: paginated with `limit/offset`, with `X-Total-Count` for pagination UI

## Trade-offs

- Task state is in-memory and process-local (not distributed/persistent across restarts).
- Progress is phase-based, not exact SQL execution percentage.
- Export in task mode writes a temporary gzip artifact before download.

These are acceptable for an assessment implementation and keep the service focused and understandable.
