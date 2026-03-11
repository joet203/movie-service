# Create a Movie Database Service

### Problem Statement

(Expected time commitment: 2-ish hours)

Use AI to build a FastAPI service that serves as a simple movie database, with the following requirements:

- Input a movie database CSV from the file `movies.csv`.
- Support downloading the whole database as a gzipped CSV file.
- Support query requests by date range and genre.
- Provide progress updates for long running requests > 2 seconds

Your assignment will be graded on meeting the above requirements, overall performance and responsiveness of your service, efficient management of CPU and memory, and graceful error handling. Outside of the above requirements, you have flexibility in implementation details. Make reasonable design choices and be prepared to explain them.

The included `movies.csv` is small by modern standards, for testing and distribution, but design your solution as if it needed to handle much larger datasets.

Please return your solution as a zipfile to the same email (i.e. recruiter) that sent it to you.

### API Surface

You should implement endpoints for:

- Submitting the original datafile
- Downloading the entire dataset
- Query endpoint for at least date ranges and genre(s), returning a list of movies
- Endpoint for real-time updates

### Deliverables

Your submission should include:

- All of your AI tool's workfiles
- A working FastAPI application
- A brief explanation of your design choices
- Instructions on how to build and run your application

# Code Template Instructions

### Installation

Install `uv` and run `uv sync`

### Run

Run the following command at root directory (this will also install dependency)

```bash
uv run fastapi dev main.py
```

### Add dependencies

If you need any other packages, just use `uv` to add them

```bash
uv add some_other_package
```

### Useful info

Go to `localhost:8000/docs` for the `Swagger` UI

### Implemented API Endpoints

- `POST /datasets` — upload/ingest CSV (returns `202` with `task_id`)
- `GET /tasks/{task_id}/events` — real-time task progress via SSE
- `GET /movies` — query by `start_year`, `end_year`, `genre` (plus sort/pagination)
  - Returns `200` with rows when query finishes quickly
  - Returns `202` with `task_id` when query exceeds ~2 seconds
- `GET /tasks/{task_id}/results` — fetch rows for a completed long-running query
- `GET /datasets/download` — download full dataset as `movies.csv.gz`
