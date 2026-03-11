# Movie Database Service - Assessment

## Requirements

FastAPI service serving as a simple movie database:

1. **Upload CSV** - Endpoint to submit the movie database CSV (`movies.csv`)
2. **Download dataset** - Download the whole database as a gzipped CSV file
3. **Query endpoint** - Query by date range and genre(s), returning a list of movies
4. **Progress updates** - Real-time progress updates for long-running requests (>2 seconds)

## Grading Criteria

- Meeting the 4 requirements above
- Overall performance and responsiveness
- Efficient CPU and memory management
- Graceful error handling
- Reasonable design choices (be prepared to explain them)

## Key Constraints

- Design as if handling much larger datasets (CSV is ~367K lines)
- CSV has columns: `movie_name`, `year`, `genres`, `rating`
- Some rows have missing `year` and/or `rating` values
- Genres are comma-separated within quotes (e.g., `"Action, Crime, Drama"`)

## Project Setup

- Python 3.13+, FastAPI, Pydantic
- Package manager: `uv`
- Run: `uv run fastapi dev main.py`
- Add deps: `uv add <package>`
- Swagger UI: `localhost:8000/docs`

## Deliverables

- All AI tool workfiles
- Working FastAPI application
- Brief explanation of design choices
- Instructions to build and run

## Existing Structure

```
main.py          - FastAPI app entry, includes movies router
app/movies.py    - Router with placeholder /hello and /search endpoints
app/model.py     - MoviesQuery Pydantic model (start_year, end_year, genre)
movies.csv       - ~367K rows, cols: movie_name, year, genres, rating
pyproject.toml   - Dependencies: fastapi[standard], pydantic
```
