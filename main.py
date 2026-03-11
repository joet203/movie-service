from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from app import db
from app import movies as movies_module
from app.movies import router as movies_router

PROJECT_DIR = Path(__file__).parent
FRONTEND_DIR = PROJECT_DIR / "frontend"
SAMPLE_CSV = PROJECT_DIR / "movies.csv"
LARGE_CSV = FRONTEND_DIR / "movies_large.csv"


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield
    movies_module.cleanup_all_task_artifacts()
    db.close_db()


app = FastAPI(
    title="Movie API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count", "X-Query-Time"],
)

app.include_router(movies_router)


@app.get("/")
async def serve_frontend():
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/design")
async def serve_design():
    return FileResponse(FRONTEND_DIR / "design.html")


@app.get("/interview")
async def serve_interview():
    return FileResponse(FRONTEND_DIR / "interview.html")


@app.get("/plan")
async def serve_plan():
    return FileResponse(FRONTEND_DIR / "plan.html")


@app.get("/readme")
async def serve_readme():
    return FileResponse(FRONTEND_DIR / "readme.html")


@app.get("/sample-data")
async def download_sample_csv():
    if not SAMPLE_CSV.exists():
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Sample CSV not found")
    return FileResponse(
        SAMPLE_CSV,
        media_type="text/csv",
        filename="movies.csv",
    )


@app.get("/sample-data-large")
async def download_large_csv():
    if not LARGE_CSV.exists():
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Large CSV not found — run: uv run python generate_large_csv.py")
    return FileResponse(
        LARGE_CSV,
        media_type="text/csv",
        filename="movies_large.csv",
    )
