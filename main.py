from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from app import db
from app.movies import router as movies_router

PROJECT_DIR = Path(__file__).parent
FRONTEND_DIR = PROJECT_DIR / "frontend"
SAMPLE_CSV = PROJECT_DIR / "movies.csv"


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield
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
    expose_headers=["X-Total-Count"],
)

app.include_router(movies_router)


@app.get("/")
async def serve_frontend():
    return FileResponse(FRONTEND_DIR / "index.html")


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
