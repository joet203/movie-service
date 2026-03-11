from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import db
from app.movies import router as movies_router


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
