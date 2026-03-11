from pydantic import BaseModel


class Movie(BaseModel):
    movie_name: str
    year: int | None
    genres: str
    rating: float | None


class TaskResponse(BaseModel):
    task_id: str


class TaskStatus(BaseModel):
    status: str
    progress: int
    error: str | None = None


class HealthResponse(BaseModel):
    status: str
