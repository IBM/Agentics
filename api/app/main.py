import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from api.app.api.v1.atypes import router as atypes_router
from api.app.api.v1.sessions import router as sessions_router
from api.app.api.v1.agents import router as agents_router
from api.app.core.rate_limit import limiter

from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure the atypes directory exists on container start
    atypes_path = os.getenv("ATYPES_PATH", "/code/api/predefined_types")
    os.makedirs(atypes_path, exist_ok=True)
    yield


app = FastAPI(
    title="Agentics Demo API",
    version="0.1.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

allowed_origins = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in allowed_origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(atypes_router, prefix="/v1")
app.include_router(sessions_router, prefix="/v1")
app.include_router(agents_router, prefix="/v1")


@app.get("/healthz")
def health():
    return {"status": "ok"}
