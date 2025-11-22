from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from agentics.api.config import settings
from agentics.api.routers import system, session
from agentics.api.services.app_registry import app_registry

# Import apps to register them
from agentics.api.applications.dummy import DummyApp


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app_registry.register(DummyApp)
    yield
    # Shutdown


app = FastAPI(title=settings.API_TITLE, version=settings.API_VERSION, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(system.router, tags=["System"])
app.include_router(session.router, tags=["Session"])
