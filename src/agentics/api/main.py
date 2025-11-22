from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from agentics.api.config import settings
from agentics.api.routers import system, session
from agentics.api.services.app_registry import app_registry
from agentics.api.dependencies import verify_api_key, limiter

# Import apps
from agentics.api.applications.dummy import DummyApp
from agentics.api.applications.macro_economic_analysis.app import MacroEconApp
from agentics.api.applications.dynamic_info_extraction.app import DynamicExtractionApp
from agentics.api.applications.text2sql.app import Text2SqlApp
from agentics.api.applications.smart_spreadsheet.app import SmartSpreadsheetApp


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Register Apps
    app_registry.register(DummyApp)
    app_registry.register(MacroEconApp)
    app_registry.register(DynamicExtractionApp)
    app_registry.register(Text2SqlApp)
    app_registry.register(SmartSpreadsheetApp)
    yield
    # In production, we could add a "cleanup all sessions" hook here


app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    lifespan=lifespan,
    # Enforce API Key on ALL endpoints by default
    dependencies=[Depends(verify_api_key)],
)

# Set up Rate Limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Lock this down to specific domains in Phase 5
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(system.router, tags=["System"])
app.include_router(session.router, tags=["Session"])
