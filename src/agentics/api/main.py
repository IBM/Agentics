from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from agentics.api.applications.smart_spreadsheet.app import SmartSpreadsheetApp
from agentics.api.applications.text2sql.app import Text2SqlApp
from agentics.api.config import settings
from agentics.api.routers import system, session
from agentics.api.services.app_registry import app_registry

# Import apps to register them
from agentics.api.applications.dummy import DummyApp
from agentics.api.applications.macro_economic_analysis.app import MacroEconApp
from agentics.api.applications.dynamic_info_extraction.app import DynamicExtractionApp


@asynccontextmanager
async def lifespan(app: FastAPI):
    app_registry.register(DummyApp)
    app_registry.register(MacroEconApp)
    app_registry.register(DynamicExtractionApp)
    app_registry.register(Text2SqlApp)
    app_registry.register(SmartSpreadsheetApp)
    yield


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
