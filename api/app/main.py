from fastapi import FastAPI
from dotenv import load_dotenv
from .api.v1.atypes import router as atypes_router
from .api.v1.sessions import router as sessions_router
from .api.v1.agents import router as agents_router

load_dotenv()

app = FastAPI(title="Agentics Demo API", version="0.1.0")

app.include_router(atypes_router, prefix="/v1")
app.include_router(sessions_router, prefix="/v1")
app.include_router(agents_router, prefix="/v1")


@app.get("/healthz")
def health():
    return {"status": "ok"}
