from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from api.app.api.v1.atypes import router as atypes_router
from api.app.api.v1.sessions import router as sessions_router
from api.app.api.v1.agents import router as agents_router
from api.app.core.rate_limit import limiter

app = FastAPI(title="Agentics Demo API", version="0.2.0")

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
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
