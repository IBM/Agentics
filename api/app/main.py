from fastapi import FastAPI
from dotenv import load_dotenv
from .api.v1.atypes import router as atypes_router

load_dotenv()

app = FastAPI(title="Agentics Demo API", version="0.1.0")


@app.get("/healthz")
def health():
    return {"status": "ok"}


app.include_router(atypes_router, prefix="/v1")
