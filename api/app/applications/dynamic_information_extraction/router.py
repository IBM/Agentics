from fastapi import APIRouter

APP_METADATA = {
    "name": "Dynamic Information Extraction",
    "description": "Upload files (CSV/JSON) and extract structured data using Agentics.",
    "files_needed": True,
}

router = APIRouter(tags=["App: Dynamic Information Extraction"])


@router.get("/health")
def app_health():
    return {"status": "Dynamic Information Extraction App is ready"}
