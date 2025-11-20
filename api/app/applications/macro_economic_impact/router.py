from fastapi import APIRouter

APP_METADATA = {
    "name": "Macro Economic Impact",
    "description": "Analyze market factors over time using pre-loaded datasets.",
    "files_needed": False,
}

router = APIRouter(tags=["App: Macro Economic Impact"])


@router.get("/health")
def app_health():
    return {"status": "Macro Economic Impact App is ready"}
