from fastapi import APIRouter
from agentics.api.services.app_registry import app_registry

router = APIRouter()

@router.get("/health")
async def health_check():
    return {"status": "ok"}

@router.get("/apps")
async def list_applications():
    return {"applications": app_registry.list_apps()}