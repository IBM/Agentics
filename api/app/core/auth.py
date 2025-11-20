import os
from fastapi import HTTPException, Header
from typing import Optional

IS_DEV_MODE = os.getenv("CREWAI_DEV_MODE", "false").lower() == "true"


def get_valid_keys() -> set[str]:
    keys_str = os.getenv("API_KEYS", "")
    if not keys_str:
        return set()
    return set(k.strip() for k in keys_str.split(",") if k.strip())


VALID_KEYS = get_valid_keys()


async def verify_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    if IS_DEV_MODE:
        return "dev_mode"

    if not VALID_KEYS:
        raise HTTPException(
            status_code=500,
            detail="Server misconfiguration: No API keys configured and not in dev mode.",
        )

    if not x_api_key:
        raise HTTPException(status_code=401, detail="API key required")

    if x_api_key not in VALID_KEYS:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return x_api_key


async def optional_api_key(x_api_key: Optional[str] = Header(None)) -> Optional[str]:
    if IS_DEV_MODE:
        return "dev_mode"
    if not VALID_KEYS:
        return None
    return x_api_key
