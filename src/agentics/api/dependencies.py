import asyncio
from fastapi import HTTPException, Security, Request
from fastapi.security import APIKeyHeader
from slowapi import Limiter
from slowapi.util import get_remote_address

from agentics.api.config import settings

# 1. Rate Limiter
# Uses the client's IP address to track limits
limiter = Limiter(key_func=get_remote_address)

# 2. Authentication
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def verify_api_key(key: str = Security(api_key_header)):
    """
    Validates the X-API-Key header.
    """
    if not key or key != settings.API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Could not validate credentials"
        )
    return key

# 3. Concurrency Guard
# A global semaphore to prevent overloading the LLM provider
# or running out of memory with too many heavy AG instances.
execution_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_EXECUTIONS)

async def get_execution_token():
    """
    Dependency that acquires a semaphore token.
    Waits if the system is under heavy load.
    """
    async with execution_semaphore:
        yield