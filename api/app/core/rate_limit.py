from slowapi import Limiter
from slowapi.util import get_remote_address
from fastapi import Request


def get_api_key_or_ip(request: Request) -> str:
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return f"key:{api_key}"
    return get_remote_address(request)


limiter = Limiter(key_func=get_api_key_or_ip)
