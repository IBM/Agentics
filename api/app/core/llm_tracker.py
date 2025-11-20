from typing import Dict

_USAGE: Dict[str, int] = {}
MAX_TOKENS_PER_SESSION = 50000


def estimate_tokens(text: str) -> int:
    return len(text) // 4


def track_usage(session_id: str, prompt: str | list[str]) -> None:
    if isinstance(prompt, list):
        tokens = sum(estimate_tokens(p) for p in prompt)
    else:
        tokens = estimate_tokens(prompt)

    current = _USAGE.get(session_id, 0)
    _USAGE[session_id] = current + tokens


def check_quota(session_id: str) -> tuple[bool, int, int]:
    used = _USAGE.get(session_id, 0)
    remaining = MAX_TOKENS_PER_SESSION - used
    return used < MAX_TOKENS_PER_SESSION, used, remaining


def reset_usage(session_id: str) -> None:
    _USAGE.pop(session_id, None)


def get_usage(session_id: str) -> dict:
    used = _USAGE.get(session_id, 0)
    return {
        "used": used,
        "limit": MAX_TOKENS_PER_SESSION,
        "remaining": MAX_TOKENS_PER_SESSION - used,
    }
