from agentics import AG


async def nl_to_atype(name: str, description: str, retries: int = 3):
    ag = AG()
    await ag.generate_atype(description, retry=retries)
    if not ag.atype_code:
        raise ValueError("LLM failed to generate type")
    return ag.atype_code, ag.atype
