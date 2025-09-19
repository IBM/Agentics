from typing import Type

from crewai import LLM
from pydantic import BaseModel

from agentics.core.llm_connections import get_llm_provider


async def generate_structured_output(
    prompt: str, output_type: Type[BaseModel], llm: LLM = get_llm_provider()
):
    structured_llm = llm.with_structured_output(output_type)
    return await structured_llm.ainvoke(prompt)
