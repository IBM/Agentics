from typing import Type

from crewai import LLM
from pydantic import BaseModel

from agentics.core.llm_connections import watsonx_llm


async def generate_structured_output(
    prompt: str, output_type: Type[BaseModel], llm: LLM = watsonx_llm
):
    structured_llm = llm.with_structured_output(output_type)
    return await structured_llm.ainvoke(prompt)
