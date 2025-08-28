import os

from crewai import LLM
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()


if model := os.getenv("OLLAMA_MODEL_ID"):
    ollama_llm = LLM(
        model=model,
        base_url=os.getenv("OLLAMA_URL", "http://localhost:11434"),
    )

if model := os.getenv("OPENAI_MODEL_ID"):
    openai_llm = LLM(
        model=model,
        temperature=0.8,
        top_p=0.9,
        stop=["END"],
        api_key=os.getenv("OPENAI_API_KEY"),
        seed=42,
    )

if model := os.getenv("MODEL_ID"):
    watsonx_llm = LLM(
        model=model,
        base_url=os.getenv("WATSONX_URL"),
        project_id=os.getenv("WATSONX_PROJECTID"),
        max_tokens=8000,
        temperature=0.9,
    )

if os.getenv("VLLM_URL"):
    vllm_llm = AsyncOpenAI(
        api_key="EMPTY",
        base_url=os.getenv("VLLM_URL"),
        default_headers={
            "Content-Type": "application/json",
        },
    )

if os.getenv("VLLM_MODEL_ID"):
    vllm_crewai = LLM(
        model=os.getenv("VLLM_MODEL_ID"),
        api_key="EMPTY",
        base_url=os.getenv("VLLM_URL"),
        max_tokens=8000,
        temperature=0.0,
    )
