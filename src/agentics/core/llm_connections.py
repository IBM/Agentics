import os

from crewai import LLM
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()




ollama_llm = LLM(
    model=os.getenv("OLLAMA_MODEL_ID"),
    base_url="http://localhost:11434"
)

openai_llm = LLM(
    model=os.getenv("OPENAI_MODEL_ID"), # call model by provider/model_name
    temperature=0.8,
    top_p=0.9,
    stop=["END"],
    api_key=os.getenv("OPENAI_API_KEY"),
    seed=42
)

watsonx_llm = LLM(
    model=os.getenv("MODEL_ID"),
    base_url=os.getenv("WATSONX_URL"),
    project_id=os.getenv("WATSONX_PROJECTID"),
    max_tokens=8000,
    temperature=0.9,
)

vllm_llm = AsyncOpenAI(
    api_key="EMPTY",
    base_url=os.getenv("VLLM_URL"),
    default_headers={
        "Content-Type": "application/json",
    },
)

vllm_crewai = LLM(
    model=os.getenv("VLLM_MODEL_ID"),
    api_key="EMPTY",
    base_url=os.getenv("VLLM_URL"),
    max_tokens=8000,
    temperature=0.0,
)
