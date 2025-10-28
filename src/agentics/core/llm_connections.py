import os

from crewai import LLM
from dotenv import load_dotenv
from loguru import logger
from openai import AsyncOpenAI

# Turbo Models gpt-oss:20b, deepseek-v3.1:671b

load_dotenv()

verbose = False


def get_llm_provider(provider_name: str = None) -> LLM:
    """
    Retrieve the LLM instance based on the provider name. If no provider name is given,
    the function returns the first available LLM.

    Args:
        provider_name (str): The name of the LLM provider (e.g., 'openai', 'watsonx', 'gemini').

    Returns:
        LLM: The corresponding LLM instance.

    Raises:
        ValueError: If the specified provider is not available.
    """

    if provider_name is None or provider_name == "":
        if len(available_llms) > 0:
            if verbose:
                logger.debug(
                    f"Available LLM providers: {list(available_llms)}. None specified, defaulting to '{list(available_llms)[0]}'"
                )
            return list(available_llms.values())[0]
        else:
            raise ValueError(
                "No LLM is available. Please check your .env configuration."
            )

    else:
        if provider_name in available_llms:
            if verbose:
                logger.debug(f"Using specified LLM provider: {provider_name}")
            return available_llms[provider_name]
        else:
            raise ValueError(
                f"LLM provider '{provider_name}' is not available. Please check your .env configuration."
            )


available_llms = {}

gemini_llm = (
    LLM(model=os.getenv("GEMINI_MODEL_ID", "gemini/gemini-2.0-flash"), temperature=0.7)
    if os.getenv("GEMINI_API_KEY")
    else None
)


# ollama_llm = (
#     LLM(model=os.getenv("OLLAMA_MODEL_ID"), base_url="http://localhost:11434")
#     if os.getenv("OLLAMA_MODEL_ID")
#     else None
# )


openai_llm = (
    LLM(
        model=os.getenv(
            "OPENAI_MODEL_ID", "openai/gpt-4"
        ),  # call model by provider/model_name
        temperature=0.8,
        top_p=0.9,
        stop=["END"],
        api_key=os.getenv("OPENAI_API_KEY"),
        seed=42,
    )
    if os.getenv("OPENAI_API_KEY") and os.getenv("OPENAI_MODEL_ID")
    else None
)

watsonx_llm = (
    LLM(
        model=os.getenv("MODEL_ID"),
        base_url=os.getenv("WATSONX_URL"),
        project_id=os.getenv("WATSONX_PROJECTID"),
        api_key=os.getenv("WATSONX_APIKEY"),
        temperature=0,
        max_tokens=4000,
        max_input_tokens=100000,
    )
    if os.getenv("WATSONX_APIKEY")
    and os.getenv("WATSONX_URL")
    and os.getenv("WATSONX_PROJECTID")
    and os.getenv("MODEL_ID")
    else None
)

vllm_llm = (
    AsyncOpenAI(
        api_key="EMPTY",
        base_url=os.getenv("VLLM_URL"),
        default_headers={
            "Content-Type": "application/json",
        },
    )
    if os.getenv("VLLM_URL")
    else None
)

vllm_crewai = (
    LLM(
        model=os.getenv("VLLM_MODEL_ID"),
        api_key="EMPTY",
        base_url=os.getenv("VLLM_URL"),
        max_tokens=1000,
        temperature=0.0,
    )
    if os.getenv("VLLM_URL") and os.getenv("VLLM_MODEL_ID")
    else None
)

i = 0
if watsonx_llm:
    available_llms["watsonx"] = watsonx_llm
    i += 1
if gemini_llm:
    available_llms["gemini"] = gemini_llm
    i += 1
if openai_llm:
    available_llms["openai"] = openai_llm
