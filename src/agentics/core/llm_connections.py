import os

from crewai import LLM
from dotenv import load_dotenv
from loguru import logger
from openai import AsyncOpenAI

load_dotenv()

verbose = False


def get_llm_provider(provider_name: str | None = None) -> LLM | AsyncOpenAI | None:
    """
    Retrieve the LLM instance based on the provider name. If no provider name is given,
    the function returns the first available LLM.

    Args:
        provider_name (str): The name of the LLM provider (e.g., 'openai', 'watsonx', 'gemini').

    Returns:
        LLM | AsyncOpenAI | None: The corresponding LLM instance.
    """
    llms = _get_available_llms()

    if provider_name is None or provider_name == "":
        if len(llms) > 0:
            if verbose:
                logger.debug(
                    f"Available LLM providers: {list(llms)}. None specified, defaulting to '{list(llms)[0]}'"
                )
            return list(llms.values())[0]
        else:
            logger.debug("No LLM is available. Please check your .env configuration.")
            return None

    if provider_name in llms:
        if verbose:
            logger.debug(f"Using specified LLM provider: {provider_name}")
        return llms[provider_name]

    logger.debug(
        f"LLM provider '{provider_name}' is not available. Please check your .env configuration."
    )
    return None


def _check_env(*var_names: str) -> bool:
    """Check if all given environment variables are non-empty."""
    return all(os.getenv(var) for var in var_names)


def _get_openai_compatible_variants() -> dict[str, tuple[str, str, str]]:
    """Discover OpenAI-compatible variants from env vars like OPENAI_COMPATIBLE_<VARIANT>_API_KEY."""
    variants = {}
    suffixes = set()

    # Find all unique suffixes
    for var_name in os.environ:
        if var_name.startswith("OPENAI_COMPATIBLE_") and var_name.endswith("_API_KEY"):
            suffix = var_name[len("OPENAI_COMPATIBLE_") : -len("_API_KEY")]
            suffixes.add(suffix)

    # Create entries for each suffix
    for suffix in suffixes:
        prefix = f"OPENAI_COMPATIBLE_{suffix}" if suffix else "OPENAI_COMPATIBLE"
        key = f"openai_compatible_{suffix.lower()}" if suffix else "openai_compatible"

        api_key_var = f"{prefix}_API_KEY"
        model_id_var = f"{prefix}_MODEL_ID"
        base_url_var = f"{prefix}_BASE_URL"

        if _check_env(api_key_var, model_id_var, base_url_var):
            variants[key] = (api_key_var, model_id_var, base_url_var)

    return variants


def _get_available_llms() -> dict[str, LLM | AsyncOpenAI]:
    """Dynamically compute available LLMs based on environment configuration."""
    llms: dict[str, LLM | AsyncOpenAI] = {}

    # Gemini LLM
    if os.getenv("GEMINI_API_KEY"):
        gemini_llm = LLM(
            model=os.getenv("GEMINI_MODEL_ID", "gemini/gemini-2.0-flash"),
            temperature=0.7,
        )
        llms["gemini"] = gemini_llm

    # Ollama LLM
    if _check_env("OLLAMA_MODEL_ID"):
        llms["ollama_llm"] = LLM(
            model=os.getenv("OLLAMA_MODEL_ID"),
            base_url="http://localhost:11434",
        )

    # OpenAI LLM
    if _check_env("OPENAI_API_KEY"):
        openai_llm = LLM(
            model=os.getenv("OPENAI_MODEL_ID", "openai/gpt-4"),
            temperature=0.8,
            top_p=0.9,
            stop=["END"],
            api_key=os.getenv("OPENAI_API_KEY"),
            seed=42,
        )
        llms["openai_llm"] = openai_llm
        llms["openai"] = openai_llm

    # OpenAI Compatible LLMs (supports multiple variants)
    for key, (
        api_key_var,
        model_id_var,
        base_url_var,
    ) in _get_openai_compatible_variants().items():
        llm = LLM(
            model=os.getenv(model_id_var),
            temperature=0.8,
            top_p=0.9,
            api_key=os.getenv(api_key_var),
            base_url=os.getenv(base_url_var),
            seed=42,
        )
        llms[f"{key}_llm"] = llm
        llms[key] = llm

    # WatsonX LLM
    if _check_env("WATSONX_APIKEY", "WATSONX_URL", "WATSONX_PROJECTID", "MODEL_ID"):
        watsonx_llm = LLM(
            model=os.getenv("MODEL_ID"),
            base_url=os.getenv("WATSONX_URL"),
            project_id=os.getenv("WATSONX_PROJECTID"),
            api_key=os.getenv("WATSONX_APIKEY"),
            temperature=0,
            max_tokens=4000,
            max_input_tokens=100000,
        )
        llms["watsonx_llm"] = watsonx_llm
        llms["watsonx"] = watsonx_llm

    # VLLM (AsyncOpenAI)
    if _check_env("VLLM_URL"):
        llms["vllm_llm"] = AsyncOpenAI(
            api_key="EMPTY",
            base_url=os.getenv("VLLM_URL"),
            default_headers={"Content-Type": "application/json"},
        )

    # VLLM (CrewAI)
    if _check_env("VLLM_URL", "VLLM_MODEL_ID"):
        llms["vllm_crewai"] = LLM(
            model=os.getenv("VLLM_MODEL_ID"),
            api_key="EMPTY",
            base_url=os.getenv("VLLM_URL"),
            max_tokens=1000,
            temperature=0.0,
        )

    # LiteLLM (100+ providers via CrewAI's native support)
    # CrewAI natively supports LiteLLM. Use model format: "litellm/provider/model-name"
    # or just use the model name directly if API key is in env
    if _check_env("LITELLM_MODEL"):
        model_name = os.getenv("LITELLM_MODEL")
        # If not already prefixed with litellm/, add it
        if not model_name.startswith("litellm/"):
            model_name = f"litellm/{model_name}"

        litellm_llm = LLM(
            model=model_name,
            temperature=float(os.getenv("LITELLM_TEMPERATURE", "0.8")),
            top_p=float(os.getenv("LITELLM_TOP_P", "0.9")),
        )
        llms["litellm"] = litellm_llm

    return llms


def __getattr__(name: str) -> dict[str, LLM | AsyncOpenAI] | LLM | AsyncOpenAI | None:
    """
    Module-level attribute access for backward compatibility.

    Allows accessing 'available_llms' and individual LLM variables dynamically.
    """
    if name == "available_llms":
        return _get_available_llms()

    llms = _get_available_llms()
    if name in llms:
        return llms[name]

    # Allow graceful access to known LLM patterns that might not be configured
    known_prefixes = (
        "openai_compatible",
        "watsonx",
        "gemini",
        "openai",
        "vllm",
        "ollama",
        "litellm",
    )
    if any(name.startswith(prefix) for prefix in known_prefixes):
        return None

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
