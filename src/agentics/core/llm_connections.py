import os

from crewai import LLM
from dotenv import load_dotenv
from loguru import logger
from openai import AsyncOpenAI

load_dotenv()

# Track which environment variables are used for each LLM
_llms_env_vars: dict[str, list[str]] = {}

# Cache for available LLMs (computed once at first use)
_available_llms_cache: dict[str, LLM | AsyncOpenAI] | None = None


def get_llm_provider(provider_name: str | None = None) -> LLM | AsyncOpenAI | None:
    """
    Retrieve the LLM instance based on the provider name. If no provider name is given,
    the function returns the first available LLM.

    Args:
        provider_name (str): The name of the LLM provider (e.g., 'openai', 'watsonx', 'gemini').

    Returns:
        LLM | AsyncOpenAI | None: The corresponding LLM instance.
    """
    llms = _get_cached_available_llms()

    if not provider_name:
        if llms:  # Not empty
            logger.trace(
                f"Available LLM providers: {list(llms)}. None specified, defaulting to '{list(llms)[0]}'"
            )
            first_provider = next((iter(llms.values())))
            return first_provider
        else:
            logger.trace("No LLM is available. Please check your .env configuration.")
            return None

    if provider_name in llms:
        logger.trace(f"Using specified LLM provider: {provider_name}")
        return llms[provider_name]

    logger.debug(
        f"LLM provider '{provider_name}' is not available. Please check your .env configuration."
    )
    return None


def _check_env(*var_names: str) -> bool:
    """Check if all given environment variables are non-empty."""
    return all(os.getenv(var) for var in var_names)


def _get_cached_available_llms() -> dict[str, LLM | AsyncOpenAI]:
    """
    Get cached LLMs or compute and cache them on first call.
    
    This avoids repeatedly scanning environment variables on every access.
    Call refresh_llm_cache() if you need to reload the configuration.
    """
    global _available_llms_cache
    if _available_llms_cache is None:
        _available_llms_cache = get_available_llms()
    return _available_llms_cache


def refresh_llm_cache() -> dict[str, LLM | AsyncOpenAI]:
    """
    Force refresh the LLM cache.
    
    Call this if environment variables change at runtime.
    """
    global _available_llms_cache
    _available_llms_cache = None
    return _get_cached_available_llms()


def _get_llm_params(model: str) -> dict:
    """
    Get provider-specific LLM parameters based on the model name.

    Some providers have constraints (e.g., Claude doesn't allow both temperature and top_p).

    Args:
        model: The model identifier (e.g., "aws/claude-haiku-4-5", "gpt-4")

    Returns:
        dict: LLM parameters with provider-specific constraints applied
    """
    params: dict = {
        "temperature": 0.8,
        "top_p": 0.9,
    }

    # Claude models don't support both temperature and top_p together
    if "claude" in model.lower():
        # For Claude, only use temperature, remove top_p
        params.pop("top_p", None)
        params["temperature"] = 0.7

    return params


def get_llms_env_vars() -> dict[str, list[str]]:
    """
    Get the environment variables used for each LLM.

    Returns:
        dict[str, list[str]]: A mapping of LLM names to the env vars used to configure them.
    """
    return _llms_env_vars.copy()


def get_available_llms() -> dict[str, LLM | AsyncOpenAI]:
    """Dynamically compute available LLMs based on environment configuration."""
    llms: dict[str, LLM | AsyncOpenAI] = {}
    _llms_env_vars.clear()

    # Gemini LLM
    if os.getenv("GEMINI_API_KEY"):
        llms["gemini"] = LLM(
            model=os.getenv("GEMINI_MODEL_ID", "gemini/gemini-2.0-flash"),
            temperature=0.7,
        )
        _llms_env_vars["gemini"] = ["GEMINI_API_KEY", "GEMINI_MODEL_ID"]
        logger.debug(f"Registered LLM provider: gemini")

    # Ollama LLM
    if _check_env("OLLAMA_MODEL_ID"):
        llms["ollama_llm"] = LLM(
            model=os.getenv("OLLAMA_MODEL_ID"),
            base_url="http://localhost:11434",
        )
        _llms_env_vars["ollama_llm"] = ["OLLAMA_MODEL_ID"]
        logger.debug(f"Registered LLM provider: ollama_llm")

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
        env_vars = ["OPENAI_API_KEY", "OPENAI_MODEL_ID"]
        _llms_env_vars["openai_llm"] = env_vars
        _llms_env_vars["openai"] = env_vars
        logger.debug(f"Registered LLM provider: openai_llm")
        logger.debug(f"Registered LLM provider: openai")

    # OpenAI Compatible LLM
    if _check_env(
        "OPENAI_COMPATIBLE_API_KEY",
        "OPENAI_COMPATIBLE_MODEL_ID",
        "OPENAI_COMPATIBLE_BASE_URL",
    ):
        openai_compatible_llm = LLM(
            model=os.getenv("OPENAI_COMPATIBLE_MODEL_ID"),
            temperature=0.8,
            top_p=0.9,
            api_key=os.getenv("OPENAI_COMPATIBLE_API_KEY"),
            base_url=os.getenv("OPENAI_COMPATIBLE_BASE_URL"),
            seed=42,
        )
        llms["openai_compatible_llm"] = openai_compatible_llm
        llms["openai_compatible"] = openai_compatible_llm
        env_vars = [
            "OPENAI_COMPATIBLE_API_KEY",
            "OPENAI_COMPATIBLE_MODEL_ID",
            "OPENAI_COMPATIBLE_BASE_URL",
        ]
        _llms_env_vars["openai_compatible_llm"] = env_vars
        _llms_env_vars["openai_compatible"] = env_vars
        logger.debug(f"Registered LLM provider: openai_compatible_llm")
        logger.debug(f"Registered LLM provider: openai_compatible")

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
        env_vars = ["WATSONX_APIKEY", "WATSONX_URL", "WATSONX_PROJECTID", "MODEL_ID"]
        _llms_env_vars["watsonx_llm"] = env_vars
        _llms_env_vars["watsonx"] = env_vars
        logger.debug(f"Registered LLM provider: watsonx_llm")
        logger.debug(f"Registered LLM provider: watsonx")

    # VLLM (AsyncOpenAI)
    if _check_env("VLLM_URL"):
        llms["vllm_llm"] = AsyncOpenAI(
            api_key="EMPTY",
            base_url=os.getenv("VLLM_URL"),
            default_headers={"Content-Type": "application/json"},
        )
        _llms_env_vars["vllm_llm"] = ["VLLM_URL"]

    # VLLM (CrewAI)
    if _check_env("VLLM_URL", "VLLM_MODEL_ID"):
        llms["vllm_crewai"] = LLM(
            model=os.getenv("VLLM_MODEL_ID"),
            api_key="EMPTY",
            base_url=os.getenv("VLLM_URL"),
            max_tokens=1000,
            temperature=0.0,
        )
        _llms_env_vars["vllm_crewai"] = ["VLLM_URL", "VLLM_MODEL_ID"]

    # LiteLLM (100+ providers via CrewAI's native support)
    # CrewAI natively supports LiteLLM. Use model format: "litellm/provider/model-name"
    # or just use the model name directly if API key is in env
    if _check_env("LITELLM_MODEL"):
        model_name = os.getenv("LITELLM_MODEL")
        # If not already prefixed with litellm/, add it
        if not model_name.startswith("litellm/"):
            model_name = f"litellm/{model_name}"

        # Get provider-specific parameters
        litellm_params = _get_llm_params(model_name)

        # Override with env vars if present
        if os.getenv("LITELLM_TEMPERATURE"):
            litellm_params["temperature"] = float(os.getenv("LITELLM_TEMPERATURE"))
        if os.getenv("LITELLM_TOP_P") and "top_p" in litellm_params:
            litellm_params["top_p"] = float(os.getenv("LITELLM_TOP_P"))

        litellm_llm = LLM(
            model=model_name,
            **litellm_params,
        )
        llms["litellm"] = litellm_llm
        _llms_env_vars["litellm"] = [
            "LITELLM_MODEL",
            "LITELLM_TEMPERATURE",
            "LITELLM_TOP_P",
        ]
        logger.debug(f"Registered LLM provider: litellm")

    # LiteLLM Proxy
    if _check_env("LITELLM_PROXY_URL", "LITELLM_PROXY_API_KEY", "LITELLM_PROXY_MODEL"):
        proxy_model = os.getenv("LITELLM_PROXY_MODEL")
        # Validate that model name starts with litellm_proxy/
        if not proxy_model.startswith("litellm_proxy/"):
            logger.warning(
                f"LITELLM_PROXY_MODEL '{proxy_model}' does not start with 'litellm_proxy/'. "
                "Skipping LiteLLM Proxy configuration. "
                "Please set LITELLM_PROXY_MODEL to a value like 'litellm_proxy/<name>'."
            )
        else:
            # Get provider-specific parameters
            proxy_params = _get_llm_params(proxy_model)

            # Override with env vars if present
            if os.getenv("LITELLM_PROXY_TEMPERATURE"):
                proxy_params["temperature"] = float(
                    os.getenv("LITELLM_PROXY_TEMPERATURE")
                )
            if os.getenv("LITELLM_PROXY_TOP_P") and "top_p" in proxy_params:
                proxy_params["top_p"] = float(os.getenv("LITELLM_PROXY_TOP_P"))

            litellm_proxy_llm = LLM(
                model=proxy_model,
                api_key=os.getenv("LITELLM_PROXY_API_KEY"),
                base_url=os.getenv("LITELLM_PROXY_URL"),
                **proxy_params,
            )
            llms["litellm_proxy_llm"] = litellm_proxy_llm
            llms["litellm_proxy"] = litellm_proxy_llm
            env_vars = [
                "LITELLM_PROXY_URL",
                "LITELLM_PROXY_API_KEY",
                "LITELLM_PROXY_MODEL",
                "LITELLM_PROXY_TEMPERATURE",
                "LITELLM_PROXY_TOP_P",
            ]
            _llms_env_vars["litellm_proxy_llm"] = env_vars
            _llms_env_vars["litellm_proxy"] = env_vars

    # LiteLLM Proxy - Multiple Numbered Configurations (for parallel experiments)
    # Scan for LITELLM_PROXY_1_*, LITELLM_PROXY_2_*, etc.
    # Supports up to 10 numbered configurations (1-10)
    for i in range(1, 11):
        url_key = f"LITELLM_PROXY_{i}_URL"
        api_key_key = f"LITELLM_PROXY_{i}_API_KEY"
        model_key = f"LITELLM_PROXY_{i}_MODEL"
        temp_key = f"LITELLM_PROXY_{i}_TEMPERATURE"
        top_p_key = f"LITELLM_PROXY_{i}_TOP_P"

        if _check_env(url_key, api_key_key, model_key):
            proxy_model = os.getenv(model_key)
            # Validate that model name starts with litellm_proxy/
            if not proxy_model.startswith("litellm_proxy/"):
                logger.warning(
                    f"{model_key} '{proxy_model}' does not start with 'litellm_proxy/'. "
                    f"Skipping LiteLLM Proxy configuration #{i}. "
                    f"Please set {model_key} to a value like 'litellm_proxy/<name>'."
                )
                continue

            # Get provider-specific parameters
            proxy_params = _get_llm_params(proxy_model)

            # Override with env vars if present
            if os.getenv(temp_key):
                proxy_params["temperature"] = float(os.getenv(temp_key))
            if os.getenv(top_p_key) and "top_p" in proxy_params:
                proxy_params["top_p"] = float(os.getenv(top_p_key))

            litellm_proxy_llm = LLM(
                model=proxy_model,
                api_key=os.getenv(api_key_key),
                base_url=os.getenv(url_key),
                **proxy_params,
            )
            # Register with both numbered and short names for convenience
            provider_name = f"litellm_proxy_{i}"
            llms[provider_name] = litellm_proxy_llm
            env_vars = [url_key, api_key_key, model_key, temp_key, top_p_key]
            _llms_env_vars[provider_name] = env_vars
            logger.debug(f"Registered LLM provider: {provider_name}")

    return llms


def __getattr__(name: str) -> dict[str, LLM | AsyncOpenAI] | LLM | AsyncOpenAI | None:
    """
    Module-level attribute access for backward compatibility.

    Allows accessing 'available_llms' and individual LLM variables dynamically.
    """
    if name == "available_llms":
        return _get_cached_available_llms()

    llms = _get_cached_available_llms()
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
        "litellm_proxy",
    )
    if any(name.startswith(prefix) for prefix in known_prefixes):
        return None

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
