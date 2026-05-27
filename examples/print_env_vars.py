"""
Print environment variables related to Agentics and LLM configuration.
This helps debug configuration issues.
"""

import os

from dotenv import load_dotenv

# Load .env file
load_dotenv()

print("=" * 80)
print("AGENTICS ENVIRONMENT VARIABLES")
print("=" * 80)

# Define categories of environment variables to check
env_categories = {
    "Agentics Configuration": [
        "AGENTICS_TRACE_MODE",
        "SELECTED_LLM",
    ],
    "OpenAI": [
        "OPENAI_API_KEY",
        "OPENAI_MODEL_ID",
        "OPENAI_BASE_URL",
    ],
    "WatsonX": [
        "WATSONX_APIKEY",
        "WATSONX_PROJECTID",
        "WATSONX_URL",
        "MODEL_ID",
    ],
    "Gemini": [
        "GEMINI_API_KEY",
        "GOOGLE_API_KEY",
        "GEMINI_MODEL_ID",
    ],
    "Anthropic": [
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_MODEL_ID",
    ],
    "Ollama": [
        "OLLAMA_MODEL_ID",
        "OLLAMA_TURBO_API_KEY",
    ],
    "VLLM": [
        "VLLM_URL",
        "VLLM_MODEL_ID",
    ],
    "LiteLLM": [
        "LITELLM_MODEL",
        "LITELLM_TEMPERATURE",
        "LITELLM_TOP_P",
    ],
    "LiteLLM Proxy": [
        "LITELLM_PROXY_URL",
        "LITELLM_PROXY_API_KEY",
        "LITELLM_PROXY_MODEL",
    ],
    "CrewAI": [
        "CREWAI_DISABLE_TELEMETRY",
        "CREWAI_TRACING_ENABLED",
        "OTEL_SDK_DISABLED",
    ],
    "Kafka/Streaming": [
        "KAFKA_SERVER",
        "KAFKA_INPUT_TOPIC",
        "KAFKA_OUTPUT_TOPIC",
        "AGSTREAM_BACKENDS",
        "AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL",
    ],
}


def mask_sensitive(key: str, value: str) -> str:
    """Mask sensitive values like API keys"""
    sensitive_keywords = ["key", "apikey", "password", "secret", "token"]

    if any(keyword in key.lower() for keyword in sensitive_keywords):
        if value and len(value) > 8:
            # Show first 4 and last 4 characters
            return f"{value[:4]}...{value[-4:]}"
        elif value:
            return "***"
    return value


# Print each category
for category, vars_list in env_categories.items():
    print(f"\n{category}")
    print("-" * 80)

    found_any = False
    for var_name in vars_list:
        value = os.getenv(var_name)
        if value is not None:
            masked_value = mask_sensitive(var_name, value)
            print(f"  ✓ {var_name:40} = {masked_value}")
            found_any = True
        else:
            print(f"    {var_name:40} = (not set)")

    if not found_any:
        print(f"  (No {category} variables configured)")

# Print all environment variables (optional, commented out by default)
print("\n" + "=" * 80)
print("ALL ENVIRONMENT VARIABLES (filtered)")
print("=" * 80)
print(
    "\nShowing only variables containing: agentics, llm, openai, watsonx, gemini, kafka"
)
print("-" * 80)

all_vars = sorted(os.environ.items())
filtered_keywords = [
    "agentics",
    "llm",
    "openai",
    "watsonx",
    "gemini",
    "kafka",
    "anthropic",
    "ollama",
    "vllm",
    "litellm",
    "crewai",
]

for key, value in all_vars:
    if any(keyword in key.lower() for keyword in filtered_keywords):
        masked_value = mask_sensitive(key, value)
        print(f"  {key:40} = {masked_value}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

# Determine which LLM is configured
selected_llm = os.getenv("SELECTED_LLM", "auto-detect")
print(f"\nSELECTED_LLM: {selected_llm}")

# Check which LLMs are available
available_llms = []
if os.getenv("OPENAI_API_KEY"):
    available_llms.append("OpenAI")
if os.getenv("WATSONX_APIKEY"):
    available_llms.append("WatsonX")
if os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"):
    available_llms.append("Gemini")
if os.getenv("ANTHROPIC_API_KEY"):
    available_llms.append("Anthropic")
if os.getenv("OLLAMA_MODEL_ID"):
    available_llms.append("Ollama")
if os.getenv("VLLM_URL"):
    available_llms.append("VLLM")
if os.getenv("LITELLM_MODEL"):
    available_llms.append("LiteLLM")
if os.getenv("LITELLM_PROXY_URL"):
    available_llms.append("LiteLLM Proxy")

print(
    f"\nAvailable LLM Providers: {', '.join(available_llms) if available_llms else 'None detected'}"
)

if not available_llms:
    print("\n⚠️  WARNING: No LLM providers detected!")
    print("Please configure at least one LLM provider in your .env file.")
else:
    print(f"\n✓ {len(available_llms)} LLM provider(s) configured")

print("\n" + "=" * 80)

# Made with Bob
