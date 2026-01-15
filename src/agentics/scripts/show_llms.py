#!/usr/bin/env python3
"""Script to display all available LLMs configured in the environment using a rich table."""

import os
from typing import Any

from rich.console import Console
from rich.table import Table
from rich.text import Text

from agentics.core.llm_connections import (
    get_available_llms,
    get_llm_provider,
    get_llms_env_vars,
)


def _check_api_key_format(api_key: str | None) -> bool:
    """Check if an API key exists and has a reasonable format."""
    if not api_key:
        return False
    # Basic validation: API keys should be non-empty and reasonably long
    return len(api_key) > 4


def _check_openai_auth(llm_obj: Any) -> str:
    """Check OpenAI authentication status."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not _check_api_key_format(api_key):
        return "[bold red]✗ NO AUTH[/bold red]"
    return "[bold green]✓ AUTHENTICATED[/bold green]"


def _check_google_auth(llm_obj: Any) -> str:
    """Check Google/Gemini authentication status."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not _check_api_key_format(api_key):
        return "[bold red]✗ NO AUTH[/bold red]"
    return "[bold green]✓ AUTHENTICATED[/bold green]"


def _check_litellm_auth(llm_obj: Any) -> str:
    """Check LiteLLM authentication status."""
    model = os.getenv("LITELLM_MODEL")
    if not model:
        return "[bold red]✗ NO AUTH[/bold red]"

    # Try to extract provider from model name
    provider = model.split("/")[0] if "/" in model else model

    # Check if we have required auth for the provider
    # Most providers require an API key in environment
    required_keys = [
        f"{provider.upper()}_API_KEY",
        "LITELLM_API_KEY",
    ]

    for key_name in required_keys:
        if _check_api_key_format(os.getenv(key_name)):
            return "[bold green]✓ AUTHENTICATED[/bold green]"

    return "[bold yellow]⚠ PARTIAL[/bold yellow]"


def _check_litellm_proxy_auth(llm_obj: Any) -> str:
    """Check LiteLLM Proxy authentication status."""
    api_key = os.getenv("LITELLM_PROXY_API_KEY")
    base_url = os.getenv("LITELLM_PROXY_URL")

    if not _check_api_key_format(api_key):
        return "[bold red]✗ NO AUTH[/bold red]"
    if not base_url:
        return "[bold yellow]⚠ PARTIAL[/bold yellow]"

    return "[bold green]✓ AUTHENTICATED[/bold green]"


def _check_watsonx_auth(llm_obj: Any) -> str:
    """Check WatsonX authentication status."""
    required_vars = ["WATSONX_APIKEY", "WATSONX_URL", "WATSONX_PROJECTID"]
    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        return "[bold red]✗ NO AUTH[/bold red]"

    api_key = os.getenv("WATSONX_APIKEY")
    if not _check_api_key_format(api_key):
        return "[bold red]✗ NO AUTH[/bold red]"

    return "[bold green]✓ AUTHENTICATED[/bold green]"


def _get_auth_status(llm_name: str, llm_obj: Any) -> str:
    """Get authentication status for an LLM based on its type."""
    provider = _get_provider_name(llm_name, llm_obj)

    # Check specific providers
    if "gemini" in llm_name.lower():
        return _check_google_auth(llm_obj)
    elif "openai" in llm_name.lower() and "compatible" not in llm_name.lower():
        return _check_openai_auth(llm_obj)
    elif "litellm_proxy" in llm_name.lower():
        return _check_litellm_proxy_auth(llm_obj)
    elif "litellm" in llm_name.lower():
        return _check_litellm_auth(llm_obj)
    elif "watsonx" in llm_name.lower():
        return _check_watsonx_auth(llm_obj)
    elif "vllm" in llm_name.lower():
        # vLLM requires URL
        base_url = os.getenv("VLLM_URL")
        if base_url:
            return "[bold green]✓ AUTHENTICATED[/bold green]"
        return "[bold red]✗ NO AUTH[/bold red]"
    elif "ollama" in llm_name.lower():
        # Ollama requires model ID
        model_id = os.getenv("OLLAMA_MODEL_ID")
        if model_id:
            return "[bold green]✓ AUTHENTICATED[/bold green]"
        return "[bold red]✗ NO AUTH[/bold red]"
    elif "openai_compatible" in llm_name.lower():
        # OpenAI compatible requires API key and URL
        api_key = os.getenv("OPENAI_COMPATIBLE_API_KEY")
        base_url = os.getenv("OPENAI_COMPATIBLE_BASE_URL")
        if _check_api_key_format(api_key) and base_url:
            return "[bold green]✓ AUTHENTICATED[/bold green]"
        return "[bold red]✗ NO AUTH[/bold red]"

    return "[bold yellow]⚠ UNKNOWN[/bold yellow]"


def _get_provider_name(llm_name: str, llm_obj) -> str:
    """Extract provider name from LLM name and object."""
    # Map based on LLM name patterns
    if "gemini" in llm_name.lower():
        return "Gemini"
    elif "openai_compatible" in llm_name.lower():
        return "OpenAI Compatible"
    elif "openai" in llm_name.lower():
        return "OpenAI"
    elif "watsonx" in llm_name.lower():
        return "WatsonX"
    elif "vllm" in llm_name.lower():
        if "crewai" in llm_name.lower():
            return "vLLM (CrewAI)"
        return "vLLM (AsyncOpenAI)"
    elif "ollama" in llm_name.lower():
        return "Ollama"
    elif "litellm_proxy" in llm_name.lower():
        return "LiteLLM Proxy"
    elif "litellm" in llm_name.lower():
        return "LiteLLM"
    else:
        # Fallback to class name
        return type(llm_obj).__name__


def _get_model_info(llm_obj) -> str:
    """Extract model information from LLM object."""
    if hasattr(llm_obj, "model"):
        model = llm_obj.model
        if isinstance(model, str):
            # Extract just the model name for display
            if "/" in model:
                return model.split("/")[-1]
            return model
    return "N/A"


def main() -> None:
    """Display available LLMs in a rich table with the active one highlighted."""
    console = Console()
    llms = get_available_llms()
    llms_env_vars = get_llms_env_vars()

    if not llms:
        console.print(
            "[yellow]No LLMs are currently configured.[/yellow]\n"
            "Please configure at least one LLM by setting the required environment variables."
        )
        return

    # Group aliases by LLM instance
    llm_groups: dict[int, list[str]] = {}  # Map of id(llm) -> [names]
    for name, llm in llms.items():
        llm_id = id(llm)
        if llm_id not in llm_groups:
            llm_groups[llm_id] = []
        llm_groups[llm_id].append(name)

    # Get the default/active LLM
    active_llm = get_llm_provider()
    active_llm_name = None
    active_provider = None
    active_model = None

    if active_llm:
        for name, llm in llms.items():
            if llm is active_llm:
                active_llm_name = name
                active_provider = _get_provider_name(name, llm)
                active_model = _get_model_info(llm)
                break

    # Create table
    table = Table(title="Available LLMs", show_header=True, header_style="bold magenta")
    table.add_column("LLM Names", style="cyan")
    table.add_column("Provider", style="green")
    table.add_column("Model", style="blue")
    table.add_column("Environment Variables", style="yellow")
    table.add_column("Status", style="yellow")

    # Track which instances we've already displayed
    displayed_instances = set()

    for name, llm in sorted(llms.items()):
        llm_id = id(llm)

        # Skip if we've already displayed this instance
        if llm_id in displayed_instances:
            continue

        displayed_instances.add(llm_id)

        # Get all aliases for this instance
        aliases = sorted(llm_groups[llm_id])
        names_str = ", ".join(aliases)

        provider = _get_provider_name(name, llm)
        model = _get_model_info(llm)
        env_vars = llms_env_vars.get(name, [])
        # Filter to only show URL, model, and API key related variables
        filtered_vars = [
            var
            for var in env_vars
            if any(
                keyword in var.lower() for keyword in ["url", "model", "api_key", "key"]
            )
        ]
        env_vars_str = ", ".join(filtered_vars) if filtered_vars else "N/A"
        is_active = name == active_llm_name

        auth_status = _get_auth_status(name, llm)
        active_indicator = " ● ACTIVE" if is_active else ""
        status = f"{auth_status}{active_indicator}"
        table.add_row(names_str, provider, model, env_vars_str, status)

    # Add active LLM footer row spanning all columns
    if active_llm_name:
        active_text = Text()
        active_text.append("🎯 Active LLM: ", style="bold yellow")
        active_text.append(active_llm_name, style="bold cyan")
        active_text.append(" | ", style="yellow")
        active_text.append(active_provider, style="bold green")
        active_text.append(" | ", style="yellow")
        active_text.append(active_model, style="bold blue")
        table.add_row(active_text, "", "", "", style="bold yellow on blue")

    console.print(table)

    # Calculate statistics
    num_instances = len(llm_groups)
    num_aliases = len(llms) - num_instances

    console.print(f"\n[bold]Total:[/bold] {num_instances} LLM instance(s) configured")
    if num_aliases > 0:
        console.print(f"[bold]Aliases:[/bold] {num_aliases} additional name(s)")


if __name__ == "__main__":
    main()
