#!/usr/bin/env python3
"""Script to display all available LLMs configured in the environment using a rich table."""

from rich.console import Console
from rich.table import Table
from rich.text import Text

from agentics.core.llm_connections import (
    get_available_llms,
    get_llm_provider,
    get_llms_env_vars,
)


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
        env_vars_str = ", ".join(env_vars) if env_vars else "N/A"
        is_active = name == active_llm_name

        status = "[bold green]● ACTIVE[/bold green]" if is_active else ""
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
