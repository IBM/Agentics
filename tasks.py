#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "invoke-toolkit==0.0.42",
# ]
# ///
#
# This task file can be run with
#

import ast
import functools
import getpass
import json
import subprocess
import tempfile
from logging import getLogger
from pathlib import Path
from shutil import which
from typing import Callable

logger = getLogger(__name__)
from invoke_toolkit import Context, script, task

toplvel = subprocess.check_output("git rev-parse --show-toplevel", shell=True).decode()


class EnvDict:
    """Dictionary-like interface for .env files that preserves comments."""

    def __init__(self, env_file: str):
        """Initialize EnvDict with a file path."""
        self.env_file = env_file
        self.path = Path(env_file)
        self._load()

    def _load(self) -> None:
        """Load the env file into memory."""
        if not self.path.exists():
            self._lines = []
            self._data = {}
            return

        self._lines = self.path.read_text().splitlines(keepends=True)
        self._data = {}

        # Parse existing non-commented variables
        for line in self._lines:
            stripped = line.lstrip()
            if not stripped.startswith("#") and "=" in stripped:
                key, value = stripped.split("=", 1)
                # Remove quotes if present
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                self._data[key.strip()] = value.rstrip("\n")

    def _save(self) -> None:
        """Save changes back to the env file."""
        self.path.write_text("".join(self._lines))

    def __getitem__(self, key: str) -> str:
        """Get a value by key, raise KeyError if not found."""
        return self._data[key]

    def __setitem__(self, key: str, value: str) -> None:
        """Set a value, updating the file."""
        self.set(key, value)

    def get(self, key: str, default=None) -> str | None:
        """Get a value safely, returning default if not found."""
        value = self._data.get(key, default)
        if value is not None:
            try:
                return ast.literal_eval(value)
            except (ValueError, SyntaxError):
                return value
        return default

    def setdefault(self, key: str, default: str) -> str:
        """Set default value if key doesn't exist, return the value."""
        if key in self._data:
            return self._data[key]
        self.set(key, default)
        return default

    def set(self, key: str, value: str) -> bool:
        """
        Set a key-value pair, preserving comments and structure.

        Returns:
            bool: True if the file was modified, False if value was unchanged
        """
        # Check if key already exists
        key_index = None
        for i, line in enumerate(self._lines):
            stripped = line.lstrip()
            if not stripped.startswith("#") and stripped.startswith(f"{key}="):
                key_index = i
                break

        old_value = self._data.get(key)

        if old_value == value:
            return False  # No change needed

        self._data[key] = value

        if key_index is not None:
            # Update existing line, preserving indentation
            indent = len(self._lines[key_index]) - len(self._lines[key_index].lstrip())
            self._lines[key_index] = " " * indent + f'{key}="{value}"\n'
        else:
            # Append new line
            if self._lines and not self._lines[-1].endswith("\n"):
                self._lines[-1] += "\n"
            self._lines.append(f'{key}="{value}"\n')

        self._save()
        return True

    def __contains__(self, key: str) -> bool:
        """Check if a key exists."""
        return key in self._data

    def keys(self):
        """Return all keys."""
        return self._data.keys()

    def values(self):
        """Return all values."""
        return self._data.values()

    def items(self):
        """Return all key-value pairs."""
        return self._data.items()

    def __repr__(self) -> str:
        """Return string representation."""
        return f"EnvDict({self._data})"


@task(
    help={
        "container_engine": "Which container to use [green]docker[/green], [yellow]podman[/yellow] or [red]nerdctl[/red]"
    }
)
def test_in_isolation(
    ctx: Context,
    container_engine: str = "docker",
    # image="ghcr.io/astral-sh/uv:alpine",
    image="ghcr.io/astral-sh/uv:python3.12-trixie",
    volumes: list[str] = [],
    workdir: str = "/code",
    cmd: str = "uv run pytest ",
    platform: str = "linux/amd64",
    user: str = "root",
):
    top_level = ctx.run("git rev-parse --show-toplevel", hide=True).stdout.strip()
    with ctx.cd(top_level):
        name = Path(top_level).name
        name = name.replace(".", "_")
        volumes.append("$PWD:/code")
        volumes.append(f"{name}_venv:/code/.venv")
        volumes.append(f"{name}_cache:/{user}/.cache")
        vols = " ".join(f"-v {v}" for v in volumes)
        ctx.run(
            f"{container_engine} run {vols} -w {workdir} --platform {platform} "
            f"--rm -ti {image} {cmd}",
            pty=True,
        )


@task()
def update_docs(ctx: Context, remote_name: str = ""):
    """Updates Github contents


    Supports multiple remote names
    """

    name_remote_url: str = ctx.run(
        "git remote -v | awk '{print $1, $2}' | uniq", hide=True
    ).stdout
    remote_map = dict(line.split(" ") for line in name_remote_url.splitlines())
    if remote_name and not remote_name in remote_map:
        ctx.rich_exit(
            f"Can't find [red]{remote_name}[/red] in the remotes, try running "
            + f"[green]git remote -v[/green] and checking if {remote_name} is present"
        )
    try:
        remote_name = next(
            name
            for name, url in remote_map.items()
            if url.startswith("git@") and "github.com" in url
        )
    except StopIteration:
        ctx.rich_exit(
            "Can't find [bold]github.com[/bold] in the remotes, try running "
            + "[green]git remote -v[/green] and checking if github.com is present"
        )
    with ctx.status(f"Updating Github pages for remote {remote_name}"):
        ctx.run(f"uv run --group docs mkdocs gh-deploy -r {remote_name}", pty=True)


@task()
def serve_docs(ctx: Context):
    """Serve the docs"""

    with ctx.status("Running mkdocs serve"):
        ctx.run("uv run --group docs mkdocs serve", pty=True)


def _extract_base_url(url: str) -> str:
    """
    Extract base URL from a URL by removing path components.

    Args:
        url: The full URL (e.g., "https://example.com/api/v1")

    Returns:
        str: The base URL (e.g., "https://example.com")
    """
    from urllib.parse import urlparse

    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    return base_url


def _validate_api_key(ctx: Context, url: str, api_key: str) -> bool:
    """
    Validate API key by making a test request to the proxy.

    Args:
        ctx: The invoke Context
        url: The LiteLLM proxy URL
        api_key: The API key to validate

    Returns:
        bool: True if valid, False if 4xx error
    """
    logger.debug(f"{api_key!r} {url!r}")
    try:
        api_url = f"{url.rstrip('/')}/models"
        result = ctx.run(
            f"curl -s -o /dev/null -w '%{{http_code}}' -H 'Authorization: Bearer {api_key}' '{api_url}'",
            warn=True,
            hide=True,
        )
        if result.ok:
            status_code = int(result.stdout.strip())
            if 400 <= status_code < 500:
                return False
            return 200 <= status_code < 300
        return False
    except Exception:
        return False


def prompt_user(
    ctx: Context,
    value_name: str,
    initial_value: str | None,
    validation_function: Callable[..., bool] | None = None,
    echo: bool = False,
) -> tuple[bool, str]:
    """
    Prompt user for a value with optional validation.

    Args:
        ctx: The invoke Context for printing messages
        value_name: Name of the value being prompted for (used in messages)
        initial_value: Existing value if any (empty string if not set)
        validation_function: Optional function to validate user input
        echo: If True, show input as it's typed; if False, hide it (for sensitive data)

    Returns:
        tuple[bool, str]: (changed, value) where changed indicates if the value was modified
    """
    if initial_value:
        while True:
            message = f"[green]{value_name}[/] already present. Type [yellow]new value[/] or [bold]enter[/] to keep it:"
            ctx.print(message)
            if echo:
                user_input = input("->").strip()
            else:
                user_input = getpass.getpass("->").strip()
            if not user_input:
                ctx.print(
                    f"Keeping existing value of [green]{value_name}[/green] ✅",
                    initial_value if echo else "",
                )
                return False, initial_value
            elif validation_function is not None:
                if validation_function(user_input):
                    return True, user_input
    else:
        while True:
            message = f"Please provide the value for [yellow]{value_name}[/]:"
            ctx.print(message)
            if echo:
                user_input = input("> ").strip()
            else:
                user_input = getpass.getpass(">").strip()
            if validation_function is not None:
                if validation_function(user_input):
                    return True, user_input
                else:
                    ctx.print("The supplied value is not valid")
            else:
                return True, user_input


@task(aliases=["setup"])
def setup_litellm(ctx: Context, env_file: str = ".env", query: str = ""):
    """Setup LiteLLM environment variables"""
    if not which("fzf"):
        ctx.rich_exit(
            "[red]fzf[/red] command is missing, install with [bold]brew[/bold] or visit"
            + "https://github.com/junegunn/fzf"
        )

    if not which("yq"):
        ctx.rich_exit(
            "[red]yq[/red] command is missing, install with [bold]brew[/bold] or visit"
            + "https://github.com/mikefarah/yq"
        )

    # Create .env file if it doesn't exist
    env_path = Path(env_file)
    if not env_path.exists():
        ctx.print(f"[yellow]Creating new {env_file} file...[/yellow]")
        env_path.touch()

    # Initialize env dict for file handling
    env = EnvDict(env_file)

    # 1. Check and prompt for LITELLM_PROXY_URL if not defined
    url = env.get("LITELLM_PROXY_URL")

    changed, new_value = prompt_user(
        ctx, value_name="LITELLM_PROXY_URL", initial_value=url
    )
    if changed:
        # Extract base URL before saving
        url = _extract_base_url(new_value)
        changed = env.set("LITELLM_PROXY_URL", url)

        if changed:
            ctx.print(f"[green]✓[/green] Updated LITELLM_PROXY_URL in {env_file}")

    # 2. Check and prompt for LITELLM_PROXY_API_KEY if not defined or invalid
    def validate_api_for_url(api_key):
        assert url is not None
        with ctx.status("[yellow]Validating API key...[/yellow]"):
            if _validate_api_key(ctx, url, api_key):
                ctx.print("[green]✓[/green] API key is valid")
                return True
            else:
                ctx.print(
                    "[red]✗[/red] API key validation failed (4xx error). Please try again."
                )
                return False

    api_key = env.get("LITELLM_PROXY_API_KEY")
    changed, new_value = prompt_user(
        ctx,
        value_name="LITELLM_PROXY_API_KEY",
        initial_value=api_key,
        validation_function=validate_api_for_url,
    )

    if changed:
        api_key = new_value
        env.set("LITELLM_PROXY_API_KEY", api_key)

    prompt = f"Select model from {url}"
    fzf_args = f"--prompt='{prompt} >>' "
    if query:
        fzf_args = f"{fzf_args} --query={query}"

    with tempfile.NamedTemporaryFile(mode="w") as f:
        with ctx.status(f"Pulling models from {url}"):
            models = ctx.run(
                """
                uvx --from litellm[proxy] litellm-proxy models list --format json | \
                yq -pj '.[] | .id'
                """,
                hide=not ctx.config.run.echo,
                env={
                    "LITELLM_PROXY_URL": url,
                    "LITELLM_PROXY_API_KEY": api_key,
                },
            )
        f.write(models.stdout)
        f.flush()

        model: str = ctx.run(f"fzf <{f.name} {fzf_args}").stdout.strip()

    if not model:
        ctx.rich_exit("No model selected.")

    new_model = f"litellm_proxy/{model}"
    ctx.print(
        f"Updating [green]{env_file}[/green] LITELLM_PROXY_MODEL to {new_model!r}"
    )

    changed = env.set("LITELLM_PROXY_MODEL", new_model)
    if changed:
        ctx.print(f"[green]✓[/green] Successfully updated {env_file}")
    else:
        ctx.print(
            f"[yellow]⚠[/yellow] Value unchanged: {env_file} already has LITELLM_PROXY_MODEL={new_model!r}"
        )

    # Show verification command
    ctx.print(
        "\n[bold blue]To verify the model setup, run:[/bold blue]\n"
        f"[cyan]uv run --env-file {env_file} show-llms[/cyan]\n"
    )


@task()
def debug_llm_run(ctx: Context, script: str):
    """
    This is [bold green]uv run[/bold green] with [bold white]hunter[/bold white] tracing litellm calls
    """
    env = {
        "PYTHONHUNTER": 'Q(module_regex="(litellm.main.*)$")&(Q(kind="call")|Q(kind="return"))'
    }
    ctx.print_err(
        f"Running {script} with  🐍 tracer set for this expression", env["PYTHONHUNTER"]
    )
    ctx.run(f"uv run --with hunter {script}", pty=True)


script()
