#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "invoke-toolkit==0.0.42",
#     "dotenv",
# ]
# ///
#
# This task file can be run with
#

import os
import subprocess
from pathlib import Path
from shutil import which

from invoke_toolkit import Context, script, task

toplvel = subprocess.check_output("git rev-parse --show-toplevel", shell=True).decode()


def _update_env_file(envfile: str, key: str, value: str) -> bool:
    """
    Update or add an environment variable in an env file.

    If the key exists, its value is updated. If it doesn't exist, a new line is appended.

    Args:
        envfile: Path to the env file
        key: Environment variable key (e.g., "LITELLM_PROXY_MODEL")
        value: Environment variable value

    Returns:
        bool: True if the file was modified, False if the value was unchanged
    """
    env_path = Path(envfile)
    content = env_path.read_text()
    original_content = content

    # Check if key exists
    if f"{key}=" in content:
        # Replace existing value
        lines = content.splitlines(keepends=True)
        updated_lines = []
        for line in lines:
            if line.startswith(f"{key}="):
                updated_lines.append(f'{key}="{value}"\n')
            else:
                updated_lines.append(line)
        content = "".join(updated_lines)
    else:
        # Append new line at the end
        if not content.endswith("\n"):
            content += "\n"
        content += f'{key}="{value}"\n'

    # Check if content actually changed
    if content == original_content:
        return False

    # Write back to the env file
    env_path.write_text(content)
    return True


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
        ctx.run("uv run --group docs server", pty=True)


@task(aliases=["c"])
def change_litlellm_env_model(ctx: Context, envfile: str = ".env", query: str = ""):
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
    args = ""
    url = os.getenv("LITELLM_PROXY_URL", "???")
    prompt = f"Select model from {url}"
    if not Path(envfile).exists():
        ctx.rich_exit(f"{envfile} doesn't exist")
    args = f"{args} --prompt '{prompt} >> '"
    if query:
        args = f"{args} --query {query}"

    res = ctx.run(
        "uvx --from litellm[proxy] litellm-proxy models list --format json "
        + f"| yq -pj '.[] | .id' | fzf {args} ",
        hide="out",
    )
    model = res.stdout.strip()
    if not model:
        ctx.rich_exit("No model selected.")
    new_model = f"litellm_proxy/{model}"
    ctx.print(f"Updating [green]{envfile}[/green] LITELLM_PROXY_MODEL to {new_model!r}")

    changed = _update_env_file(envfile, "LITELLM_PROXY_MODEL", new_model)
    if changed:
        ctx.print(f"[green]✓[/green] Successfully updated {envfile}")
    else:
        ctx.print(
            f"[yellow]⚠[/yellow] Value unchanged: {envfile} already has LITELLM_PROXY_MODEL={new_model!r}"
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
