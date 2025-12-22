#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "invoke-toolkit==0.0.26",
# ]
# ///

from pathlib import Path

from invoke_toolkit import Context, script, task


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


script()
