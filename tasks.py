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


script()
