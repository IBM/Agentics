# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "invoke",
#     "pdbpp",
#     "hunter",
#     "rich",
# ]
# ///

"""
You can run this tasks with *any* of these options:

1. uv run inv -l | --help | [command]
2. uv run tasks.py -l | --help | [command]

"""
# pylint: disable=dangerous-default-value

import contextlib
import tempfile
from io import StringIO
from pathlib import Path
from textwrap import dedent
from typing import Annotated, Any, Generator, List

import rich
from invoke import Task, task
from invoke.collection import Collection
from invoke.context import Context
from rich import pretty
from rich.console import Console

pretty.install()

console = Console(stderr=True)


TOP_LEVEL: Annotated[Path, "The root directory of the repo"] = Path(__file__).parent


@task(default=True, autoprint=True)
def version(
    ctx: Context,
):
    """Shows package version (git based)"""
    with ctx.cd(TOP_LEVEL):
        return ctx.run(
            "uvx --with uv-dynamic-versioning hatchling version",
            hide=not ctx.config.run.echo,
        ).stdout.strip()


@task()
def cfg(ctx: Context):
    """Task configuration file."""
    for key in ctx.config:
        rich.print(f"[bold]{key}[/bold]")
        rich.print(dict(ctx.config[key]))


@task(
    help={
        "target_": "Target format",
        "output": "Output directory, by default is ./dist/",
    },
    autoprint=True,
)
def build(ctx: Context, target_=[], output="./dist/"):
    """Builds distributable package"""
    args = ""
    if target_:
        target = " ".join(f"-t {t}" for t in target_)
        args = f"{args} {target}"
    if output:
        args = f"{args} -d {output}"

    return ctx.run(
        f"uvx --with uv-dynamic-versioning hatchling build {args}",
        hide=not ctx.config.run.echo,
    ).stderr.strip()


@task()
def clean(ctx: Context):
    """Cleans dist"""
    ctx.run(r"rm -rf ./dist/*.{tar.gz,whl}")


@contextlib.contextmanager
def temp_dir(ctx: Context, post_clean: bool = True) -> Generator["Path", Any, Any]:
    """Creates a temporary directory and changes the context to that directory"""
    tmp_dir = Path(tempfile.mkdtemp())
    with ctx.cd(tmp_dir):
        yield tmp_dir
    if post_clean:
        with console.status(
            "Cleaning temporary directory (keep it with --no-post-clean)"
        ):
            ctx.run(f"rm -rf {tmp_dir}")


NO_VENV = {"VIRTUAL_ENV": ""}


@task(
    aliases=[
        "tpkg",
    ]
)
def test_package_isolated(
    ctx: Context,
    post_clean: bool = True,
    command_to_run: str = "ipython",
    python: str = "3.12",
):
    """
    Builds the package in a temporary directory, creates a new virtualenv and installs it
    there. Then runs the command, by default IPython
    """

    with temp_dir(ctx, post_clean == post_clean) as tmpd:
        with console.status("Building wheel"):
            with ctx.cd(TOP_LEVEL):
                wheel_location = build(ctx, target_=("wheel",), output=tmpd)

        with console.status("Creating virtualenv..."):
            ctx.run(f"uv venv --python {python}", env=NO_VENV)
        with console.status("Installing freshly backed wheel file"):
            ctx.run(f"uv pip install {wheel_location}", env=NO_VENV)
        ctx.run(
            "uv run python",
            in_stream=StringIO(
                dedent(
                    r"""
                from importlib.metadata import version
                ver = version('agentics-py')
                print(f"\n\nagentics-py version: {ver}\n\n", )
            """
                )
            ),
            pty=False,
            env=NO_VENV,
        )
        ctx.run(f"uv run {command_to_run}", pty=True, env=NO_VENV)


@task(aliases=["tpypi"])
def test_package_pypi(
    ctx: Context,
    package_: List[str] = [],
    python: str = "3.12",
    command_to_run: str = "ipython",
    post_clean: bool = False,
) -> None:
    """
    Creates a new virtualenv and installs the package PyPI version in it.
    Then runs the command, by default IPython
    """
    if not package_:
        package_ = ["agentics-py"]
    with temp_dir(ctx, post_clean=post_clean) as tmpd:
        with console.status("Creating virtualenv..."):
            ctx.run(f"uv venv --python {python}", env=NO_VENV)
        packages = " ".join(package_)
        with console.status(f"Installing {packages}"):
            ctx.run(f"uv pip install {packages}", env=NO_VENV)
        for pkg in package_:
            console.print(f"[bold]{pkg}[/bold]")
            ctx.run(f"uv pip show {pkg}", env=NO_VENV)

        ctx.run(f"uv run {command_to_run}", pty=True, env=NO_VENV)


# This is only required for configuration
ns: Collection = Collection()
local_tasks: List[Task] = [
    obj for name, obj in list(locals().items()) if isinstance(obj, Task)
]
for tsk in local_tasks:
    ns.add_task(tsk)

if __name__ == "__main__":
    from invoke.program import Program

    p = Program(namespace=ns)
    p.run()
