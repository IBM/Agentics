# pylint: disable=redefined-outer-name,
# pylint: disable=possibly-unbound-attribute
import json
from pathlib import Path
from typing import Dict

import pytest
from invoke.context import Context
from loguru import logger
from typing_extensions import Annotated


@pytest.fixture()
def venv(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx
):
    """
    Creates a virtualenv backend by venv, returns a Path
    that has its .venv directory with a clean venv created with uv
    """
    python_version = request.config.getoption("python_version", default="3.12")
    with ctx.cd(tmp_path):
        ctx.run(f"uv venv --seed --python {python_version}", in_stream=False)

    monkeypatch.setenv("VIRTUAL_ENV", str(tmp_path / ".venv"))
    return tmp_path


@pytest.fixture()
def git_root():
    """Returns the root directory of the repo"""
    return (
        Context().run("git rev-parse --show-toplevel", in_stream=False).stdout.strip()
    )


@pytest.fixture()
def ctx() -> Context:
    """Provides a Context for shell interaction

    ctx.cd is a context manager to change directories like in bash/zsh
    ctx.run will execute commands following the protocol defined Local runner
    defined at https://docs.pyinvoke.org/en/stable/api/runners.html
    """
    return Context()


@pytest.fixture()
def wheel(
    ctx, git_root, tmp_path_factory
) -> Annotated[Path, "The wheel file to install"]:
    with ctx.cd(git_root):
        output = tmp_path_factory.mktemp("dist")
        ctx.run(f"uv build -o {output}", in_stream=False)
    wheel_file, *_ = output.glob("*.whl")
    return wheel_file


@pytest.fixture()
def jupyter_kernel(ctx: Context) -> str:
    """Returns the name of a valid Jupyter kernel"""
    kernel_list: Dict[str, Any] = json.loads(
        ctx.run("jupyter kernelspec list --json", in_stream=False, hide=True).stdout
    )
    kernelspecs: Dict[str, Any] = kernel_list.get("kernelspecs", {})
    if not kernelspecs:
        kernel = "python3"
        logger.info(f"Creating kernel {kernel}")
        ctx.run(
            # https://github.com/jupyter/jupyter_client/issues/72
            f"python -m ipykernel install --user --name {kernel} --display-name 'tests'",
            echo=True,
            in_stream=False,
            hide=True,
        )
    else:
        kernel, *_ = kernelspecs.keys()
    logger.info(f"Using kernel {kernel}")
    return kernel
