from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import pytest
from invoke.context import Context
from typing_extensions import Annotated

if TYPE_CHECKING:
    from agentics.core.llm_connections import LLM


@pytest.fixture()
def venv(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx
) -> Path:
    """
    Creates a Python virtual environment using uv.
    Ensures that VIRTUAL_ENV is not passed down down to avoid conflicts.
    """
    python_version = request.config.getoption("python_version", default="3.12")
    with ctx.cd(tmp_path):
        ctx.run(f"uv venv --seed --python {python_version}", in_stream=False)

    monkeypatch.setenv("VIRTUAL_ENV", str(tmp_path / ".venv"))
    return tmp_path


@pytest.fixture()
def git_root() -> str:
    """Returns the root directory of the repo."""
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
    """
    Build a wheel file from the source code that should be pip installable.
    You can combine this fixture with the virtualenv
    """
    with ctx.cd(git_root):
        output = tmp_path_factory.mktemp("dist")
        ctx.run(
            f"uvx --with uv-dynamic-versioning hatchling build -t wheel -d {output}",
            in_stream=False,
        )
    wheel_file, *_ = output.glob("*.whl")
    return wheel_file


@pytest.fixture()
def llm_provider() -> "LLM":
    """LLLm provider function"""
    try:
        from agentics.core.llm_connections import get_llm_provider

        return get_llm_provider()
    except ValueError:
        raise pytest.skip(reason="No available LLM")
