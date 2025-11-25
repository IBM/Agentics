from pathlib import Path

import pytest
from invoke.context import Context
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
def git_root_path(git_root) -> "Path":
    """Returns the root directory of the repo"""
    return Path(git_root)


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
def llm_provider():
    try:
        from agentics.core.llm_connections import get_llm_provider

        return get_llm_provider()
    except ValueError:
        raise pytest.skip(reason="No available LLM")


def pytest_addoption(parser):
    parser.addoption(
        "--timeout", action="store", default=30, type=int, help="timeout in seconds"
    )


@pytest.fixture
def timeout_value(request):
    return request.config.getoption("--timeout")
