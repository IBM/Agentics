import shlex
import subprocess
from pathlib import Path

import pytest
from invoke.runners import Result


def test_install_in_venv_as_folder(venv, ctx, tmp_path, git_root):
    """
    Checks that uv pip install /path/to/repo works
    """
    with ctx.cd(tmp_path):
        ctx.run(f"uv pip install {git_root}", in_stream=False)
        ctx.run("uv pip list | grep agentics", in_stream=False)


def test_dist_install(wheel, tmp_path_factory, ctx, venv):
    """
    Verifies that the wheel version of the package installs without errors
    """
    with ctx.cd(venv):
        install: Result = ctx.run(f"uv pip install {wheel}", in_stream=False, warn=True)
        # ctx.run("uv pip list | grep agentics", in_stream=False)
        run = ctx.run(
            "uv run python -c 'from agentics import AG'", in_stream=False, warn=True
        )
        assert run.ok, run.stderr
