import shlex
import subprocess
from pathlib import Path

import pytest


def test_install_in_venv_as_folder(venv, ctx, tmp_path, git_root):
    with ctx.cd(tmp_path):
        ctx.run(f"uv pip install {git_root}", in_stream=False)
        ctx.run("uv pip list | grep agentics", in_stream=False)


def test_dist_install(wheel, tmp_path_factory, ctx, venv):
    with ctx.cd(venv):
        # res = ctx.run("uv pip list --help", in_stream=False)
        ctx.run(f"uv pip install {wheel}", in_stream=False)
        ctx.run("uv pip list | grep agentics", in_stream=False)
