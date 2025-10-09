import shlex
import subprocess
from pathlib import Path

import nbformat
import pytest
from nbconvert.preprocessors import ExecutePreprocessor

repo: Path = Path(
    subprocess.check_output(shlex.split("git rev-parse --show-toplevel"))
    .decode()
    .strip()
)

TEST_NOTEBOOKS = list(repo.glob("tests/**/*.ipynb"))
NOTEOBOOKS_FOR_TESTING = {p.name: p for p in TEST_NOTEBOOKS}

TIMEOUT = 600


@pytest.mark.parametrize(
    "notebook",
    NOTEOBOOKS_FOR_TESTING.values(),
    ids=NOTEOBOOKS_FOR_TESTING.keys(),
)
def test_notebook_execution(notebook: Path, monkeypatch: pytest.MonkeyPatch):
    """Verifies that the notebook can be executed.

    If the test fails, check the report"""
    with notebook.open(encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)
    monkeypatch.chdir(notebook.parent)
    ep = ExecutePreprocessor(timeout=TIMEOUT)
    ep.preprocess(nb)
