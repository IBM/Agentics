from pathlib import Path

import pytest
from pytest_subtests.plugin import SubTests


# TODO: @D3f0 make sure this is declarative
def test_examples(git_root_path: "Path", subtests: SubTests, ctx):
    tutorial_folder = git_root_path / "examples"
    examples_py_files = [
        p
        for p in tutorial_folder.glob("*.py")
        if p.name != "agentics_web_search_report.py"
    ]

    for i, example_py in enumerate(examples_py_files):
        with subtests.test(msg=f"Running example {example_py}", i=i):
            with ctx.cd(git_root_path):
                run_result = ctx.run(f"uv run {example_py}", warn=True)
                assert (
                    run_result.ok
                ), f"stderr: {run_result.stderr}\nstdout: {run_result.stdout}"
