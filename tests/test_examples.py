from pathlib import Path
from socket import timeout

import pytest
from invoke.context import Context
from invoke.exceptions import CommandTimedOut
from pytest_subtests.plugin import SubTests


# TODO: @gliozzo provide small and fast to run examples
# @pytest.mark.skip(reason="Failing in actions with mellea warning")
@pytest.mark.parametrize(
    "file_to_test",
    [
        "tests/official_tests/hello_world.py",
        "tests/official_tests/transducible_functions.py",
        # "examples/hello_world.py",
        "examples/emotion_extractor.py",
        "examples/generate_tweets.py",
        # "examples/mcp_server_example.py",
        # "examples/agentics_web_search_report.py",
    ],
)  # script: find ./examples/ -type f | yq 'split(" ")' -oj | sed -e 's|\.\/||g' | pbcopy
def test_parametrized_examples(
    git_root_path: "Path",
    file_to_test: str,
    ctx: Context,
    timeout_value: int,
    llm_provider: None,
):
    """
    Allows to select which tests to run the parametrized list of tests
    """
    py_to_test = git_root_path / file_to_test

    with ctx.cd(git_root_path):
        try:
            run_result = ctx.run(
                f"uv run {py_to_test}",
                warn=True,
                timeout=timeout_value,
                in_stream=False,
            )
            assert run_result is not None
            assert (
                run_result.ok
            ), f"stderr: {run_result.stderr}\nstdout: {run_result.stdout}"

        except CommandTimedOut:
            raise pytest.fail(
                reason=f"Running {file_to_test} took more than {timeout_value} seconds"
            )


# TODO: @D3f0 make sure this is declarative
@pytest.mark.skip(reason="Too slow to run online")
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
