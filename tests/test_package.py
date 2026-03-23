from io import StringIO
from tempfile import NamedTemporaryFile
from textwrap import dedent

import pytest


@pytest.mark.core
def test_import(wheel, tmp_path_factory, ctx, venv):
    """Checks that main Agentics class has no import issues"""

    with ctx.cd(venv):
        # res = ctx.run("uv pip list --help", in_stream=False)
        ctx.run(f"uv pip install {wheel}", in_stream=False)
        with NamedTemporaryFile(suffix=".py", mode="w") as script:
            script.write(
                dedent(
                    """\
                from agentics import (
                    Agentics as AG,  # pylint: disable=import-outside-toplevel,unused-import
                )
                exit
                """
                )
            )

            ctx.run(
                f"uv run python {script.name}",
                in_stream=StringIO(),
                timeout=10,
                echo=True,
            )
