from pathlib import Path

import papermill as pm
import pytest


@pytest.mark.skip(reason="User input not mocked")
@pytest.mark.parametrize(
    "notebook",
    (
        "tutorials/llms.ipynb",
        # "tutorials/transduction.ipynb",
        # "tutorials/agentics_basics.ipynb",
        # "tutorials/amap_reduce.ipynb",
        # tutorials/mcp_tools.ipynb"
    ),
)
def test_tutorials(git_root, tmp_path: Path, notebook):
    input_notebook = Path(git_root) / notebook

    out_nb = tmp_path / "report_out.ipynb"
    pm.execute_notebook(
        input_notebook,
        out_nb,
        parameters={"RUN_MODE": "test", "LIMIT": 100},
        cwd=".",
        kernel_name="python3",
    )


@pytest.mark.asyncio
async def test_hello_world(llm_provider):
    from typing import Optional

    from dotenv import load_dotenv
    from pydantic import BaseModel

    from agentics import AG

    load_dotenv()

    ## Define output type

    class Answer(BaseModel):
        answer: Optional[str] = None
        justification: Optional[str] = None
        confidence: Optional[float] = None

    ## Collect input text

    input_questions = [
        "What is the capital of Italy?",
        # "This is my first time I work with Agentics",
        # "What videogames inspire suicide?",
    ]

    ## Transduce input strings into objects of type Answer.
    ## You can customize this providing different llms and instructions.

    answers = await (
        AG(
            atype=Answer,
            llm=llm_provider,  ##Select your LLM from list of available options
            # instructions="""Provide an Answer for the following input text
            # only if it contains an appropriate question that do not contain
            # violent or adult language """
        )
        << input_questions
    )

    assert len(answers) == 1
    assert answers[0].answer is not None, "It's not transduced"
