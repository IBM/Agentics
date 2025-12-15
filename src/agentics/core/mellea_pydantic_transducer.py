# Put this at the top of your script
import asyncio
import logging
import os
import warnings
from typing import Optional, Type

os.environ["TQDM_DISABLE"] = "1"

# --- SAFE TQDM PATCH: override only the function, not the class ---
import tqdm

_original_tqdm = tqdm.tqdm  # keep the class intact!


def no_bar(iterable=None, *args, **kwargs):
    """Return iterable unchanged but keep tqdm.tqdm a valid function."""
    return iterable if iterable is not None else []


# Override only the *function*, not the module, not the class
tqdm.tqdm = no_bar

import mellea
from mellea.stdlib.sampling import RejectionSamplingStrategy
from pydantic import BaseModel

# Silence asyncio’s “Unclosed client session” ERROR logs
logging.getLogger("asyncio").setLevel(logging.CRITICAL)
logging.getLogger("aiohttp").setLevel(logging.CRITICAL)
logging.getLogger("fancy_logger").disabled = True

# Also silence possible ResourceWarnings about unclosed sessions/connectors
warnings.filterwarnings(
    "ignore", category=ResourceWarning, message="unclosed client session"
)
warnings.filterwarnings(
    "ignore", category=ResourceWarning, message="unclosed connector"
)


async def structured_decoding_using_mellea(
    input: str,
    targetAtype: Type[BaseModel],
    instructions: str = "",
    llm: str = "watsonx/openai/gpt-oss-120b",
) -> BaseModel | None:
    """
    Run Mellea with Granite and parse the result into `TargetAtype`.
    Returns an instance of TargetAtype or None if no output.
    """
    # The context manager should take care of opening/closing the HTTP client
    with mellea.start_session("litellm", model_id=llm) as m:
        mellea_output = await m.achat(
            instructions
            + f"your task is to transduce an Source object into an output json of the following type {targetAtype.model_json_schema()}\n"
            + input,
            # grounding_context={"": input},
            # requirements="Generate an object of the requested Pydantic type",
            # strategy=RejectionSamplingStrategy(loop_budget=5),
            # format=targetAtype,
            # return_sampling_results=True,
        )

    # At this point we're outside the `with`: session should be closed

    if mellea_output is None or mellea_output.content is None:
        return targetAtype()

    raw = mellea_output.content
    # If Mellea gave us a dict-like object:
    if isinstance(raw, dict):
        return targetAtype.model_validate(raw)
    # If it’s a JSON string:
    if isinstance(raw, str):
        return targetAtype.model_validate_json(raw)

    # Fallback: try generic validation
    return targetAtype.model_validate(raw)
