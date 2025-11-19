import asyncio
import inspect
import os
import re
from collections.abc import Iterable
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    get_origin,
)

import httpx
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from numerize.numerize import numerize
from openai import APIStatusError, AsyncOpenAI
from pydantic import BaseModel, Field, create_model
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    Text,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

A = TypeVar("A", bound=BaseModel)

load_dotenv()


def scan_directory_recursively(path: str) -> List[str]:
    """Recursively scans the directory and returns a list of file paths."""
    files = []

    def _scan(current_path: str):
        if os.path.isdir(current_path):
            with os.scandir(current_path) as entries:
                for entry in entries:
                    if entry.is_dir(follow_symlinks=False):
                        _scan(entry.path)
                    else:
                        files.append(entry.path)
        else:
            files.append(current_path)

    _scan(path)
    return files


def infer_pydantic_type(dtype: Any, sample_values: pd.Series = None) -> Any:
    if pd.api.types.is_integer_dtype(dtype):
        return Optional[int]
    elif pd.api.types.is_float_dtype(dtype):
        return Optional[float]
    elif pd.api.types.is_dict_like(dtype):
        return Optional[dict]
    elif pd.api.types.is_bool_dtype(dtype):
        return Optional[bool]
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return Optional[str]  # Or datetime.datetime
    elif sample_values is not None:
        # Check if the column contains lists of strings
        for val in sample_values:
            if isinstance(val, list) and all(isinstance(x, str) for x in val):
                return Optional[List[str]]
            elif isinstance(val, dict):
                if all(isinstance(k, str) for k in val.keys()):
                    if all(
                        isinstance(v, (str, list))
                        and (isinstance(v, str) or all(isinstance(i, str) for i in v))
                        for v in val.values()
                    ):
                        return Optional[Dict[str, Union[str, List[str]]]]
            break  # Only check the first non-null value
    return Optional[str]


def sanitize_field_name(name: str) -> str:
    name = name.strip()
    # Remove underscores only from the start
    name = re.sub(r"^_+", "", name)
    # If the result is alphanumeric, return as-is
    if re.fullmatch(r"[a-zA-Z0-9_]+", name):
        return name
    # Otherwise, remove all non-alphanumeric and non-underscore characters
    return re.sub(r"[^\w]", "", name)


def sanitize_dict_keys(obj):
    if isinstance(obj, dict):
        return {sanitize_field_name(k): sanitize_dict_keys(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_dict_keys(item) for item in obj]
    else:
        return str(obj)


def chunk_list(lst, chunk_size: int = None):
    """
    Splits a list into a list of lists, each of a given size.

    Args:
        lst (list): The list to split.
        chunk_size (int): The size of each chunk.

    Returns:
        list of lists: A list where each element is a sublist of length `chunk_size`, except possibly the last one.
    """
    if chunk_size:
        return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]
    else:
        return [lst]


def clean_for_json(obj: Any) -> Any:
    if isinstance(obj, BaseModel):
        return {k: clean_for_json(v) for k, v in obj.model_dump().items()}
    elif isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [clean_for_json(v) for v in obj]
    elif isinstance(obj, type):
        return str(obj.__name__)  # convert classes like ModelMetaclass to string
    elif inspect.isfunction(obj) or inspect.ismethod(obj):
        return f"<function {obj.__name__}>"
    elif isinstance(obj, (int, float, str, bool)) or obj is None:
        return obj
    else:
        return str(obj)


def remap_dict_keys(data: dict, mapping: dict) -> dict:
    """
    Remap the keys of a dictionary based on a provided mapping.

    Parameters:
    - data: dict — original dictionary
    - mapping: dict — mapping of old_key -> new_key

    Returns:
    - dict — new dictionary with remapped keys
    """
    return {mapping.get(k, k): v for k, v in data.items()}


def process_raw_completion_all(raw_completion):
    contents = []
    logprobs = []
    for choice in raw_completion.choices:
        contents.append(choice.message.content)
        logprobdict = {"token": [], "logprob": []}
        for logpr in choice.logprobs.content:
            logprobdict["token"].append(logpr.token)
            logprobdict["logprob"].append(logpr.logprob)
        logprobs.append(logprobdict)
    return {"contents": contents, "logprobs": logprobs}


def process_raw_completion_one(raw_completion):
    return raw_completion.choices[0].message.content


async def openai_response(
    model, base_url, user_prompt, system_prompt=None, history_messages=[], **kwargs
):
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.extend(history_messages)
    messages.append({"role": "user", "content": user_prompt})

    try:
        client = AsyncOpenAI(
            api_key="EMPTY",
            base_url=base_url,
            default_headers={
                "Content-Type": "application/json",
            },
        )

        completion = await client.chat.completions.create(
            model=model, messages=messages, timeout=100, **kwargs
        )
        if kwargs["logprobs"]:
            return process_raw_completion_all(completion, **kwargs)
        else:
            return process_raw_completion_one(completion)
    except APIStatusError as e:
        logger.error(f"API Error ({e.status_code}): {e.response.json()}")
        raise
    except httpx.ConnectError as e:
        logger.error(
            f"Connection Error: Could not connect to vLLM server at {client.base_url}. Is it running?"
        )
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during API call: {e}")
        raise


def make_all_fields_optional(
    model_cls: type[BaseModel], rename_type: str = None
) -> type[BaseModel]:
    """
    Returns a new Pydantic model class where all fields are Optional and default to None.

    Args:
        model_cls: Original Pydantic model class.
        rename_type: Name of the new model class (default: <OriginalName>Optional)

    Returns:
        New Pydantic model class with all fields optional.
    """
    fields = {}
    for name, field in model_cls.model_fields.items():
        # Original type
        annotation = field.annotation
        origin = get_origin(annotation)

        # Make it Optional if not already
        if origin is not Optional and annotation is not Any:
            annotation = Optional[annotation]

        fields[name] = (
            annotation,
            Field(default=None, title=field.title, description=field.description),
        )

    new_name = rename_type or f"{model_cls.__name__} (optional)"
    return create_model(new_name, **fields)


def is_str_or_list_of_str(input):
    return isinstance(input, str) or (
        isinstance(input, Iterable) and all(isinstance(i, str) for i in input)
    )


async def async_odered_progress(
    inputs: Sequence[Any],
    work: Callable[[Any], Awaitable[Any]],
    description: str = "Working",
    timeout: Optional[float] = None,
    transient_pbar: bool = False,
) -> list[Any]:
    """Show a Rich progress bar while awaiting async execution."""
    if transient_pbar:
        columns = (
            SpinnerColumn(style="grey50"),
            StyledColumn(TimeElapsedColumn()),
            TextColumn("{task.description}", style="grey50"),
            BarColumn(
                bar_width=40,
                style="grey30",
                complete_style="grey58",
                finished_style="grey62",
                pulse_style="grey50",
            ),
            StyledColumn(MofNCompleteColumn()),
            StyledColumn(TransductionSpeed()),
            StyledColumn(TimeRemainingColumn()),
        )
    else:
        columns = (
            SpinnerColumn(),
            TimeElapsedColumn(),
            TextColumn(f"[bold]{description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TransductionSpeed(),
            TimeRemainingColumn(),
        )
    with Progress(*columns, transient=transient_pbar) as progress:

        async def track(index: int, coro: Awaitable[Any]) -> Any:
            try:
                return index, await coro
            except Exception as e:
                return index, e  # TODO: we can put the retry here
            finally:
                progress.advance(task_id)

        task_id = progress.add_task(description, total=len(inputs))
        tasks = [asyncio.create_task(track(i, work(x))) for i, x in enumerate(inputs)]
        results: list[Any] = [None] * len(tasks)

        # complete and replace in original order
        for fut in asyncio.as_completed(tasks, timeout=timeout):
            i, val = await fut
            results[i] = val
        return results


class StyledColumn(ProgressColumn):
    """Apply a Rich style to the renderable of another column."""

    def __init__(self, inner: ProgressColumn, style: str = "grey50"):
        super().__init__()
        self.inner = inner
        self.style = style

    def render(self, task: Task):
        r = self.inner.render(task)
        if isinstance(r, Text):
            r.stylize(self.style)
            return r
        return Text(str(r), style=self.style)


class TransductionSpeed(ProgressColumn):
    """Renders human readable transfer speed."""

    def render(self, task: "Task") -> Text:
        """Show data transfer speed."""
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("? states/s", style="progress.data.speed")
        return Text(f"{speed:.3f} states/s", style="progress.data.speed")


def make_states_list_model(item_type: Type[A]) -> Type[BaseModel]:
    """
    Dynamically create a Pydantic model:

        class ATypeList(BaseModel):
            states: List[item_type] = []

    but with proper validation and default_factory.
    """
    # Name is optional; Pydantic will auto-unique if reused.
    return create_model(
        "ATypeList", states=(List[item_type], Field(default_factory=list))
    )


import json

from jsonfinder import jsonfinder


def extract_json_objects(text: str, expected_type: Type) -> List[BaseModel]:
    """
    Scan `text` and return a list of (start_index, end_index, parsed_obj)
    for every valid JSON object/array found. Works even if surrounded by prose.
    """
    good_answers = []
    for match in jsonfinder(text):
        start, end, obj = match
        try:
            good_answers.append(expected_type(**obj))
        except:
            pass
    return good_answers[-1] if len(good_answers) > 0 else []
    # json_object = [expected_type(**json.loads(to_valid_json(x))) for x in results]


def to_valid_json(obj: Any, pretty: bool = True) -> str:
    """
    Return a canonical valid JSON string. Since object key order is not significant,
    we serialize with sort_keys=True for stable output.
    """
    if pretty:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
