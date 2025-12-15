import asyncio
import datetime
import inspect
import os
import re
from collections.abc import Iterable
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_origin,
    get_type_hints,
)

import httpx
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from numerize.numerize import numerize
from openai import APIStatusError, AsyncOpenAI
from pydantic import BaseModel, Field, create_model
from pydantic.fields import FieldInfo
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


def import_pydantic_from_code(code: str):
    """
    Dynamically execute Pydantic class code and return the first
    Pydantic BaseModel subclass defined in it.

    Automatically injects basic typing symbols and pydantic imports
    so the code can safely reference them even if not explicitly imported.
    """
    # Create isolated module namespace
    module = types.ModuleType("dynamic_module")

    # Preload common symbols that generated code may need
    safe_globals = {
        "__builtins__": __builtins__,
        # Core Pydantic symbols
        "BaseModel": BaseModel,
        "Field": Field,
        # Common typing imports
        "Any": Any,
        "Optional": Optional,
        "List": List,
        "Dict": Dict,
        "Tuple": Tuple,
        "Set": Set,
        "Union": Union,
        "Literal": Literal,
        "Type": Type,
        "Sequence": Sequence,
        "Mapping": Mapping,
        "Annotated": Annotated,
        "datetime": datetime,
    }

    module.__dict__.update(safe_globals)
    try:
        # Execute the generated code
        exec(code, module.__dict__)

        # Automatically find the first Pydantic model class
        classes = [
            obj
            for obj in module.__dict__.values()
            if isinstance(obj, type)
            and issubclass(obj, BaseModel)
            and obj is not BaseModel
        ]

        if not classes:
            raise ValueError(
                "No Pydantic BaseModel subclass found in the provided code."
            )

        # Return the first detected model class
        return classes[-1]
    except:
        return None


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


import math


def sanitize_dict_keys(obj):
    """
    Recursively sanitize dictionary keys and values.

    - Cleans keys via `sanitize_field_name`.
    - Replaces NaN/NaT with None.
    - Works for nested dicts and lists.
    """
    if isinstance(obj, dict):
        return {sanitize_field_name(k): sanitize_dict_keys(v) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [sanitize_dict_keys(item) for item in obj]

    elif isinstance(obj, float) and math.isnan(obj):
        # Replace float('nan') with None
        return None

    elif pd.isna(obj) if hasattr(pd, "isna") else False:
        # Replace pandas NaN / NaT / None
        return None

    else:
        return obj


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


def llm_friendly_json(model: type[BaseModel]) -> str:
    """
    Convert a Pydantic BaseModel subclass into a clean LLM-friendly JSON template.
    Returns a JSON string the LLM can easily follow.
    """
    schema = model.model_json_schema()

    # keep only properties → field: type
    props: Dict[str, Any] = schema.get("properties", {})

    # Build a simple template the LLM can follow
    simple = {}
    for name, info in props.items():
        field_type = info.get("type", "any")
        example = info.get("examples", [None])[0] if info.get("examples") else None

        simple[name] = {
            "type": field_type,
            "description": info.get("description", ""),
            "example": example,
        }

    # Return pretty-formatted JSON string
    return json.dumps(simple, indent=4)


import ast
import textwrap
import types
from typing import Callable


def import_last_function_from_code(code: str) -> Callable:
    """
    Execute the code and return ONLY the last user-defined function,
    enriched with:
        fn.__source__         = function's own source block
        fn.__source_types__   = {"source": <input type>, "target": <output type>}
    """
    module = types.ModuleType("dynamic_module")
    code = textwrap.dedent(code)
    exec(code, module.__dict__)

    # -------------------------------------------------------------
    # 1. Find all user-defined functions
    # -------------------------------------------------------------
    funcs = [
        obj
        for obj in module.__dict__.values()
        if callable(obj)
        and hasattr(obj, "__code__")
        and obj.__module__ == module.__name__
    ]

    if not funcs:
        raise ValueError("No functions found.")

    fn = funcs[-1]  # last defined

    # -------------------------------------------------------------
    # 2. Parse AST to extract class and function blocks
    # -------------------------------------------------------------
    tree = ast.parse(code)

    class_blocks: Dict[str, str] = {}
    function_blocks: Dict[str, str] = {}

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            class_blocks[node.name] = textwrap.dedent(
                ast.get_source_segment(code, node)
            )

        if isinstance(node, ast.FunctionDef):
            function_blocks[node.name] = textwrap.dedent(
                ast.get_source_segment(code, node)
            )

    # -------------------------------------------------------------
    # 3. Attach function source code block
    # -------------------------------------------------------------
    if fn.__name__ in function_blocks:
        fn.__source__ = function_blocks[fn.__name__]
    else:
        fn.__source__ = None

    # -------------------------------------------------------------
    # 4. Identify input/output annotated types
    # -------------------------------------------------------------
    hints = inspect.get_annotations(fn, eval_str=True)

    source_model = None
    target_model = None

    for arg_name, t in hints.items():
        if arg_name == "return":
            target_model = t
        else:
            source_model = t

    # -------------------------------------------------------------
    # 5. Attach type code blocks (source & target)
    # -------------------------------------------------------------
    source_code = None
    target_code = None

    if inspect.isclass(source_model) and source_model.__name__ in class_blocks:
        source_model.__source__ = class_blocks[source_model.__name__]
        source_code = class_blocks[source_model.__name__]

    if inspect.isclass(target_model) and target_model.__name__ in class_blocks:
        target_model.__source__ = class_blocks[target_model.__name__]
        target_code = class_blocks[target_model.__name__]

    fn.__source_types__ = {"source": source_code, "target": target_code}

    return fn


import hashlib


def compute_function_hash(
    fn: Callable | str,
    #   SourceModel:Type[BaseModel] | str = None,
    #   TargetModel:Type[BaseModel] | str = None
):
    """
    Compute a SHA256 hash from:
      - function source code
      - source model definition
      - target model definition
    """

    hasher = hashlib.sha256()

    # --- function source
    if fn:
        if type(fn) == str:
            fn_source = fn
        else:
            try:
                fn_source = inspect.getsource(fn)
            except OSError:
                fn_source = fn.__source__ if hasattr(fn, "__source__") else ""
        hasher.update(fn_source.encode("utf-8"))

    # # --- source model
    # if SourceModel:
    #     if type(SourceModel) ==str:
    #         hasher.update(SourceModel)
    #     else:
    #         if SourceModel is not None:
    #             model_source = getattr(SourceModel, "__source__", str(SourceModel))
    #             hasher.update(model_source.encode("utf-8"))

    #     # --- target model
    # if TargetModel:
    #     if TargetModel is not None:
    #         model_source = getattr(TargetModel, "__source__", str(TargetModel))
    #         hasher.update(model_source.encode("utf-8"))

    # Final deterministic fingerprint
    return hasher.hexdigest()


def get_function_io_types(
    fn: Callable,
    *,
    skip_self: bool = True,
) -> Tuple[Dict[str, Any], Any]:
    """
    Infer input and output types from a function's annotations.

    Returns:
        (input_types, output_type)

    Notes:
        - Works with sync/async functions.
        - Automatically unwraps decorators.
        - Skips `self` or `cls` if desired.
        - If types were dynamically created with attached __source__,
          that metadata remains on the class object.
    """

    if not callable(fn):
        raise TypeError(f"{fn!r} is not callable")

    # Unwrap decorated functions (@aFunction, @wraps, wrapper layers)
    original = inspect.unwrap(fn)

    # Resolve type hints (handles forward references and Pydantic models)
    hints = get_type_hints(original)
    sig = inspect.signature(original)

    input_types: Dict[str, Any] = {}

    # ------------------------ INPUT TYPES ------------------------
    for name, param in sig.parameters.items():
        if skip_self and name in {"self", "cls"}:
            continue

        # Use annotated type, fallback to Any
        t = hints.get(name, Any)
        input_types[name] = t

    # ------------------------ OUTPUT TYPE ------------------------
    output_type = hints.get("return", Any)

    return input_types, output_type


from typing import Optional, Type

from pydantic import BaseModel, create_model


def make_transduction_type(
    source: Type[BaseModel],
    target: Type[BaseModel],
    *,
    name: Optional[str] = None,
) -> Type[BaseModel]:
    """
    Create a Pydantic model representing a transduction between `source` and `target`.

    The resulting model has the structure:
        {
            "source": <source model>,
            "target": <target model>
        }

    Parameters
    ----------
    source : BaseModel subclass
        Input type of the transduction.
    target : BaseModel subclass
        Output type of the transduction.
    name : str | None
        Optional name for the resulting transduction type.

    Returns
    -------
    BaseModel subclass
        A dynamically created model with slots `source` and `target`.
    """

    if name is None:
        name = f"Transduction__{source.__name__}__TO__{target.__name__}"

    TransductionModel = create_model(
        name,
        source=(source, ...),
        target=(target, ...),
        __base__=BaseModel,
    )

    return TransductionModel


def merge_pydantic_models(
    source: Type[BaseModel],
    target: Type[BaseModel],
    *,
    name: str | None = None,
) -> Type[BaseModel]:
    """
    Create a new Pydantic model with the union of fields from `source` and `target`.
    If a field appears in both, the `source` model's annotation and FieldInfo take precedence.

    Parameters
    ----------
    source : BaseModel subclass
        Preferred model for conflicting fields (annotation/Field settings win).
    target : BaseModel subclass
        Secondary model; its fields are added when not present in `source`.
    name : str | None
        Optional name for the merged model (default builds a descriptive one).

    Returns
    -------
    BaseModel subclass
        A dynamically created model with the merged schema.
    """

    # Resolve annotations (include_extras to preserve Optional/Annotated info)
    src_ann = get_type_hints(source, include_extras=True)
    tgt_ann = get_type_hints(target, include_extras=True)

    # Access FieldInfo objects (pydantic v2)
    src_fields: Dict[str, FieldInfo] = getattr(source, "model_fields", {})
    tgt_fields: Dict[str, FieldInfo] = getattr(target, "model_fields", {})

    merged_defs: Dict[str, tuple[Any, Any]] = {}

    # 1) Take all fields from source (preferred on conflict)
    for fname, ann in src_ann.items():
        finfo = src_fields.get(fname)
        if finfo is None:
            # If no FieldInfo (rare), supply a no-default sentinel by passing None
            merged_defs[fname] = (ann, None)
        else:
            # Pass FieldInfo directly so defaults/constraints/metadata are preserved
            merged_defs[fname] = (ann, finfo)

    # 2) Add fields unique to target (skip those already taken from source)
    for fname, ann in tgt_ann.items():
        if fname in merged_defs:
            continue
        finfo = tgt_fields.get(fname)
        if finfo is None:
            merged_defs[fname] = (ann, None)
        else:
            merged_defs[fname] = (ann, finfo)

    # Name the new model if not provided
    if name is None:
        name = f"{source.__name__}__UNION__{target.__name__}"

    # Create the merged model. We inherit from BaseModel to avoid pulling configs unexpectedly,
    # but you can set __base__=source to inherit source config instead if you prefer.
    Merged = create_model(
        name,
        __base__=BaseModel,
        **merged_defs,  # type: ignore[arg-type]
    )

    return Merged


def percent_non_empty_fields(instance: BaseModel) -> float:
    """
    Return the percentage of non-empty fields in a Pydantic model instance.
    """

    def _is_non_empty(value):
        """Return True if a value should count as 'filled'."""
        if value is None:
            return False
        if isinstance(value, str) and value.strip() == "":
            return False
        if isinstance(value, (list, dict, set, tuple)) and len(value) == 0:
            return False
        return True

    data = instance.model_dump()
    total = len(data)
    if total == 0:
        return 0.0

    filled = sum(1 for v in data.values() if _is_non_empty(v))
    return filled / total
