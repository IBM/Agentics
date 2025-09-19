import csv
import inspect
import json
import os
import re
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

import httpx
import pandas as pd
from dotenv import load_dotenv
from json_schema_to_pydantic import create_model as json_create_model
from loguru import logger
from openai import APIStatusError, AsyncOpenAI
from pandas import DataFrame
from pydantic import BaseModel, Field, create_model

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


def get_active_fields(state: BaseModel, allowed_fields: Set[str] = None) -> Set[str]:
    """
    Returns the set of fields in `state` that are None and optionally intersect with allowed_fields.
    """
    active_fields = {
        k for k, v in state.model_dump().items() if v is not None and v != ""
    }
    return active_fields & allowed_fields if allowed_fields else active_fields


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
        return obj


# def pydantic_model_from_csv(file_path: str) -> type[BaseModel]:
#     with open(file_path, newline="", encoding="utf-8") as csvfile:
#         reader = csv.DictReader(csvfile)
#         columns = [sanitize_field_name(x) for x in reader.fieldnames]
#         model_name = "AType#" + ":".join(columns)
#         if not columns:
#             raise ValueError("CSV file appears to have no header.")
#         fields = {col: (Optional[str], None) for col in columns}
#         return create_model(model_name, **fields)


def pydantic_model_from_csv(file_path: str) -> type[BaseModel]:
    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        columns = [sanitize_field_name(x) for x in reader.fieldnames]
        model_name = "AType#" + ":".join(columns)
        if not columns:
            raise ValueError("CSV file appears to have no header.")
        fields = {col: (Optional[str], None) for col in columns}
        return create_model(model_name, **fields)


def infer_pydantic_type(dtype: Any, sample_values: pd.Series = None) -> Any:
    if pd.api.types.is_integer_dtype(dtype):
        return Optional[int]
    elif pd.api.types.is_float_dtype(dtype):
        return Optional[float]
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


def chunk_list(lst, chunk_size):
    """
    Splits a list into a list of lists, each of a given size.

    Args:
        lst (list): The list to split.
        chunk_size (int): The size of each chunk.

    Returns:
        list of lists: A list where each element is a sublist of length `chunk_size`, except possibly the last one.
    """
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


def pydantic_model_from_dict(dict) -> type[BaseModel]:
    model_name = "AType#" + ":".join(dict.keys())
    fields = {}

    for col in dict.keys():
        sample_value = dict[col]
        pydantic_type = infer_pydantic_type(
            type(sample_value), sample_values=[sample_value]
        )
        fields[col] = (pydantic_type, Field(default=None))
    new_fields = {}
    for field, value in fields.items():
        new_fields[sanitize_field_name(field)] = value

    return create_model(model_name, **new_fields)


def pydantic_model_from_jsonl(
    file_path: str, sample_size: int = 100
) -> type[BaseModel]:
    df = pd.read_json(file_path, lines=True, nrows=sample_size, encoding="utf-8")

    model_name = "AType#" + ":".join(df.columns)
    fields = {}

    for col in df.columns:
        sample_values = df[col].head(5)
        pydantic_type = infer_pydantic_type(df[col].dtype, sample_values=sample_values)
        fields[col] = (pydantic_type, Field(default=None))
    new_fields = {}
    for field, value in fields.items():
        new_fields[sanitize_field_name(field)] = value

    return create_model(model_name, **new_fields)


def pydantic_model_from_dataframe(
    dataframe: DataFrame, sample_size: int = 100
) -> Type[BaseModel]:
    df_sample = dataframe.head(sample_size)

    model_name = "AType#" + ":".join(df_sample.columns)
    fields = {}
    for col in df_sample.columns:
        pydantic_type = infer_pydantic_type(df_sample[col].dtype)
        fields[col] = (pydantic_type, Field(default=None))

    return create_model(model_name, **fields)


def get_pydantic_fields(model: Type[BaseModel]) -> List[Tuple[str, str, str]]:
    """
    Extract field names, type strings, and descriptions from a Pydantic model.

    Returns:
        A list of tuples: (field_name, type_string, description)
    """
    hints = get_type_hints(model)
    fields_info = model.model_fields

    result = []
    for name, field in fields_info.items():
        field_type = str(hints.get(name, str))
        description = field.description or None
        result.append((name, field_type, description))

    return result


def get_pydantic_fields2(model: Type[BaseModel]) -> List[Tuple[str, str]]:
    hints = get_type_hints(model)
    return [(name, str(hints[name])) for name in model.model_fields]


def extract_pydantic_from_api_spec(
    schema_dict: dict, model_name: str
) -> Type[BaseModel]:
    # Load the raw schema (not wrapped)
    return json_create_model(schema_dict)


def extract_schema_from_api_spec(schema_dict: dict) -> List[Tuple[str, str, str]]:
    # Get the schema directly or via 'schema' key
    schema = schema_dict[0] if type(schema_dict) == tuple else schema_dict

    # Proceed only if 'properties' exists
    properties = schema.get("properties")
    if not properties:
        properties = schema
    required_fields = schema.get("required", [])  # <- this is key
    result = []

    for field_name, field_info in properties.items():
        type_name = field_info.get("type", "Any")
        description = field_info.get("description", field_info.get("title", ""))
        is_required = field_name in required_fields
        result.append((field_name, type_name, description, is_required))

    return create_pydantic_model(result)

    # properties = schema_dict.get("schema", {}).get("properties", {})
    # fields = []
    # for field_name, field_info in properties.items():
    #     type_name = field_info.get("type", "Any")
    #     description = field_info.get("description", field_info.get("title", ""))
    #     fields.append((field_name, type_name, description))
    # return create_pydantic_model(fields)


def create_pydantic_model(
    fields: List[Tuple[str, str, str, bool]], name: str = None
) -> Type[BaseModel]:
    """
    Dynamically create a Pydantic model from a list of field definitions.

    Args:
        fields: A list of (field_name, type_name, description) tuples.
        name: Optional name of the model.

    Returns:
        A dynamically created Pydantic model class.
    """
    type_mapping = {
        "string": str,
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "list": list,
        "dict": dict,
        "Optional[str]": str,
        "Optional[int]": int,
        # Extend with more types as needed
    }

    if not name:
        model_name = "AType#" + ":".join([x[0] for x in fields])
    else:
        model_name = name

    field_definitions = {}
    for field_name, type_name, description, required in fields:
        # ptype = type_mapping.get(model_name, str)  # default to str if unknown

        ptype = type_mapping[type_name] if type_name in type_mapping else Any
        if required:
            field_definitions[field_name] = (ptype, ...)
        else:
            field_definitions[field_name] = (Optional[ptype], None)
    return create_model(model_name, **field_definitions)


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


# TODO utilize the best-of-n generations and logprobs.
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

    new_name = rename_type or f"{model_cls.__name__}Optional"
    return create_model(new_name, **fields)


def are_models_structurally_identical(
    model1: type[BaseModel], model2: type[BaseModel]
) -> bool:
    if not issubclass(model1, BaseModel) or not issubclass(model2, BaseModel):
        return False

    fields1 = model1.model_fields
    fields2 = model2.model_fields

    if set(fields1.keys()) != set(fields2.keys()):
        return False

    for field_name in fields1:
        f1 = fields1[field_name]
        f2 = fields2[field_name]
        if f1.annotation != f2.annotation:
            return False
        # Optional: also check default, required, metadata etc.
        if f1.default != f2.default:
            return False
        # if f1.is_required != f2.is_required:
        #     return False

    return True


def pretty_print_atype(atype, indent: int = 2):
    """
    Recursively pretty print an 'atype' (Agentics/Pydantic typing model).
    Works on generics like list[int], dict[str, float], Optional[...], etc.
    """
    prefix = " " * indent

    origin = get_origin(atype)
    args = get_args(atype)

    if origin is None:
        # Base case: a plain class/type
        print(f"{prefix}{atype}")
    else:
        print(f"{prefix}{origin.__name__}[")
        for arg in args:
            pretty_print_atype(arg, indent + 2)
        print(f"{prefix}]")
