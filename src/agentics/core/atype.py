import csv
import datetime
import json
import types
from typing import (
    Annotated,
    Any,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

import pandas as pd
from pydantic import BaseModel, Field, create_model

from agentics.core.utils import sanitize_field_name


class AGString(BaseModel):
    string: Optional[str] = None


#################
##### Utils #####
#################


def copy_attribute_values(
    state: BaseModel, source_attribute: str, target_attribute: str
) -> BaseModel:
    """for each state, copy the value from source_attribute to the target_attribute
    Usage: for generating fewshots,
    copy values for the target_attribute from source_attribute that holds the ground_truth.
    """
    source_value = getattr(state, source_attribute)
    setattr(state, target_attribute, source_value)
    return state


from typing import Type

import pandas as pd
from pydantic import BaseModel


def get_pydantic_fields(atype: Type[BaseModel]):
    """
    Extract Pydantic model fields and return them in the same
    structure used by Streamlit's st.session_state.fields.
    """
    fields_list = []

    for field_name, field in atype.model_fields.items():
        # Determine if optional
        optional = field.is_required() is False

        # Extract annotation (clean type string)
        type_label = str(field.annotation)
        # remove typing artifacts like "<class 'int'>" -> "int"
        if type_label.startswith("<class"):
            type_label = type_label.split("'")[1]

        # Default handling
        has_default = field.default is not None or field.default_factory is not None
        default_val = None

        if field.default_factory is not None:
            default_val = f"{field.default_factory.__name__}()"
        elif field.default is not None and field.default is not Ellipsis:
            default_val = field.default

        # Add to list
        fields_list.append(
            {
                "name": field_name,
                "type_label": type_label,
                "optional": optional,
                "use_default": has_default,
                "default_value": default_val,
                "description": field.description or "",
            }
        )

    return fields_list


def get_active_fields(state: BaseModel, allowed_fields: Set[str] = None) -> Set[str]:
    """
    Returns the set of fields in `state` that are not None and optionally intersect with allowed_fields.
    """
    active_fields = {
        k for k, v in state.model_dump().items() if v is not None and v != ""
    }
    return active_fields & allowed_fields if allowed_fields else active_fields


import io
import os
from typing import IO


def pydantic_model_from_csv(
    file_source: Union[str, os.PathLike, IO[str], IO[bytes], object],
) -> type[BaseModel]:
    """
    Generate a Pydantic model dynamically from a CSV header.

    Accepts:
      - A file path (str or Path)
      - A binary or text stream (e.g., BytesIO, StringIO)
      - A Streamlit UploadedFile
      - A string containing raw CSV data
    """

    # Normalize source into a text stream
    def _to_text_stream(src) -> IO[str]:
        # --- Case 1: Path on disk ---
        if isinstance(src, (str, os.PathLike)) and os.path.exists(src):
            return open(src, "r", encoding="utf-8", newline="")

        # --- Case 2: Raw string with CSV content ---
        if isinstance(src, str) and "\n" in src:
            return io.StringIO(src)

        # --- Case 3: Streamlit UploadedFile or BytesIO ---
        if hasattr(src, "getbuffer"):
            return io.StringIO(src.getbuffer().tobytes().decode("utf-8"))
        if hasattr(src, "getvalue"):
            return io.StringIO(src.getvalue().decode("utf-8"))

        # --- Case 4: Already text stream ---
        if isinstance(src, io.TextIOBase):
            src.seek(0)
            return src

        # --- Case 5: Binary stream ---
        if isinstance(src, (io.BytesIO, io.BufferedIOBase, io.RawIOBase)):
            src.seek(0)
            return io.TextIOWrapper(src, encoding="utf-8", newline="")

        raise TypeError(f"Unsupported input type: {type(src).__name__}")

    f = _to_text_stream(file_source)
    close_after = isinstance(file_source, (str, os.PathLike)) and os.path.exists(
        file_source
    )

    try:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV file appears to have no header row.")

        columns = [sanitize_field_name(x) for x in reader.fieldnames]
        model_name = "AType#" + ":".join(columns)
        fields = {col: (Optional[str], None) for col in columns}

        return create_model(model_name, **fields)
    finally:
        if close_after:
            f.close()


def infer_pydantic_type(dtype: Any, sample_values: pd.Series = None) -> Any:
    is_dict_mask = sample_values.apply(lambda x: isinstance(x, dict))

    if pd.api.types.is_integer_dtype(dtype):
        return Optional[int]
    elif pd.api.types.is_float_dtype(dtype):
        return Optional[float]
    elif pd.api.types.is_bool_dtype(dtype):
        return Optional[bool]
    elif is_dict_mask.all():
        return Optional[dict]
    elif pd.api.types.is_list_like(dtype):
        return Optional[list]
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
    dataframe: pd.DataFrame, sample_size: int = 100
) -> Type[BaseModel]:
    df_sample = dataframe.head(sample_size)

    model_name = "AType#" + ":".join(df_sample.columns)
    fields = {}
    for col in df_sample.columns:
        pydantic_type = infer_pydantic_type(df_sample[col].dtype)
        fields[col] = (pydantic_type, Field(default=None))

    return create_model(model_name, **fields)


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
    print(fields)
    for field_name, type_name, description, required in fields:
        ptype = type_mapping[type_name] if type_name in type_mapping else Any
        if required:
            field_definitions[field_name] = (ptype, ...)
        else:
            field_definitions[field_name] = (Optional[ptype], None)
    return create_model(model_name, **field_definitions)


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
        # Datetime utilities
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


def normalize_type_label(label: str | None) -> tuple[str, bool]:
    """
    Normalize various annotation spellings to UI labels and detect Optional:
    - <class 'int'>           -> ("int", False)
    - typing.List[str]        -> ("list[str]", False)
    - datetime.date           -> ("date", False)
    - Optional[int]           -> ("int", True)
    - Union[int, None]        -> ("int", True)
    - int | None              -> ("int", True)
    - Literal['A','B']        -> ("Literal['A','B']", False)
    """

    def _base_normalize(s: str) -> str:
        # <class 'int'> -> int
        if s.startswith("<class '") and s.endswith("'>"):
            return s.split("'")[1]

        # strip typing. prefixes
        s = s.replace("typing.", "")

        # datetime -> short labels
        s = s.replace("datetime.date", "date").replace("datetime.datetime", "datetime")

        # List/Dict/Tuple -> lowercase generics
        s = (
            s.replace("List[", "list[")
            .replace("Dict[", "dict[")
            .replace("Tuple[", "tuple[")
        )

        # Canonicalize list[...] inner
        if s.startswith("list[") and s.endswith("]"):
            inner = s[5:-1].strip()
            inner = (
                inner.replace("typing.", "")
                .replace("datetime.date", "date")
                .replace("datetime.datetime", "datetime")
            )
            if inner.startswith("<class '") and inner.endswith("'>"):
                inner = inner.split("'")[1]
            return f"list[{inner}]"

        # Literal[...] keep as-is
        if s.startswith("Literal[") and s.endswith("]"):
            return s

        # NoneType -> None
        s = s.replace("NoneType", "None")
        return s

    if not label:
        return ("str", False)

    s = str(label).strip().replace("typing.", "")

    # --- Optional forms detection ---
    # Optional[T]
    if s.startswith("Optional[") and s.endswith("]"):
        core = s[len("Optional[") : -1].strip()
        return (_base_normalize(core), True)

    # Union[T, None] or Union[None, T]
    if s.startswith("Union[") and s.endswith("]"):
        inner = s[len("Union[") : -1]
        parts = [p.strip() for p in inner.split(",")]
        parts = [p.replace("NoneType", "None") for p in parts]
        if "None" in parts and len(parts) == 2:
            core = parts[0] if parts[1] == "None" else parts[1]
            return (_base_normalize(core), True)
        # Non-optional unions: normalize but keep as-is
        return (_base_normalize(f"Union[{inner}]"), False)

    # PEP 604: T | None
    if " | None" in s:
        core = s.replace(" | None", "").strip()
        return (_base_normalize(core), True)

    # Not optional
    return (_base_normalize(s), False)


import html


def pydantic_to_markdown(obj: BaseModel, title: str | None = None) -> str:
    """
    Pretty-print a Pydantic model instance as a Markdown table,
    safely rendering nested JSON inside HTML <pre><code> blocks
    (so it works inside tables and Streamlit).
    """
    if not isinstance(obj, BaseModel):
        raise TypeError("Expected a Pydantic BaseModel instance.")

    data = obj.model_dump()
    lines = []

    if title:
        lines.append(f"### {title}\n")

    lines.append("| **Field** | **Value** |")
    lines.append("|------------|------------|")

    for key, value in data.items():
        if isinstance(value, (dict, list)):
            # Pretty JSON with safe HTML escaping
            json_str = json.dumps(value, indent=2, ensure_ascii=False)
            formatted = f"<pre><code>{html.escape(json_str)}</code></pre>"
        else:
            formatted = str(value) if value is not None else "—"
        lines.append(f"| `{key}` | {formatted} |")

    return "\n".join(lines)
