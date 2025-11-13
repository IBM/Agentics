from __future__ import annotations

from typing import Any, Dict, Optional, Type, get_type_hints

from pydantic import BaseModel, create_model
from pydantic.fields import FieldInfo


def pydantic_models_from_function(fn):
    """
    Build:
    - InputModel:
        * if the function has exactly 1 param and it's already a Pydantic model,
          use that model directly
        * otherwise, build a synthetic Input model with one field per parameter
    - Target:
        * from return annotation, but ALL fields optional
    """
    sig = inspect.signature(fn)
    hints = get_type_hints(fn)

    # ----- figure out the input model -----
    params = list(sig.parameters.items())
    single_param_name = params[0][0] if len(params) == 1 else None
    single_param_ann = hints.get(single_param_name) if single_param_name else None

    if (
        len(params) == 1
        and isinstance(single_param_ann, type)
        and issubclass(single_param_ann, BaseModel)
    ):
        # user already said: def f(x: MyModel)
        InputModel = single_param_ann
    else:
        # build a composite pydantic model from all params
        input_fields = {}
        for name, param in sig.parameters.items():
            ann = hints.get(name, Any)
            if param.default is inspect._empty:
                input_fields[name] = (ann, ...)
            else:
                input_fields[name] = (ann, param.default)
        InputModel = create_model(f"{fn.__name__}Input", **input_fields)

    # ----- figure out the target model -----
    ret_ann = hints.get("return", Any)

    if isinstance(ret_ann, type) and issubclass(ret_ann, BaseModel):
        # clone as all-optional
        optional_fields = {
            f_name: (Optional[f_field.annotation], None)
            for f_name, f_field in ret_ann.model_fields.items()
        }
        Target = create_model(f"{fn.__name__}Target", **optional_fields)
    else:
        # single optional field called "result"
        Target = create_model(
            f"{fn.__name__}Target",
            result=(Optional[ret_ann], None),
        )

    return InputModel, Target


import ast
import inspect


def has_explicit_return_none(fn) -> bool:
    """
    Return True if the function has an explicit 'return None' or bare 'return'.
    """
    tree = ast.parse(inspect.getsource(fn))
    for node in ast.walk(tree):
        if isinstance(node, ast.Return):
            # bare `return`
            if node.value is None:
                return True
            # explicit `return None`
            if isinstance(node.value, ast.Constant) and node.value.value is None:
                return True
    return False


def get_function_io_types(fn: Callable) -> Tuple[Dict[str, Any], Any]:
    """
    Given a function like:
        async def f(state: EmailInput) -> Email: ...
    return:
        ({"state": EmailInput}, Email)
    Works for sync/async, 1+ params, and falls back to Any.
    """
    if not callable(fn):
        raise TypeError(f"{fn} is not callable")

    sig = inspect.signature(fn)
    hints = get_type_hints(fn)

    input_types: Dict[str, Any] = {}

    for name, param in sig.parameters.items():
        # if the parameter is annotated, use that
        if name in hints:
            input_types[name] = hints[name]
        else:
            # no annotation → Any
            input_types[name] = Any

    # return annotation (may be missing)
    output_type = hints.get("return", Any)

    return input_types, output_type


def pydantic_models_from_function(fn):
    """
    Build:
    - InputModel:
        * if the function has exactly 1 param and it's already a Pydantic model,
          use that model directly
        * otherwise, build a synthetic Input model with one field per parameter
    - Target:
        * from return annotation, but ALL fields optional
    """
    sig = inspect.signature(fn)
    hints = get_type_hints(fn)

    # ----- figure out the input model -----
    params = list(sig.parameters.items())
    single_param_name = params[0][0] if len(params) == 1 else None
    single_param_ann = hints.get(single_param_name) if single_param_name else None

    if (
        len(params) == 1
        and isinstance(single_param_ann, type)
        and issubclass(single_param_ann, BaseModel)
    ):
        # user already said: def f(x: MyModel)
        InputModel = single_param_ann
    else:
        # build a composite pydantic model from all params
        input_fields = {}
        for name, param in sig.parameters.items():
            ann = hints.get(name, Any)
            if param.default is inspect._empty:
                input_fields[name] = (ann, ...)
            else:
                input_fields[name] = (ann, param.default)
        InputModel = create_model(f"{fn.__name__}Input", **input_fields)

    # ----- figure out the target model -----
    ret_ann = hints.get("return", Any)

    if isinstance(ret_ann, type) and issubclass(ret_ann, BaseModel):
        # clone as all-optional
        optional_fields = {
            f_name: (Optional[f_field.annotation], None)
            for f_name, f_field in ret_ann.model_fields.items()
        }
        Target = create_model(f"{fn.__name__}Target", **optional_fields)
    else:
        # single optional field called "result"
        Target = create_model(
            f"{fn.__name__}Target",
            result=(Optional[ret_ann], None),
        )

    return InputModel, Target


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


from typing import get_args, get_origin


def unwrap_optional(t):
    if get_origin(t) is not None:
        args = [a for a in get_args(t) if a is not type(None)]
        if args:
            return args[0]
    return t
