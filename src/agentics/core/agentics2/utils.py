from __future__ import annotations

from typing import Any, Dict, Type, get_type_hints

from pydantic import BaseModel, create_model
from pydantic.fields import FieldInfo


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
