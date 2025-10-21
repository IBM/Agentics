# app.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, get_args, get_origin

import streamlit as st
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, create_model

from agentics.core.atype import (
    get_pydantic_fields,
    import_pydantic_from_code,
    normalize_type_label,
)

load_dotenv()
# -----------------------
# Helpers
# -----------------------


# --- helpers (place once) ----------------------------------------------------
def ensure_label_in_options(
    curr_label: str, base_options: list[str]
) -> tuple[list[str], int]:
    """Guarantee curr_label is selectable; return (options, index)."""
    options = list(base_options)  # copy
    if curr_label not in options:
        options.append(curr_label)
    try:
        idx = options.index(curr_label)
    except ValueError:
        idx = 0
    return options, idx


# -----------------------------------------------------------------------------

SIMPLE_TYPES = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "date": "date",
    "datetime": "datetime",
    "Any": Any,
}

LIST_TYPES = {
    "list[str]": list[str],
    "list[int]": list[int],
    "list[float]": list[float],
    "list[bool]": list[bool],
}


# Lazy imports (date/datetime are optional until needed)
def _resolve_builtin(name: str):
    if name == "date":
        import datetime

        return datetime.date
    if name == "datetime":
        import datetime

        return datetime.datetime
    raise KeyError(name)


def parse_field_type(label: str):
    """Map UI label -> Python type."""
    if label in SIMPLE_TYPES:
        t = SIMPLE_TYPES[label]
        return _resolve_builtin(t) if isinstance(t, str) else t
    if label in LIST_TYPES:
        return LIST_TYPES[label]
    if label.startswith("Literal["):
        # e.g. "Literal['A','B','C']"
        from typing import Literal

        # Very small safe parser: expect a python-ish list of comma-separated items
        items_str = label[len("Literal[") : -1]
        parts = [p.strip() for p in items_str.split(",") if p.strip()]
        # Allow quotes or bare numbers/bools
        parsed = []
        for p in parts:
            if (p.startswith("'") and p.endswith("'")) or (
                p.startswith('"') and p.endswith('"')
            ):
                parsed.append(p[1:-1])
            else:
                # try number/bool
                if p.lower() in ("true", "false"):
                    parsed.append(p.lower() == "true")
                else:
                    try:
                        parsed.append(int(p))
                    except ValueError:
                        try:
                            parsed.append(float(p))
                        except ValueError:
                            parsed.append(p)  # fallback as string
        return Literal[tuple(parsed)]  # type: ignore
    return Any


def type_to_str(t: Any) -> str:
    """Pretty-print a Python type into a class annotation string."""
    origin = get_origin(t)
    if origin is list:
        args = get_args(t)
        inner = type_to_str(args[0]) if args else "Any"
        return f"list[{inner}]"
    if t in (str, int, float, bool, Any):
        return t.__name__ if t is not Any else "Any"
    if t.__class__.__name__ == "NoneType":
        return "None"
    if hasattr(t, "__name__"):
        return t.__name__
    return repr(t)


def make_class_code(fields_spec: list[dict], model_name: str = None) -> str:
    """Generate a Pydantic class definition string from the UI spec."""

    model_name = model_name or "GeneratedModel"

    lines = [
        "from __future__ import annotations",
        "from pydantic import BaseModel, Field",
        "from typing import Any",
    ]
    # Check if we need date/datetime or typing extras
    needs_date = any(fs["type_label"] == "date" for fs in fields_spec)
    needs_datetime = any(fs["type_label"] == "datetime" for fs in fields_spec)
    if needs_date or needs_datetime:
        lines.append("import datetime")

    # Collect any Literal[...] labels (no extra import needed on 3.11+, but import for portability)
    if any(fs["type_label"].startswith("Literal[") for fs in fields_spec):
        lines.append("from typing import Literal")

    lines.append("")
    lines.append(f"class {model_name}(BaseModel):")
    if not fields_spec:
        lines.append("    pass")
        return "\n".join(lines)

    for fs in fields_spec:
        fname = fs["name"]
        t = parse_field_type(fs["type_label"])
        t_str = type_to_str(t)

        optional = fs["optional"]
        desc = fs["description"].strip()
        has_default = fs["use_default"]
        default_val = fs["default_value"]

        ann = t_str if not optional else f"{t_str} | None"

        # Build right-hand side
        if has_default and default_val != "":
            # Try to cast default to python value for str/int/float/bool
            dv = default_val
            if t in (int, float):
                try:
                    dv = t(default_val)
                except Exception:
                    pass
            elif t is bool:
                dv = str(default_val).strip().lower() in ("1", "true", "yes", "y", "on")
            rhs = f"Field({repr(dv)}, description={repr(desc)})" if desc else repr(dv)
        else:
            # No explicit default
            if optional:
                rhs = (
                    f"Field(default=None, description={repr(desc)})" if desc else "None"
                )
            else:
                rhs = f"Field(..., description={repr(desc)})" if desc else "Field(...)"

        lines.append(f"    {fname}: {ann} = {rhs}")

    return "\n".join(lines)


def build_model(model_name: str, fields_spec: list[dict]):
    """Create a Pydantic model dynamically from the UI spec."""
    field_defs: dict[str, tuple[Any, Any]] = {}

    for fs in fields_spec:
        fname = fs["name"]
        t = parse_field_type(fs["type_label"])
        optional = fs["optional"]
        desc = fs["description"].strip()
        has_default = fs["use_default"]
        default_val = fs["default_value"]

        ann = t if not optional else (t | None)

        if has_default and default_val != "":
            dv = default_val
            if t in (int, float):
                try:
                    dv = t(default_val)
                except Exception:
                    pass
            elif t is bool:
                dv = str(default_val).strip().lower() in ("1", "true", "yes", "y", "on")
            field_defs[fname] = (ann, Field(dv, description=desc or None))
        else:
            if optional:
                field_defs[fname] = (ann, Field(default=None, description=desc or None))
            else:
                field_defs[fname] = (ann, Field(..., description=desc or None))

    Model = create_model(model_name, **field_defs)  # type: ignore
    return Model


def pydantic_model_bulilder_ui():
    """Run the Streamlit app."""

    type_options = (
        list(SIMPLE_TYPES.keys()) + list(LIST_TYPES.keys()) + ["Literal['A','B','C']"]
    )

    if "fields" not in st.session_state:
        st.session_state.fields = []
    if "pydantic_class" not in st.session_state:
        st.session_state.pydantic_class = None

    st.set_page_config(
        page_title="Pydantic Model Builder", page_icon="🧱", layout="wide"
    )

    # st.session_state.model_name = (st.session_state.new_model_name if st.session_state.new_model_name!="NA" else None) \
    #             or (st.session_state.selected_model.split(".")[0] if st.session_state.selected_model!="NA" else "GeneratedModel")

    st.markdown("### 🧱 Visual Pydantic Model Builder" "")
    col1, col2 = st.columns([0.5, 0.5], gap="large")
    with col1:
        st.markdown("#### Select existing model")
        options = [
            f.split(".")[0]
            for f in os.listdir(Path(__file__).resolve().parent / "predefined_types")
            if f.endswith(".py") and not f.startswith("__")
        ]
        st.session_state.selected_model = st.selectbox(
            "Predefined Models", options=options
        )

        load_model = st.button("Load Selected Model")
        delete_model = st.button("Delete Selected Model")
        new_model = st.button("Create New Model")

        if new_model:
            st.session_state.selected_model = "NA"
            st.session_state.new_model_name = "NA"
            st.session_state.pydantic_class = None
            st.session_state.fields = []
            st.rerun()

        if load_model and st.session_state.selected_model != "NA":
            with open(
                Path(__file__).resolve().parent
                / "predefined_types"
                / (st.session_state.selected_model + ".py"),
                "r",
            ) as f:
                st.session_state.code = f.read()
                st.session_state.new_model_name = st.session_state.selected_model
                st.session_state.pydantic_class = import_pydantic_from_code(
                    st.session_state.code
                )
                st.session_state.fields = get_pydantic_fields(
                    st.session_state.pydantic_class
                )
                st.rerun()
        if delete_model and st.session_state.selected_model != "NA":
            os.remove(
                Path(__file__).resolve().parent
                / "predefined_types"
                / (st.session_state.selected_model + ".py")
            )
            st.session_state.selected_model = "NA"
            st.session_state.new_model_name = "NA"
            st.session_state.pydantic_class = None
            st.session_state.fields = []
            st.success("Deleted model.")
            st.rerun()

        st.markdown("#### Edit Type Fields")

        if len(st.session_state.fields) > 0:
            st.markdown("### Fields")
            to_delete = []

            for i, fs in enumerate(st.session_state.fields):
                with st.expander(
                    f"Field #{i+1}: `{fs.get('name','')}`", expanded=False
                ):
                    c1, c2, c4 = st.columns([0.4, 0.4, 0.2])

                    # Name
                    fs["name"] = c1.text_input(
                        "Name", value=fs.get("name", ""), key=f"name_{i}"
                    )

                    # Normalize incoming type; detect optional from the label itself
                    raw_label = fs.get("type_label", "str")
                    core_label, is_opt_detected = normalize_type_label(raw_label)

                    # Ensure the (normalized) label is available in the select options
                    _options, _idx = ensure_label_in_options(core_label, type_options)

                    # Type select
                    fs["type_label"] = c2.selectbox(
                        "Type",
                        options=_options,
                        index=_idx,
                        key=f"type_{i}",
                    )

                    fs["use_default"] = c4.checkbox(
                        "Default?",
                        value=bool(fs.get("use_default", False)),
                        key=f"default_flag_{i}",
                    )

                    # Default value input (enabled only if "Default?" is on)
                    fs["default_value"] = st.text_input(
                        "Default (as text)",
                        value=fs.get("default_value", ""),
                        key=f"default_val_{i}",
                        disabled=not fs["use_default"],
                    )

                    # Description
                    fs["description"] = st.text_area(
                        "Description",
                        value=fs.get("description", ""),
                        key=f"desc_{i}",
                        height=80,
                    )

                    # Delete button
                    if st.button("🗑️ Remove this field", key=f"del_{i}"):
                        to_delete.append(i)

            if to_delete:
                for idx in sorted(to_delete, reverse=True):
                    del st.session_state.fields[idx]
                st.rerun()
        else:
            st.info("Add fields from the sidebar to begin.")

        with st.popover("Add field"):

            new_name = st.text_input(
                "Field name", key="new_field_name", placeholder="e.g., title"
            )
            new_type = st.selectbox(
                "Field type", options=type_options, key="new_field_type", index=0
            )
            # new_optional = st.checkbox("Optional", key="new_field_optional", value=False)
            new_use_default = st.checkbox(
                "Set default value", key="new_use_default", value=False
            )
            new_default_value = st.text_input(
                "Default (as text)",
                key="new_default_value",
                disabled=not new_use_default,
            )
            new_description = st.text_area(
                "Description (optional)", key="new_desc", height=80
            )

            if st.button("➕ Add field"):
                if not new_name.strip():
                    st.warning("Please provide a field name.")
                else:
                    st.session_state.fields.append(
                        {
                            "name": new_name.strip(),
                            "type_label": new_type,
                            "optional": True,
                            "use_default": new_use_default,
                            "default_value": new_default_value,
                            "description": new_description,
                        }
                    )
                    st.success(f"Added field `{new_name.strip()}`")
                    st.rerun()
        st.session_state.new_model_name = st.text_input("New Model Name")
        save_model = st.button("Save Model")
        if save_model and st.session_state.fields:
            if st.session_state.selected_model == "NA":
                st.warning(
                    "Please select a predefined model to overwrite, or create a new one."
                )
            else:
                output_path = Path(
                    "./applications/macro_economic_impact/predefined_types"
                ) / (st.session_state.new_model_name + ".py")
                with open(output_path, "w") as f:
                    f.write(
                        make_class_code(
                            st.session_state.fields,
                            model_name=st.session_state.new_model_name,
                        )
                    )
                st.success(f"Saved model to predefined_types/{output_path.name}")

    with col2:

        tabs = st.tabs(
            ["🧾 Generated Class Code", "▶️ Preview & Validate", "🧩 JSON Schema"]
        )

        if st.session_state.fields:

            with tabs[0]:

                st.code(st.session_state.code, language="python")
                st.download_button(
                    "⬇️ Download as .py",
                    data=st.session_state.code,
                    file_name=f"{st.session_state.selected_model}",
                    mime="text/x-python",
                )

            with tabs[1]:

                colA, colB = st.columns(2)
                with colA:
                    st.markdown("**Enter sample JSON to validate**")
                    sample = st.text_area(
                        "JSON input",
                        value=json.dumps(
                            {f["name"]: None for f in st.session_state.fields}, indent=2
                        ),
                        height=220,
                    )
                    if st.button("Validate JSON"):
                        try:
                            data = json.loads(sample)
                            obj = st.session_state.pydantic_class.model_validate(data)
                            st.success("Validation succeeded.")
                            st.json(obj.model_dump())
                        except (json.JSONDecodeError, ValidationError, Exception) as e:
                            st.error(f"Validation failed:\n\n{e}")

                with colB:
                    st.markdown("**Create an instance with a small form**")
                    form_vals = {}
                    for f in st.session_state.fields:
                        label = f["name"]
                        t = parse_field_type(f["type_label"])
                        if t is bool:
                            form_vals[label] = st.checkbox(label, value=False)
                        else:
                            form_vals[label] = st.text_input(label, "")
                    if st.button("Build instance from form"):
                        # naive casting: try int/float/bool; leave strings otherwise
                        casted = {}
                        for f in st.session_state.fields:
                            name = f["name"]
                            val = form_vals[name]
                            t = parse_field_type(f["type_label"])
                            if val == "":
                                casted[name] = None
                            else:
                                if t is int:
                                    try:
                                        casted[name] = int(val)
                                    except:
                                        casted[name] = val
                                elif t is float:
                                    try:
                                        casted[name] = float(val)
                                    except:
                                        casted[name] = val
                                elif t is bool:
                                    casted[name] = str(val).strip().lower() in (
                                        "1",
                                        "true",
                                        "yes",
                                        "y",
                                        "on",
                                    )
                                else:
                                    casted[name] = val
                        try:
                            obj = st.session_state.pydantic_class.model_validate(casted)
                            st.success("Instance created:")
                            st.json(obj.model_dump())
                        except ValidationError as e:
                            st.error(str(e))

            with tabs[2]:
                st.session_state.pydantic_class = build_model(
                    st.session_state.selected_model.split(".")[0],
                    st.session_state.fields,
                )
                st.json(st.session_state.pydantic_class.model_json_schema())
