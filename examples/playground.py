"""Playground to experience Agentics on your data

To run the file:
1. >> `poetry install --with playground`
2. >> `cd examples/`
3. >> `streamlit run playground.py`
"""

import asyncio
import json
from typing import Any, List, Literal, Optional  # noqa

import numpy as np
import pandas as pd
import pyarrow as pa
import streamlit as st
import streamlit_antd_components as sac
from pydantic import BaseModel, Field, create_model
from sklearn.model_selection import train_test_split
from streamlit.dataframe_util import convert_anything_to_pandas_df
from streamlit.elements.lib.column_config_utils import determine_dataframe_schema
from streamlit.elements.widgets.data_editor import _apply_dataframe_edits

from agentics import Agentics as AG
from agentics.core.utils import make_all_fields_optional

LLM = None  # import and replace eg. `from agentics import ollama_llm`

N_SAMPLES = 20
TYPE_OPTIONS = ["`str`", "`int`", "`float`", "`bool`", "`Literal`"]


class AGType(BaseModel):
    """Placeholder class to infer the field type and description."""

    FieldName: Optional[str] = None
    FieldValues: Optional[List[Any]] = None
    FieldType: Optional[Any] = Field("Optional[str]")
    FieldDescription: Optional[str] = None


AGType_instruction = (
    "For the given sources: '{sources}', generalise the target FieldType and FieldDescription categories, where the "
    "field relates to a pydantic field type, i.e. {type_options} and the declarative description "
    "note categorical fields are given as lists or tuples of types, but generalise them"
    "is tells you more about information about the data values. "
    "If you are unsure or any of the sources are empty or '', make the FieldType an `Optional[str]`"
)


def _editor_to_df() -> pd.DataFrame:
    df = st.session_state.get("self_transduction_types")
    edited_df = st.session_state.get("self_transduction_types_edited")
    data_df = convert_anything_to_pandas_df(df)
    df_schema = determine_dataframe_schema(
        data_df, pa.Table.from_pandas(data_df).schema
    )
    _apply_dataframe_edits(data_df, edited_df, df_schema)
    st.session_state["types_ag"] = AG.from_dataframe(data_df, atype=AGType)


def _set_instruction(ag: AG, source_fields: List[str], target_fields: List[str]) -> str:
    _source_fields = ""
    for field in source_fields:
        description = ag.atype.model_fields[field].description
        _source_fields += f"{'`' + str(field) + '`' if not description else '`' + str(field) + '` (' + str(description) + ')'}, "
    _source_fields = _source_fields[:-2]
    _target_fields = ""
    for field in target_fields:
        description = ag.atype.model_fields[field].description
        _target_fields += f"{'`' + str(field) + '`' if not description else '`' + str(field) + '` (' + str(description) + ')'}, "
    _target_fields = _target_fields[:-2]
    ag.instructions = st.text_area(
        "Instruction",
        f"Given the source fields: {_source_fields}, "
        f"generate the target fields: {_target_fields}. ",
        height=150,
    )


def _highlight_missing(index, **kwargs):
    if index.name in kwargs["target_fields"]:
        return ["background-color: #a7F0ba"] * len(index)
    return [""] * len(index)


@st.fragment()
def self_transduction(df: pd.DataFrame):
    if (types_ag := st.session_state.get("types_ag")) is None:
        sources = list(df.columns)
        types_ag = AG(
            atype=AGType,
            instructions=AGType_instruction.format(
                sources=sources, type_options=TYPE_OPTIONS
            ),
            llm=LLM,
        )
        for source in sources:
            types_ag.states.append(
                AGType(FieldName=source, FieldValues=df[source].to_list())
            )

    column_config = {
        "FieldName": st.column_config.TextColumn("Field"),
        "FieldType": st.column_config.TextColumn("Type"),
        "FieldDescription": st.column_config.TextColumn("Description", width="large"),
    }
    if len(types_ag) == len(df.columns):
        if st.button("Describe", icon="🪄"):
            source_ph = st.empty()
            with source_ph.status("Filling in the descriptions..."):
                types_ag = asyncio.run(
                    types_ag.self_transduction(
                        ["FieldName", "FieldValues"], ["FieldType", "FieldDescription"]
                    )
                )
                st.session_state["self_transduction_types"] = types_ag.to_dataframe()
            source_ph.empty()

    self_transduction_types = st.session_state["self_transduction_types"] = (
        types_ag.to_dataframe()
    )
    types_df = st.data_editor(
        self_transduction_types,
        column_order=["FieldName", "FieldType", "FieldDescription"],
        num_rows="dynamic",
        row_height=75,
        column_config=column_config,
        use_container_width=True,
        key="self_transduction_types_edited",
        on_change=_editor_to_df,
    )
    source_fields = list(df.columns)
    target_fields = list(
        set([state.FieldName for state in types_ag]).difference(set(source_fields))
    )
    if target_fields:
        fields = {}
        for _, col in types_df.iterrows():
            fields[col.FieldName] = (
                col.FieldType,
                Field(default=None, description=col.FieldDescription),
            )
        try:
            atype = make_all_fields_optional(create_model("Transduction", **fields))
            transduction_ag = AG.from_dataframe(df, llm=LLM)
            transduction_ag = transduction_ag.rebind_atype(atype)
            _set_instruction(transduction_ag, source_fields, target_fields)

            if st.button("Transduce", icon="🪄"):
                source_ph = st.empty()
                with source_ph.status("Augmenting your data..."):
                    transduction_ag = asyncio.run(
                        transduction_ag.self_transduction(source_fields, target_fields)
                    )
                source_ph.empty()
                st.write("Results")
                styled_df = transduction_ag.to_dataframe().style.apply(
                    _highlight_missing, target_fields=target_fields
                )
                st.dataframe(styled_df)
                st.download_button(
                    "Download",
                    data=transduction_ag.to_csv(),
                    file_name="augmented_data.csv",
                    icon="💾",
                )
        except Exception as e:
            match str(e):
                case "keywords must be strings":
                    st.info("Field names can't be empty")
                case _ if (
                    "Input should be a valid string [type=string_type, input_value=nan, input_type=float]_"
                    in str(e)
                ):
                    st.info("Types can't be empty")
                case _:
                    st.info(e)
    else:
        st.info("Add a new field to augment that data (+ bottom of the table)")
    return


def load_dataframe() -> pd.DataFrame:
    with st.expander(
        "Upload a file",
        expanded=True,
    ):
        file = st.file_uploader("Import your csv file", ["csv"], key="uploaded_file")
        if file:
            if file.name.endswith("jsonl"):
                df = pd.read_json(file, lines=True)
            if file.name.endswith("json"):
                df = pd.read_json(file)
            elif file.name.endswith(("csv")):
                df = pd.read_csv(file)
            df = df[:N_SAMPLES]
            st.write("##### Sample data")
            st.dataframe(df.head(), hide_index=True)
            st.session_state["df"] = df.fillna(value="")


def dashboard():
    st.markdown(
        "# Agentics Playground\n" "##### Accelerating your structured data workloads"
    )
    load_dataframe()
    if (df := st.session_state.get("df")) is not None:
        tabs = sac.tabs(
            [
                sac.TabsItem("Data Augmentation"),
                sac.TabsItem("Chaining", disabled=True),
                sac.TabsItem("MCP Tools", disabled=True),
            ],
            align="center",
            return_index=True,
        )
        match tabs:
            case 0:
                self_transduction(df)
            case 1:
                pass
            case 2:
                pass


if __name__ == "__main__":
    st.set_page_config(
        page_title="Agentics playground",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    dashboard()
