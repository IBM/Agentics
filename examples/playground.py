"""Playground to experience Agentics on your data

To run the file:
1. >> `poetry install --with playground`
2. >> `cd examples/`
3. >> `streamlit run playground.py`
"""

import asyncio
from io import StringIO
import json
from typing import Any, List, Optional, Literal  # noqa

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
from agentics import ollama_llm
from agentics.core.utils import make_all_fields_optional

LLM = None
N_SAMPLES = 20
TYPE_OPTIONS = ["`str`", "`int`", "`float`", "`bool`", "`Literal`"]

def split_df(
    df: pd.DataFrame,
    target_columns: str | List[str],
    test_size: float = 0.2,
    random_state: int = 42,
):
    X = df.drop(target_columns, axis=1)
    y = df[target_columns]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    return X_train, X_test, y_train, y_test


def load_dataframe() -> pd.DataFrame:
    with st.expander(
        "Upload a file",
        expanded=True,
    ):
        file = st.file_uploader(
            "Import your csv file", ["csv", "json", "jsonl"], key="uploaded_file"
        )
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
            st.session_state["df"] = df  # .fillna(value="")


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


def get_source_agentics(sources: Optional[List[str]] = None):
    df: pd.DataFrame = st.session_state.get("df")
    cols = st.columns([0.8, 0.2], vertical_alignment="bottom")
    sources = cols[0].multiselect(
        "Select the `Source` columns to use as the starting data",
        df.columns if len(df.columns) > 1 else df.columns[0],
    )
    if len(sources) > 0:
        # TODO: allow for Fields and descriptions to be be loaded from file and not only inferred
        source_types = AG(
            atype=AGType,
            instructions=AGType_instruction.format(
                sources=sources, type_options=TYPE_OPTIONS
            ),
        )
        for source in sources:
            source_types.states.append(
                AGType(FieldName=source, FieldValues=df[source].to_list())
            )

        column_config = {
            "FieldName": st.column_config.TextColumn("Field"),
            "FieldType": st.column_config.TextColumn("Type"),
            "FieldDescription": st.column_config.TextColumn(
                "Description", width="large"
            ),
        }
        if cols[1].button("AutoType", icon="🪄"):
            source_ph = st.empty()
            with source_ph.status("Filling in the descriptions..."):
                source_types = asyncio.run(
                    source_types.self_transduction(
                        ["FieldName", "FieldValues"], ["FieldType", "FieldDescription"]
                    )
                )
            source_ph.empty()
        else:
            source_types = source_types("FieldName", "FieldType", "FieldDescription")

        sources = st.data_editor(
            source_types.to_dataframe(),
            column_order=["FieldName", "FieldType", "FieldDescription"],
            row_height=75,
            column_config=column_config,
            use_container_width=True,
            key="sources",
        )

        fields = {}
        for _, col in sources.iterrows():
            fields[col.FieldName] = (
                col.FieldType | Optional[str],
                Field(default=None, description=col.FieldDescription),
            )
        st.session_state["source_fields"] = fields


def get_target_agentics():
    st.markdown("Provide the additional `Target` data that you want to infer")
    st.html("<br>")
    targets = st.data_editor(
        pd.DataFrame(
            [{"FieldName": None, "FieldType": None, "FieldDescription": None}]
        ),
        column_order=["FieldName", "FieldType", "FieldDescription"],
        column_config={
            "FieldName": st.column_config.TextColumn("Field"),
            "FieldType": st.column_config.TextColumn("Type"),
            "FieldDescription": st.column_config.TextColumn(
                "Description", width="large"
            ),
        },
        row_height=75,
        use_container_width=True,
        num_rows="dynamic",
    )
    target_fields = {}
    for _, col in targets.iterrows():
        if col.FieldName and col.FieldType:
            target_fields[col.FieldName] = (
                col.FieldType,
                Field(default=None, description=col.FieldDescription),
            )
    if target_fields:
        st.session_state["target_fields"] = target_fields
        source_fields: AG = st.session_state.get("source_fields")
        if source_fields:
            transduction_atype = make_all_fields_optional(
                create_model("Transduction", **source_fields, **target_fields)
            )
            df = st.session_state["df"]
            transduction_ag = AG.from_dataframe(
                df[list(source_fields)], atype=transduction_atype
            )
            st.session_state["transduction_ag"] = transduction_ag


@st.fragment()
def tranduce():
    sources_ag = st.session_state.get("sources_ag")
    targets_ag = st.session_state.get("targets_ag")
    _source_fields = ""
    if sources_ag:
        for field in sources_ag.transduce_fields:
            description = sources_ag.atype.model_fields[field].description
            _source_fields += f"{'`' + str(field) + '`' if not description else '`' + str(field) + '` (' + str(description) + ')'}, "
        _source_fields = _source_fields[:-2]
    _target_fields = ""
    if targets_ag:
        for field in targets_ag.transduce_fields:
            description = targets_ag.atype.model_fields[field].description
            _target_fields += f"{'`' + str(field) + '`' if not description else '`' + str(field) + '` (' + str(description) + ')'}, "
        _target_fields = _target_fields[:-2]
    if _source_fields and _target_fields:
        sources_ag.instructions = st.text_area(
            "Instruction",
            f"Given the source fields: {_source_fields}, "
            f"generate the target fields: {_target_fields}. ",
            height=75,
        )
    data = sources_ag, targets_ag
    if data:
        X, y = data
        if st.button("Tranduce!", icon="🪄"):
            result_ph = st.empty()
            with result_ph.status("Infering outputs"):
                st.session_state["prediction"] = asyncio.run(y << X)
            result_ph.empty()
            evaluate()


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

    a = types_ag.to_dataframe()
    st.dataframe(a)
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
                    data=transduction_ag.to_jsonl(),
                    file_name="augmented_data.jsonl",
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


@st.fragment()
def evaluate() -> pd.DataFrame:
    df: pd.DataFrame = st.session_state.get("df")
    pred = st.session_state["prediction"]
    if pred and st.button("✅ Evaluate"):
        highlight_ph = st.empty()
        highlight_ph.dataframe(pred.to_dataframe(), hide_index=True)
        st.write("Select the fields to run an evaluation")
        cols = st.columns(2)
        prediction: AG = st.session_state.get("prediction")
        source_field = cols[0].selectbox("compare source field", df.columns, index=None)
        target_field = cols[1].selectbox(
            "compare target field", prediction.atype.model_fields, index=None
        )
        if source_field and target_field and st.button("Evaluate", icon="🪄"):
            ph = st.empty()
            with ph.status("Evaluating the fields"):
                highlighted_df = highlight(
                    prediction.to_dataframe(),
                    pd.DataFrame(df[source_field]),
                    source_field,
                    target_field,
                )
            ph = st.empty()
            highlight_ph.dataframe(highlighted_df, hide_index=True)


def redact(df: pd.DataFrame, redaction_ratio: float = 1.0):
    mask = np.random.choice(
        [True, False],
        size=df.shape,
        p=[redaction_ratio, 1 - redaction_ratio],
    )
    df[mask] = None
    return df


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
        cols = st.columns([0.5, 0.5])
        match tabs:
            case 0:
                self_transduction(df)
            case 1:
                with cols[0].container(border=True):
                    get_source_agentics()
                with cols[1].container(border=True):
                    get_target_agentics()
                tranduce()


class SimilarityScore(BaseModel):
    predicted_value: Any
    gold_value: Any
    is_classified_the_same: Optional[bool] = None
    justification: Optional[str] = None


def highlight(
    pred: pd.DataFrame, gold: pd.DataFrame, target_field: str, source_field: str
) -> pd.DataFrame:
    """Compare two DataFrames and highlight matches in green, differences in red"""

    def highlight_cells(val):
        score = AG(atype=SimilarityScore)
        for (_, p), (_, g) in zip(pred.iterrows(), gold.iterrows()):
            score.states.append(
                SimilarityScore(
                    predicted_value=p[source_field], gold_value=g[target_field]
                )
            )
        score.instructions = "return `True` if the values are classified as the same, else return `False`. Also give a reason for your answer"

        score = asyncio.run(
            score("is_classified_the_same", "justification")
            << score("predicted_value", "gold_value")
        )

        mask_series = pd.Series(
            [_.is_classified_the_same for _ in score.states], index=pred.index
        )
        mask = pred.loc[mask_series]
        result = mask.applymap(lambda x: "background-color: #a7F0ba" if x else "")
        return result

    return pred.style.apply(highlight_cells, axis=None)


if __name__ == "__main__":
    st.set_page_config(
        page_title="Agentics playground",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    dashboard()
