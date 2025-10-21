import asyncio
from typing import Optional

import streamlit as st
from pydantic import BaseModel

from agentics import AG
from agentics.core.atype import create_pydantic_model, get_pydantic_fields

# st.set_page_config(
#     page_title="Agentics",
#     layout="wide",
#     initial_sidebar_state="expanded",
# )

st.header("Agentics Playground")


class BaseAgenticType(BaseModel):
    source: Optional[str] = None
    target: Optional[str] = None


if "ag" not in st.session_state:
    st.session_state.ag = AG(atype=BaseAgenticType, states=[BaseAgenticType()])
if "transduced" not in st.session_state:
    st.session_state.transduced = AG(atype=BaseAgenticType, states=[BaseAgenticType()])

# col1, col2 = st.columns(2)

with st.sidebar.popover("Type Mapping"):
    st.markdown(
        """
```python
"str": str,
"int": int,
"float": float,
"bool": bool,
"list": list,
"dict": dict,
"Optional[str]": str,
"Optional[int]": int,
"""
    )
with st.sidebar:
    st.subheader("AType")

    type_model = st.data_editor(
        ## TODO:  get_pydantic_fields debug
        get_pydantic_fields(st.session_state.ag.atype),
        num_rows="dynamic",
        use_container_width=True,
    )
    update_atype = st.button("Update Atype")

    # st.subheader("Memory")
    # selected_memory = st.selectbox(
    #     "select memory", options=["NONE"] + asyncio.run(memory.get_collections())
    # )

    st.subheader("self_transduction")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    # dataframe_path = st.text_input("Import Dataframe", value="./data/top_movies.csv")
    max_rows = st.number_input("Number of Rows", min_value=1, value=10)
    load_dataframe = st.button("load_dataframe")
    source_fields = st.multiselect(
        "source_fields", options=st.session_state.ag.atype.model_fields
    )
    target_fields = st.multiselect(
        "target_fields", options=st.session_state.ag.atype.model_fields
    )
    task_description = st.text_area("Task Description (Optional)", value=None)
    perform_transduction = st.button("perform_transduction")


import hashlib

import pandas as pd


def hash_obj(obj) -> str:
    return hashlib.md5(
        pd.util.hash_pandas_object(obj.to_dataframe(), index=True).values
    ).hexdigest()


# Store hash of current .ag dataframe in session_state
current_hash = hash_obj(st.session_state.ag)

# Store last known hash
if "ag_hash" not in st.session_state:
    st.session_state.ag_hash = current_hash

if "ag_version" not in st.session_state:
    st.session_state.ag_version = 0


# Check for change
if st.session_state.ag_hash != current_hash:
    st.session_state.ag_hash = current_hash
    st.session_state.ag_version = st.session_state.ag_version + 1

# Trigger a refresh key based on change
st.subheader("Input States")
st.session_state.input_data_editor = st.data_editor(
    st.session_state.ag.to_dataframe(),
    num_rows="dynamic",
    use_container_width=True,
    key=f"ag_editor_input_{st.session_state.ag_version}",
)


update_input_states = st.button("Update Input States")


st.subheader("Output States")
st.session_state.output_data_editor = st.data_editor(
    st.session_state.transduced.to_dataframe(),
    num_rows="dynamic",
    use_container_width=True,
    key=f"ag_editor_output_{st.session_state.ag_version}",
)

update_output_states = st.button("Update Output States")


import os

if load_dataframe and uploaded_file:
    temp_file = os.path.join("/tmp/", "playground_uploaded.csv")
    with open(
        temp_file,
        "wb",
    ) as f:
        f.write(uploaded_file.getbuffer())
    st.session_state.ag = AG.from_csv(
        temp_file,
        max_rows=max_rows,
    )
    st.rerun()

if update_input_states:
    st.session_state.ag = AG.from_dataframe(st.session_state.input_data_editor)
    st.rerun()

if update_atype:
    atype = create_pydantic_model(type_model)
    st.session_state.ag = st.session_state.ag.rebind_atype(atype)
    st.rerun()

if perform_transduction:
    with st.spinner():
        # st.session_state.ag.memory_collection = (
        #     selected_memory if selected_memory != "NONE" else None
        # )

        st.session_state.transduced = asyncio.run(
            st.session_state.ag.self_transduction(source_fields, target_fields)
        )
        # st.write(st.session_state.transduced)

        st.session_state.output_data_editor = st.dataframe(
            st.session_state.transduced.to_dataframe()
        )
        # st.rerun()
if update_output_states:
    st.session_state.ag = st.session_state.transduced
    st.session_state.input_data_editor = st.session_state.output_data_editor
    st.rerun()
