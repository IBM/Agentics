import asyncio
import os

import streamlit as st
from agentic_db import AgenticDB
from atypes import Dataset, Question
from pandas import DataFrame
from transductions import answer_question_from_data

DISCOVERY_TASK_PATH = "/Users/gliozzo/Code/agentics911/agentics/sandbox/discoverybench/discoverybench/real/demo"

st.header("Discovery Demo")
if "databases" not in st.session_state:
    st.session_state.databases = []
if "dataset" not in st.session_state:
    st.session_state.dataset = None
if "selected_question" not in st.session_state:
    st.session_state.selected_question = None

if "selected_question_object" not in st.session_state:
    st.session_state.selected_question_object = None


st.session_state.discovery_tasks = os.listdir(DISCOVERY_TASK_PATH)


with st.sidebar:
    st.markdown("### Select Sample Discovery Tasks")
    with st.form("Select Discovery Tasks"):
        selected_task = st.selectbox(
            "Task Name", options=st.session_state.discovery_tasks, index=None
        )
        select_task_button = st.form_submit_button("Select Task")

    if select_task_button:
        with st.spinner("Loading dataset, wait a few secs ..."):
            st.session_state.dataset = Dataset.import_from_discovery_bench_metadata(
                selected_task, metadata_path=DISCOVERY_TASK_PATH
            )
            st.session_state.databases = st.session_state.dataset.dbs
        st.rerun()

    if st.session_state.dataset:
        with st.form("Select question"):
            st.session_state.selected_question = st.selectbox(
                "Select Question",
                options=[
                    question.question for question in st.session_state.dataset.questions
                ],
                index=None,
            )
            selected_question_button = st.form_submit_button("Select Question")

        if selected_question_button:
            st.session_state.selected_question_object = st.session_state.dataset.get_questions_as_ag().filter_by_attribute_value(
                "question", st.session_state.selected_question
            )[
                0
            ]

    if st.session_state.selected_question_object:
        with st.popover("Question Details"):
            st.write(st.session_state.selected_question_object)

    if st.session_state.databases:
        for db in st.session_state.databases:
            st.markdown(
                f"""### {db.name if db.name else ''} 
N rows: {len(DataFrame(db.df))  if db.df else ''}

"""
            )
            st.dataframe(DataFrame(db.df).head(10))
            with st.popover("Dataset Info"):
                st.write(db)

    st.markdown("### Import data")
    uploaded_files = st.file_uploader(
        "Upload one or more CSV files", type=["csv"], accept_multiple_files=True
    )
    # if st.session_state.dataset:
    #     for question in st.session_state.dataset.questions[:3]:
    #         st.write(question.question)

    if uploaded_files:
        st.success(f"{len(uploaded_files)} file(s) uploaded successfully.")
        st.session_state.databases = []
        for file in uploaded_files:

            try:
                db = AgenticDB()
                df = db.import_db_from_csv(file)  # ✅ pass the buffer
                st.session_state.databases.append(
                    db
                )  # show a preview of the first rows

            except Exception as e:
                st.error(f"❌ Error importing {file.name}: {e}")


with st.form("Discovery Task"):
    question = st.text_area(
        "Research Question",
        value=(
            st.session_state.selected_question_object.question
            if st.session_state.selected_question_object
            else None
        ),
    )
    domain_knowledge = st.text_area(
        "Domain Knowledge",
        value=(
            st.session_state.selected_question_object.domain_knowledge
            if st.session_state.selected_question_object
            else None
        ),
    )
    execute_query_button = st.form_submit_button("Submit Discovery Task")


if execute_query_button:
    with st.spinner(
        "Agentics is reading your documents and generating intermediate evidence before answering your question. This might take some time ..."
    ):

        state = Question(
            question=question,
            dbs=st.session_state.databases,
            domain_knowledge=domain_knowledge,
        )
        answer = asyncio.run(answer_question_from_data(state))
        st.markdown(
            f"""
#### Short Answer: 

{state.full_answer.short_answer}

#### Full Answer 

{state.full_answer.full_answer}

##### evidence 
"""
        )
    if state.full_answer.selected_evidence:
        for evidence in state.full_answer.selected_evidence:
            if evidence.evidence_found:
                st.markdown(
                    f"""
{evidence.evidence}

===================================================

"""
                )
