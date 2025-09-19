import asyncio
import json
import os

import streamlit as st
from text2sql import Text2sqlQuestion, execute_questions
from utils import evaluate_execution_accuracy, get_schema_from_file, load_benchmark

from agentics import AG

if "benchmark_questions" not in st.session_state:
    st.session_state.benchmark_questions = AG(atype=Text2sqlQuestion)

if "benchmark_metadata" not in st.session_state:
    st.session_state.benchmark_metadata = {}


st.header("Agentics Text2SQL")


with st.form("Benchmark Selection"):
    benchmark_id = st.selectbox(
        "Choose your Benchmark", options=list(load_benchmark().keys())
    )
    select_benchmark = st.form_submit_button("Select Benchmark")
    evaluate_benchmark = st.form_submit_button("Evaluate Benchmark")


if select_benchmark:
    st.session_state.benchmark_metadata = load_benchmark(benchmark_id)
    if "datasource_url" in st.session_state.benchmark_metadata:
        questions = json.load(
            open(
                os.path.join(os.getenv("SQL_BENCHMARKS_FOLDER"), benchmark_id + ".json")
            )
        )
        st.session_state.benchmark_questions = AG(atype=Text2sqlQuestion)
        for question in questions:
            st.session_state.benchmark_questions.states.append(
                Text2sqlQuestion(
                    question=question["page_content"],
                    sql=question["sql"],
                    benchmark_id=benchmark_id,
                    endpoint_id=str(
                        st.session_state.benchmark_metadata["datasource_url"].split(
                            "/"
                        )[-1]
                    ),
                )
            )

    else:

        st.session_state.benchmark_questions = AG.from_jsonl(
            os.path.join(os.getenv("SQL_BENCHMARKS_FOLDER"), benchmark_id + ".json"),
            jsonl=False,
            atype=Text2sqlQuestion,
        )
    final_questions = []
    for question in st.session_state.benchmark_questions:
        question.benchmark_id = benchmark_id
        final_questions.append(question)
    st.session_state.benchmark_questions.states = final_questions


if evaluate_benchmark and st.session_state.benchmark_questions:
    executed_questions = asyncio.run(
        execute_questions(st.session_state.benchmark_questions)
    )
    st.write(evaluate_execution_accuracy(executed_questions))


with st.form("Select Question"):
    select_question = st.selectbox(
        "Choose a question",
        format_func=lambda x: x.question,
        options=st.session_state.benchmark_questions.states,
    )
    execute_selected = st.form_submit_button("Execute Selected Question")

with st.form("Ask your own question"):
    db = None
    if "datasource_url" not in st.session_state.benchmark_metadata:
        db = st.selectbox(
            "Choose Target DB", options=list(get_schema_from_file(benchmark_id).keys())
        )
    user_question = st.text_input("Aks your question")
    execute_user_question = st.form_submit_button("Ask Question")


if execute_selected:
    test = AG(atype=Text2sqlQuestion, states=[select_question])


if execute_user_question:
    question = Text2sqlQuestion(
        question=user_question,
        db_id=db,
        benchmark_id=benchmark_id,
        endpoint_id=str(
            st.session_state.benchmark_metadata["datasource_url"].split("/")[-1]
        ),
    )
    test = AG(atype=Text2sqlQuestion, states=[question])


if execute_selected or execute_user_question:
    test = asyncio.run(execute_questions(test))
    col1, col2 = st.columns(2)
    col1.markdown("### System")
    try:
        col1.dataframe(json.loads(test[0].system_output_df))
    except:
        col1.write(test[0].system_output_df)
    col2.markdown("### GT")
    try:
        col2.dataframe(json.loads(test[0].gt_output_df))
    except:
        col2.write(test[0].gt_output_df)

    with st.popover("See details"):
        st.write(test[0])
    # if test.system_output_df.startswith("Error:"):
    #     st.write(test.system_output_df)
    # else:
    #     import json
    #     st.dataframe(json.loads(test.system_output_df))
    # st.write(load_benchmark(benchmark_id))
    # test = AG.from_jsonl(
    #     os.path.join(os.getenv("SQL_BENCHMARKS_FOLDER"),
    #                             benchmark_id + ".json"),
    #     jsonl=False,
    #     atype=Text2sqlQuestion,
    #     max_rows=10)
    # st.write(test[0])

#     question_state= AG(atype=Text2sqlQuestion, states=(Text2sqlQuestion(db_id=target_db, question=question)))
#     answer = asyncio.run(execute_questions(question_state))
#     st.write(answer)
