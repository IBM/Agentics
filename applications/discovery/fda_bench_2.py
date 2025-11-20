from dotenv import load_dotenv

load_dotenv()
import asyncio
import os
from typing import Any, List, Optional

from my_sql_db import SqliteDB
from pydantic import BaseModel, Field

from agentics.core.agentics_2 import AgenticsTransduction as AG
from agentics.core.transducible_functions import Transduce, With, transducible


def lcs_length(x: str, y: str) -> int:
    """
    Compute the length of the Longest Common Subsequence (LCS).
    Efficient DP implementation.
    """
    m, n = len(x), len(y)
    dp = [[0] * (n + 1) for _ in range(m + 1)]

    for i in range(m):
        for j in range(n):
            if x[i] == y[j]:
                dp[i + 1][j + 1] = dp[i][j] + 1
            else:
                dp[i + 1][j + 1] = max(dp[i][j + 1], dp[i + 1][j])
    return dp[m][n]


from collections import Counter


def rouge_1(hypothesis: str, reference: str) -> float:
    """
    Compute ROUGE-1 F1 score between hypothesis and reference.
    Based on unigram (token) overlap.
    """

    # Tokenize by whitespace
    hyp_tokens = hypothesis.split()
    ref_tokens = reference.split()

    # Count unigrams
    hyp_counts = Counter(hyp_tokens)
    ref_counts = Counter(ref_tokens)

    # Compute overlap: sum(min(count_ref, count_hyp))
    overlap = sum(min(hyp_counts[t], ref_counts[t]) for t in ref_counts)

    if overlap == 0:
        return 0.0

    precision = overlap / len(hyp_tokens)
    recall = overlap / len(ref_tokens)

    if precision + recall == 0:
        return 0.0

    f1 = (2 * precision * recall) / (precision + recall)
    return f1


def rouge_l(hypothesis: str, reference: str) -> float:
    """
    Compute ROUGE-L F1 score between hypothesis and reference.
    Token-based or char-based depending on preprocessing.
    """
    # Tokenize by whitespace
    hyp_tokens = hypothesis.split()
    ref_tokens = reference.split()

    # Compute LCS over tokens
    lcs = lcs_length(hyp_tokens, ref_tokens)

    if lcs == 0:
        return 0.0

    precision = lcs / len(hyp_tokens)
    recall = lcs / len(ref_tokens)

    if precision + recall == 0:
        return 0.0

    f1 = (2 * precision * recall) / (precision + recall)
    return f1


class SubtaskInput(BaseModel):
    model_config = {"extra": "ignore"}

    database_name: Optional[str] = None
    natural_language_query: Optional[str] = None


class GoldSubtask(BaseModel):
    model_config = {"extra": "ignore"}

    subtask_id: Optional[str] = None
    tool: Optional[str] = None
    description: Optional[str] = None

    input: Optional[SubtaskInput] = None
    expected_SQL: Optional[str] = None
    expected_result: Optional[Any] = None


class FinalAnswerReport(BaseModel):
    original_question: Optional[str] = None
    generated_sql: Optional[str] = None
    final_answer: Optional[str] = Field(
        None,
        description="""A markdown summarizing the findings from the search query with the following structure: 
Executive Summary 
Data Analysis Results
Conclusions
""",
    )
    short_answer: Optional[str] = Field(
        None, description="A synthetic answer summarizing the above"
    )
    relevant_source_data: Optional[list[str]] = Field(
        None,
        description="A list of extracts from the relevant source data that has been used to provide an answer",
    )
    confidence: Optional[float] = None


class AnswerEvaluation(BaseModel):
    original_question: Optional[str] = None
    generated_sql: Optional[str] = None
    final_answer: Optional[str] = None

    ground_truth_report: Optional[str] = None

    answer_assessment: Optional[str] = Field(
        None,
        description="""Express a judgment on the quality of the generated AnswerReport""",
    )
    answer_score: Optional[float] = Field(
        None,
        description="""Assign a score from 0.0 to 1.0 on the correctness of the final_answer""",
    )


class SQLQueries(BaseModel):
    sql_queries: Optional[list[str]] = Field(
        None,
        description="List of 5 sql queries that can help finding needed information to answer the input question",
    )


class QuestionPlan(BaseModel):
    sub_questions: Optional[list[str]] = Field(None, description="List of subquestions")


class FDATaskInstance(BaseModel):
    model_config = {"extra": "ignore"}

    task_id: Optional[str] = None
    instance_id: Optional[str] = None
    db: Optional[str] = None
    level: Optional[str] = None
    database_type: Optional[str] = None
    question_type: Optional[str] = None
    db_schema: Optional[Any] = None
    query: Optional[str] = None
    ground_truth_report: Optional[str] = None
    generated_sqls: Optional[SQLQueries] = None
    output_dataframes: Optional[list[Any]] = None
    answer: Optional[FinalAnswerReport] = None
    answer_evaluation: Optional[AnswerEvaluation] = None


class Question(BaseModel):
    question: Optional[str] = None


import pathlib

# llm="watsonx/openai/gpt-oss-120b"
llm = AG.get_llm_provider("watsonx")


async def get_fda_DB(question: FDATaskInstance) -> SqliteDB:
    if question.database_type == "bird":
        db_path = str(
            pathlib.Path("/Users/gliozzo/Data/Text2SQL/bird/train/database")
            / question.db
            / f"{question.db}.sqlite"
        )
    db = SqliteDB(db_path=db_path)
    await db.import_db()
    return db


# @transducible(llm=llm)
# async def generate_sql_query(question: FDATaskInstance)-> SQLQuery:
#     """ Generate a SQL query to answer this question given the DB schema"""
#     db = await get_fda_DB(question)
#     transduce_obj=FDATaskInstance(query= question.query,
#                                   db_schema=db.sqlite_schema)
#     return Transduce(transduce_obj)


# import json
# @transducible(llm=llm)
# async def generate_and_execute_sql_query(question: FDATaskInstance) -> FDATaskInstance:
#     question.generated_sqls = await generate_sql_query([question for _ in range(5)])
#     db = await get_fda_DB(question)

#     outputs = [await db.async_execute_sql(question.generated_sqls) for sql_query in question.generated_sqls]
#     question.output_dataframes = []
#     for output in outputs:
#         try:
#             question.output_dataframes.append(json.loads(output))
#         except:
#             question.output_dataframes.append({})
#     return question


@transducible(llm=llm)
async def answer_and_evaluate_question(question: FDATaskInstance) -> FDATaskInstance:
    # questions= await generate_and_execute_sql_query(question)
    db = await get_fda_DB(question)

    generate_question_plan = QuestionPlan << With(
        FDATaskInstance,
        instructions="""Generate a plan to answer the input question. 
        For each step return a subquestion""",
    )
    question_plan = await generate_question_plan(
        FDATaskInstance(query=question.query, db_schema=db.sqlite_schema)
    )
    question_plan.sub_questions += question.query

    generate_queries = SQLQueries << With(
        QuestionPlan,
        instructions=f"""Generate a sql query for each of the subquestions in the question plan.
        This is the target DB schema:
        {db.sqlite_schema}""",
    )

    question.generated_sqls = await generate_queries(question_plan)

    # With(FDATaskInstance ,
    #     instructions= "Generate 5 Different SQL queries that can help find useful information "
    #     "from the db to answer the input question")
    # question.generated_sqls = await generate_queries(FDATaskInstance(query= question.query, db_schema=db.sqlite_schema))

    question.output_dataframes = []
    for query in question.generated_sqls.sql_queries:
        result_df = await db.async_execute_sql(query)
        question.output_dataframes.append(result_df)

    generate_answer = FinalAnswerReport << With(
        FDATaskInstance,
        instructions="""Generate an Answer for the input question taking into consideration the 
        generated sql queries and the corresponding dataframes resulting from issuing them against the db """,
    )
    answer = await generate_answer(
        FDATaskInstance(
            query=question.query,
            generated_sqls=question.generated_sqls,
            output_dataframes=question.output_dataframes,
        )
    )

    llm_as_a_judge = AnswerEvaluation << With(
        FDATaskInstance,
        instructions="""
                                            Read the input question, the generated answer 
                                            and the grund truth, and express a judgment""",
    )
    question.answer_evaluation = await llm_as_a_judge(
        FDATaskInstance(
            query=question.query,
            answer=answer,
            output_dataframes=question.output_dataframes,
            generated_sqls=question.generated_sqls,
            ground_truth_report=question.ground_truth_report,
        )
    )

    return question


import argparse


def main():
    parser = argparse.ArgumentParser(description="FDA Bench")
    parser.add_argument(
        "--input_path",
        required=False,
        default="/Users/gliozzo/Code/FDAbench/fda_report_data.json",
        help="File in which the input data is located, should be a jsonl list of pydantic objects",
    )
    parser.add_argument(
        "--output_path",
        required=False,
        default="",
        help="File in which the generated object will be saved or loaded, depending if eval or test",
    )
    parser.add_argument(
        "--max",
        required=False,
        default="None",
        help="Max number of processed examples, default None",
    )

    parser.add_argument("--mode", type=str, default="test", help="test or eval")

    args = parser.parse_args()
    if args.mode == "test":
        test_data = AG.from_jsonl(args.input_path, atype=FDATaskInstance)
        bird_data = test_data.filter_by_attribute_value("database_type", "bird")
        max_instances = int(args.max) if args.max != "None" else None
        answers = asyncio.run(
            answer_and_evaluate_question(bird_data.states[:max_instances])
        )
        answers_ag = AG(atype=FDATaskInstance, states=answers)
        if args.output_path != "":
            answers_ag.to_jsonl(args.output_path)
        for answer in answers:
            print(answer.answer_evaluation)
    if args.mode == "eval":
        answers = AG.from_jsonl(args.output_path, atype=FDATaskInstance)
        total_score = 0
        for answer in answers:
            # print(answer.answer_evaluation)
            if answer.answer_evaluation and answer.answer_evaluation.answer_score:
                total_score += answer.answer_evaluation.answer_score
            else:
                print(answer)
        print(f"Average Score: {total_score/len(answers)} on {len(answers)} questions")


if __name__ == "__main__":
    main()
