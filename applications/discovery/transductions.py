from agentic_db import AgenticDB
from atypes import Answer, IntermediateEvidence, Question
from pandas import DataFrame

from agentics.core.agentics2.agentics_2 import AG
from agentics.core.agentics2.transducible_functions import (
    ParameterWrapper,
    transducible,
)

transducible()


async def describe_dataset(state: AgenticDB) -> str:
    """Provide a paragraph long description for the following dataset"""
    return ParameterWrapper(state)
    # # new_ag = AgenticDB(df=state.df)
    # # return ParameterWrapper(new_ag)


transducible()


async def answer_question_from_data(state: Question) -> Question:

    if state.question:
        source_data = [AG.from_dataframe(DataFrame(db.df)) for db in state.dbs]

        intermediate_evidence = AG(atype=IntermediateEvidence)

        for data in source_data:
            dataset_descriptions = [
                f"""
==================

Name: {db.name} 
Description:
{db.dataset_description} 
Columns:
{db.columns}

"""
                for db in state.dbs
            ]

            intermediate_evidence_for_source = await (
                AG(
                    atype=IntermediateEvidence,
                    transduction_type="areduce",
                    areduce_batch_size=10000,
                    instructions=f"""
You have been provided with a CSV file which might contain relevant information to answer a given QUESTION. 
QUESTION: {state.question}

Your task is to collect intermediate evidence needed to answer the question from the provided data at a later stage 
DATASET descriptions: 
{"".join(dataset_descriptions)}
DOMAIN_KNOWLEDGE: {state.domain_knowledge}
QUESTION: {state.question}
""",
                )
                << data
            )

            intermediate_evidence.states += intermediate_evidence_for_source.states

        final_answer = await (
            AG(
                atype=Answer,
                instructions=f"""
You previously collected intermediate evidence to answer a given QUESTION after inspecting several data sources.
Your task is to proivide a single answer to the question taking into account the provided evidence. 
DATASET descriptions: 
{"".join(dataset_descriptions)}
DOMAIN_KNOWLEDGE: {state.domain_knowledge}
QUESTION: {state.question}
""",
                transduction_type="areduce",
                areduce_batch_size=10000,
            )
            << intermediate_evidence
        )

        if len(final_answer) > 0:
            state.generated_hypothesis = final_answer[0].short_answer
            state.full_answer = final_answer[0]
        state.intermediate_evidence = intermediate_evidence.states

    return state
