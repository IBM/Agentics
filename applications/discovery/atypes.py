import json
import os
from pathlib import Path
from typing import Optional, Union

from agentic_db import AgenticDB
from pydantic import BaseModel, Field

from agentics.core.agentics2.agentics_2 import AgenticsTransduction as AG


class IntermediateEvidence(BaseModel):
    evidence_found: Optional[bool] = Field(
        None,
        description="Return True if you found any relevant evidence for the QUESTION, False otherwise",
    )
    original_question: Optional[str] = None
    evidence: Optional[str] = Field(
        None,
        description="Identify useful information needed to answer the given QUESTION",
    )
    partial_answer: Optional[str] = Field(
        None,
        description="Provide a partial answer for the given Question bsed on the SOURCE data",
    )


class Answer(BaseModel):
    short_answer: Optional[str] = Field(
        None,
        description="Provide a one sentence answer which specifically addresses the question",
    )
    full_answer: Optional[str] = Field(
        None,
        description="Provide a detailed answer for the given question taking into consideration the evidence sources provided",
    )
    selected_evidence: Optional[list[IntermediateEvidence]]
    confidence: Optional[float] = None


class Question(BaseModel):
    qid: Optional[Union[int, str]] = None
    true_hypothesis: Optional[str] = None
    generated_hypothesis: Optional[str] = Field(
        None,
        description="A specific hypothesis that supports the question, derived from the analysis of the input dataset",
    )
    domain_knowledge: Optional[str] = None
    question_type: Optional[str] = None
    question: Optional[str] = None
    dataset: Optional[str] = None
    dbs: Optional[list[AgenticDB]] = None
    intermediate_evidence: Optional[list[IntermediateEvidence]] = []
    full_answer: Optional[Answer] = None

    @classmethod
    def import_questions_from_metadata_as_ag(
        cls, metadata_path: str, import_dbs: bool = False
    ) -> AG:
        metadata = json.load(open(metadata_path, "r"))
        dbs = AgenticDB.import_from_discovery_bench_metadata(metadata_path)
        output = AG(atype=Question)
        for question in metadata["queries"][0]:
            question_obj = Question(**question)
            question_obj.domain_knowledge = metadata.get("domain_knowledge")
            if import_dbs:
                question_obj.dbs = dbs
            output.append(question_obj)
        return output


class Dataset(BaseModel):
    metadata_path: Optional[str] = None
    datasets_descriptions: Optional[list[str]] = None
    dbs: Optional[list[AgenticDB]] = None
    questions: Optional[list[Question]] = []

    @classmethod
    def import_from_discovery_bench_metadata(
        cls,
        dataset,
        metadata_path="/Users/gliozzo/Code/agentics911/agentics/sandbox/discoverybench/discoverybench/real/demo",
    ) -> str:
        dataset_obj = Dataset()

        if not dataset_obj.questions:
            dataset_obj.questions = []
        base_path = Path(metadata_path) / dataset
        print("Importing dataset", end="")
        for metadata in os.listdir(base_path):
            if metadata.endswith(".json"):
                if not dataset_obj.dbs:
                    dataset_obj.dbs = AgenticDB.import_from_discovery_bench_metadata(
                        base_path / metadata
                    )
                print(".", end=".")
                dataset_obj.questions += Question.import_questions_from_metadata_as_ag(
                    base_path / metadata, import_dbs=False
                ).states
        dataset_obj.get_source_descriptions()
        return dataset_obj

    def get_source_descriptions(self) -> str:
        self.datasets_descriptions = []
        for db in self.dbs:

            self.datasets_descriptions.append(
                f"""
            Dataset Description: {db.dataset_description}
            Columns: {db.columns}
            """
            )
        return self.datasets_descriptions

    def get_questions_as_ag(self) -> AG:
        return AG(atype=Question, states=self.questions)
