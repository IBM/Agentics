from dotenv import load_dotenv
load_dotenv()
from agentics import  AG
import json
from typing import Optional, Union
from pydantic import BaseModel, Field
from agentic_db import AgenticDB
from eval.new_eval import run_eval_gold_vs_gen_NL_hypo_workflow
import asyncio
import os
from pathlib import Path
from pandas import DataFrame
from loguru import logger
import agentics.core.llm_connections as llm_connections
import logging
logging.getLogger("httpx").setLevel(logging.WARNING)

DISCOVERYBENCH_ROOT=os.getenv("DISCOVERYBENCH_ROOT", "/tmp/discoverybench")

class IntermediateEvidence(BaseModel):
    evidence_found:Optional[bool] = Field(None,description="Return True if you found any relevant evidence for the QUESTION, False otherwise")
    original_question:Optional[str] = None
    evidence: Optional[str]=Field(None, description="Identify useful information needed to answer the given QUESTION")
    partial_answer: Optional[str]=Field(None, description="Provide a partial answer for the given Question bsed on the SOURCE data")


class Answer(BaseModel):
    short_answer:Optional[str] = Field(None,description="Provide a one sentence answer which specifically addresses the question")
    full_answer: Optional[str] = Field(None,description="Provide a detailed answer for the given question taking into consideration the evidence sources provided")
    selected_evidence:Optional[list[IntermediateEvidence]]
    confidence: Optional[float] = None
    

class Question(BaseModel):
    qid:Optional[Union[int,str]] = None
    true_hypothesis:Optional[str] = None
    generated_hypothesis: Optional[str] = Field(None,description="A specific hypothesis that supports the question, derived from the analysis of the input dataset")
    question_type: Optional[str] = None
    question:Optional[str] = None
    metadata_path:Optional[str] =None
    metadata:Optional[dict] = None
    metadata_id:Optional[Union[int,str]] =None
    dataset:Optional[str]=None
    dbs:Optional[list[AgenticDB]] = None
    intermediate_evidence: Optional[list[IntermediateEvidence]] = []
    full_answer:Optional[Answer] = None

    @classmethod
    def import_questions_from_metadata_as_ag(cls, metadata_path:str)->AG:
        metadata = json.load(open(metadata_path, 'r'))
        metadata_id = int(metadata_path.name.split(".")[0].split("_")[1])
        dbs = AgenticDB.import_from_discovery_bench_metadata(metadata_path)
        output =  AG(atype=Question)
        for question in metadata["queries"][0]:
            question_obj = Question(**question)
            question_obj.metadata_id = metadata_id
            question_obj.metadata=metadata
            question_obj.dbs=dbs
            question_obj.metadata_path = str(metadata_path)
            output.append(question_obj)
        return output
    
    
class Dataset(BaseModel):
    metadata_path:Optional[str]=None
    datasets_descriptions:Optional[list[str]] = None
    dbs:Optional[list[AgenticDB]] = None
    questions:Optional[list[Question]]=[]

    @classmethod
    def import_from_discovery_bench_metadata(cls, 
                                             dataset, 
                                             metadata_path=os.path.join(DISCOVERYBENCH_ROOT, "/discoverybench/real/test")) -> str:
        dataset_obj=Dataset()
        
        if not dataset_obj.questions: dataset_obj.questions=[]
        base_path=Path(metadata_path)/dataset
        print("Importing dataset",end="")
        for metadata in os.listdir(base_path):
            if metadata.endswith(".json"):
                if not dataset_obj.dbs:
                    dataset_obj.dbs = AgenticDB.import_from_discovery_bench_metadata(base_path/metadata)
                print(".",end=".")
                dataset_obj.questions += Question.import_questions_from_metadata_as_ag(base_path/metadata).states
        dataset_obj.get_source_descriptions()
        return dataset_obj
 
    def get_source_descriptions(self)->str:
        self.datasets_descriptions=[]
        for db in self.dbs:
            self.datasets_descriptions.append(f"""
            Dataset Description: {db.dataset_description}
            Columns: {db.columns}
            """)
        return self.datasets_descriptions


async def answer_question_from_data(state:Question)-> Question:
    if state.question:
        source_data =  [AG.from_dataframe(DataFrame(db.df)) for db in state.dbs]

        intermediate_evidence=AG(atype=IntermediateEvidence)
        if Config.llm_provider:
            intermediate_evidence.llm = llm_connections.__getattr__(Config.llm_provider)
       
        for data in source_data:
            ag_args = {
                "atype": IntermediateEvidence,
                "transduction_type": "areduce", 
                "areduce_batch_size": 10000,
                "instructions": f"""
You have been provided with a CSV file which might contain relevant information to answer a given QUESTION. 
QUESTION: {state.question}

Your task is to collect intermediate evidence needed to answer the question from the provided data at a later stage 

DOMAIN_KNOWLEDGE: {state.metadata["domain_knowledge"]}
WORKFLOW_TAGS: {state.metadata["workflow_tags"]}
QUESTION: {state.question}
"""
            }
            if Config.llm_provider:
                ag_args["llm"]= llm_connections.__getattr__(Config.llm_provider)            

            intermediate_evidence_for_source= await (AG(**ag_args) << data)
            intermediate_evidence.states += intermediate_evidence_for_source.states

        ag_args = {
            "atype": Answer,
            "transduction_type": "areduce", 
            "areduce_batch_size": 10000,
            "instructions": f"""
You previously collected intermediate evidence to answer a given QUESTION after inspecting several data sources.
Your task is to proivide a single answer to the question taking into account the provided evidence. 
QUESTION: {state.question}
DOMAIN_KNOWLEDGE: {state.metadata["domain_knowledge"]}
WORKFLOW_TAGS: {state.metadata["workflow_tags"]}
"""
        }
        if Config.llm_provider:
            ag_args["llm"]= llm_connections.__getattr__(Config.llm_provider)                 
        final_answer=await (AG(**ag_args)<< intermediate_evidence)

        if len(final_answer)>0:
            state.generated_hypothesis=final_answer[0].short_answer
            state.full_answer= final_answer[0]
        state.intermediate_evidence = intermediate_evidence.states
        
    return state            


async def execute_all_datasets(output_path:str, 
                               selected_datasets:list[str]=None, 
                               task_path:str =os.path.join(DISCOVERYBENCH_ROOT, "/discoverybench/real/test")):
    """Save answers on disk to minimize memory space as AG[Question] jsonl files, one for each dataset.
    """
    task_path=Path(task_path)
    output_path=Path(output_path)
    if not os.path.exists(output_path): os.mkdir(output_path)
    for dataset_name in selected_datasets or os.listdir(task_path):
    
        dataset=Dataset.import_from_discovery_bench_metadata(dataset_name)
        questions=AG(atype=Question,states=dataset.questions)
        if Config.llm_provider:
            questions.llm=llm_connections.__getattr__(Config.llm_provider)

        if os.path.exists(output_path/f"{dataset_name}.jsonl"):
            already_processed_answers = AG.from_jsonl(output_path/f"{dataset_name}.jsonl", atype=Question)
        else: already_processed_answers=[]
        already_processed = set([answer.question for answer in already_processed_answers])

        for question in questions:
            if question.question not in already_processed:

                temp_ag =  AG(atype=Question, states=[question])
                if Config.llm_provider:
                    temp_ag.llm = llm_connections.__getattr__(Config.llm_provider)

                answer = await (temp_ag.amap(answer_question_from_data))
                if len(answer) > 0: 
                    answer.to_jsonl(output_path/f"{dataset_name}.jsonl",append=True)
            else: logger.warning(f"Skipping question {question.question}\nAlready Processed")

def evaluate_dataset(dataset: str, 
                    questions:AG,
                    ground_truth:str = os.path.join(DISCOVERYBENCH_ROOT, "/eval/answer_key_real.csv"),
                    output_eval:str=None,
                    use_short_answer:bool=False,
                    llm_provider:str=None)-> float:
    ground_truth=AG.from_csv(ground_truth)
    examples_hash={}
    for example in ground_truth:
        examples_hash[f"{example.dataset},{example.metadataid},{example.query_id}"] = example.gold_hypo

    if llm_provider:
        selected_llm = _get_cached_available_llms().get(llm_provider)
    else:
        selected_llm = get_llm_provider()

    final_score=0
    for question in questions :
        gold= examples_hash.get(f"{dataset},{question.metadata_id},{question.qid}")
        system_prediction=  question.generated_hypothesis if use_short_answer else  (question.full_answer and question.full_answer.full_answer)
        if system_prediction:
            if gold:
                evaluation_result= run_eval_gold_vs_gen_NL_hypo_workflow(
                    question.question, 
                    examples_hash[f"{dataset},{question.metadata_id},{question.qid}"], 
                    None, 
                    system_prediction,
                    None, 
                    question.metadata, 
                    'gpt-4-1106-preview', 
                    dataset_type = "real",
                    llm_object=selected_llm)
        else:
            evaluation_result = {"query":question.question,
                                 "HypoA": gold,
                                 "HypoB": system_prediction,
                                 "recall_context": 0,
                                 "mean_accuracy_score":0,
                                 "final_score":0}
            
        final_score += evaluation_result["final_score"]
        if output_eval:
            with open(output_eval,"a") as f: f.write(json.dumps(evaluation_result) + "\n")
        print(evaluation_result)

    print("final_score: ",final_score/len(questions))
    return final_score/len(questions)

    
def evaluate_all(system_output_path:str, use_short_answer:False, llm_provider:str=None):
    """Evaluate all datasets in the output folder.
    Args:
        system_output_path: Path to the generated hypotheses
        use_short_answer: If True, use short_answer; if False, use full_answer
        llm_provider: Optional LLM provider name (e.g., 'gemini', 'litellm_proxy_1', 'litellm_proxy_2')
    """
    system_output_path=Path(system_output_path)
    for output in os.listdir(system_output_path):
        dataset_name=output.split(".")[0]
        answers=AG.from_jsonl(system_output_path/output, atype=Question)
        evaluate_dataset(dataset_name,
                         answers, 
                         output_eval= system_output_path / "evaluation.json",
                         use_short_answer = use_short_answer,
                         llm_provider=llm_provider)


import argparse


class Config:
    """Module-level configuration for LLM provider and other settings."""
    llm_provider = None


def main():
    """CLI entry point: pass the dataset name (e.g., archaeology)."""
    parser = argparse.ArgumentParser(
        description="Execute all datasets by name (e.g. archaeology, climate, fossils)."
    )
    parser.add_argument(
    "-o", "--output_folder",
    type=str,
    default="/tmp/try6",
    required=False,
    help="Output folder for transduced objects"
    )
    parser.add_argument(
        "-d", "--selected_dataset",
        type=str,
        default=None,
        help="(Optional) Execute a single dataset by name"
    )
    parser.add_argument(
        "-m", "--mode",
        type=str,
        default="generation",
        help="If mode = generation, run an entire evaluation on the whole discovery bench" \
        "if mode = evaluation a Performs a complete evaluation of the system output in --output_folder"
    )
    parser.add_argument(
        "-s", "--use_short_answer",
        type=bool,
        default=False,
        help="(Optional) Use generated short answer as output hypothesis if True, use full_answer"
    )
    parser.add_argument(
        "-p", "--llm-provider",
        type=str,
        default=None,
        help="(Optional) Specify LLM provider to use (e.g., 'gemini', 'litellm_proxy_1', 'litellm_proxy_2', 'litellm_proxy_3')"
    )

    args = parser.parse_args()
    
    # Set global config
    Config.llm_provider = args.llm_provider
    
    if Config.llm_provider:
        logger.info(f"Using LLM provider: {Config.llm_provider}")
    else:
        logger.info("Using default LLM provider (first available)")
    
    if args.mode == "generation":
        asyncio.run(execute_all_datasets(args.output_folder, selected_datasets=[args.selected_dataset] if args.selected_dataset else None))
    elif args.mode == "evaluation":
        evaluate_all(args.output_folder, use_short_answer=args.use_short_answer, llm_provider=args.llm_provider)


if __name__ == "__main__":
    main()
