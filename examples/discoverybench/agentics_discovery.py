import asyncio
import json
import os
import time
from typing import Optional, Union
from pydantic import BaseModel, Field
from pathlib import Path
from pandas import DataFrame

from dotenv import load_dotenv
load_dotenv()

from agentics import AG
from agentic_db import AgenticDB
import agentics.core.llm_connections as llm_connections
from eval.new_eval import run_eval_gold_vs_gen_NL_hypo_workflow
from analysis import analyze_directory

from loguru import logger
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
                                             metadata_path=os.path.join(DISCOVERYBENCH_ROOT, "discoverybench/real/test")) -> str:
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
                               task_path:str=os.path.join(DISCOVERYBENCH_ROOT, "discoverybench/real/test")):
    """Save answers on disk to minimize memory space as AG[Question] jsonl files, one for each dataset.
    Writes to temporary files during processing, then merges with main files after completion.
    """
    task_path=Path(task_path)
    output_path=Path(output_path)
    if not os.path.exists(output_path): os.mkdir(output_path)
    processed_datasets = []
    
    for dataset_name in selected_datasets or os.listdir(task_path):
    
        dataset=Dataset.import_from_discovery_bench_metadata(dataset_name)
        questions=AG(atype=Question,states=dataset.questions)
        for ind, q in enumerate(questions):
            print(ind, q.question)

        if Config.llm_provider:
            questions.llm=llm_connections.__getattr__(Config.llm_provider)

        main_file = output_path/f"{dataset_name}.jsonl"
        tmp_file = output_path/f"{dataset_name}_tmp.jsonl"
        
        if os.path.exists(main_file):
            already_processed_answers = AG.from_jsonl(main_file, atype=Question)
        else: 
            already_processed_answers=[]

        already_processed = set()
        generated_hypothesis_null = set()
        for answer in already_processed_answers:
            if answer.generated_hypothesis:
                already_processed.add((answer.question, answer.qid))
            else:
                generated_hypothesis_null.add((answer.question, answer.qid))
        logger.info(f"Dataset: {dataset_name} | Total Questions: {len(questions)} | Already Processed (non-null hypothesis): {len(already_processed)} | Failed (null hypothesis): {len(generated_hypothesis_null)}")

        for question in questions:
            if (question.question, question.qid) not in already_processed:
                temp_ag =  AG(atype=Question, states=[question])
                if Config.llm_provider:
                    temp_ag.llm = llm_connections.__getattr__(Config.llm_provider)

                answer = await (temp_ag.amap(answer_question_from_data))
                # drop a few fields to save space
                if len(answer) > 0:
                    try: 
                        answer[0].dbs = None
                        # answer[0].metadata = None     # this is needed!
                        answer.to_jsonl(tmp_file, append=True)
                    except Exception as e:
                        logger.error(f"Error processing answer for question {question.question}: {e}")
            elif Config.verbose: 
                logger.warning(f"Skipping question {question.question}\nAlready Processed with non-null hypothesis")
        
        processed_datasets.append(dataset_name)
    
    logger.info("Merging temporary files with main output files...")
    for dataset_name in processed_datasets:
        main_file = output_path/f"{dataset_name}.jsonl"
        tmp_file = output_path/f"{dataset_name}_tmp.jsonl"
        if tmp_file.exists():
            merge_jsonl_files(main_file, tmp_file)
    
    validate_jsonl_files(output_path)


def evaluate_dataset(dataset: str, 
                    questions:AG,
                    ground_truth:str = os.path.join(DISCOVERYBENCH_ROOT, "eval/answer_key_real.csv"),
                    output_eval:str=None,
                    use_short_answer:bool=True)-> tuple:
    """Evaluate a dataset and return accumulated score, question count, and evaluation results.
    
    Args:
        dataset: Dataset name
        questions: AG object containing questions
        ground_truth: Path to ground truth CSV
        output_eval: Path to evaluation output file (not used for writing, just for compatibility)
        use_short_answer: Whether to use short or full answer
        
    Returns:
        Tuple of (accumulated_final_score, num_questions_evaluated, num_missing_predictions, evaluation_results_list)
    """
    ground_truth=AG.from_csv(ground_truth)
    examples_hash={}
    for example in ground_truth:
        examples_hash[f"{example.dataset},{example.metadataid},{example.query_id}"] = example.gold_hypo

    assert Config.llm_provider is not None, "LLM provider must be specified for evaluation."
    selected_llm = llm_connections.__getattr__(Config.llm_provider)    

    total_final_score = 0
    num_evaluated = 0
    num_missing_prediction = 0
    evaluation_results = []  # Collect results for sorting
    
    for question in questions:
        gold = examples_hash.get(f"{dataset},{question.metadata_id},{question.qid}")

        if use_short_answer:
            if question.generated_hypothesis:
                system_prediction = question.generated_hypothesis 
            else:
                system_prediction = question.full_answer.full_answer if question.full_answer else None
        else:
            system_prediction = question.full_answer.full_answer if question.full_answer else None
        
        # Handle all cases properly
        if system_prediction and gold:
            # Both exist - run evaluation
            evaluation_result = run_eval_gold_vs_gen_NL_hypo_workflow(
                question.question, 
                gold, 
                None, 
                system_prediction,
                None, 
                question.metadata, 
                'gpt-4-1106-preview', 
                dataset_type = "real",
                llm_object=selected_llm)
        else:
            # Missing prediction or gold - create default result
            evaluation_result = {
                "query": question.question,
                "HypoA": gold,
                "HypoB": system_prediction,
                "recall_context": 0,
                "mean_accuracy_score": 0,
                "final_score": 0,
                "hypothesis_comparison": 0
            }
            num_missing_prediction += 1
        
        evaluation_result["dataset"] = dataset
        evaluation_result["metadata_id"] = question.metadata_id
        evaluation_result["qid"] = question.qid
        evaluation_result["question_type"] = question.question_type
        evaluation_result["llm_provider"] = Config.llm_provider
        evaluation_result["metadata"] = question.metadata

        total_final_score += evaluation_result["final_score"]
        num_evaluated += 1
        
        # Collect result for sorting (no immediate write)
        evaluation_results.append(evaluation_result)
        print(evaluation_result)

    avg_score = total_final_score / num_evaluated if num_evaluated > 0 else 0
    logger.info(f"Dataset {dataset}: total_final_score={total_final_score}, num_questions={num_evaluated}, avg_score={avg_score}, num_missing_prediction={num_missing_prediction}")
    return total_final_score, num_evaluated, num_missing_prediction, evaluation_results


def merge_jsonl_files(main_file: Path, tmp_file: Path) -> None:
    """Merge tmp jsonl file with main jsonl file, prioritizing entries with non-null generated_hypothesis.
    
    Args:
        main_file: Path to the main jsonl file
        tmp_file: Path to the temporary jsonl file
    """
    # Load all entries from both files
    main_entries = {}
    tmp_entries = {}
    
    # Read main file if it exists
    if main_file.exists():
        with open(main_file, 'r') as f:
            for line in f:
                if line.strip():
                    entry = json.loads(line)
                    key = (entry.get('qid'), entry.get('question'))
                    main_entries[key] = entry
    
    # Read tmp file if it exists
    if tmp_file.exists():
        with open(tmp_file, 'r') as f:
            for line in f:
                if line.strip():
                    entry = json.loads(line)
                    key = (entry.get('qid'), entry.get('question'))
                    tmp_entries[key] = entry
    
    # Merge: prioritize entries with non-null generated_hypothesis
    merged_entries = {}
    
    # Start with main entries
    merged_entries.update(main_entries)
    
    # Add/update with tmp entries, preferring non-null generated_hypothesis
    for key, tmp_entry in tmp_entries.items():
        if key in merged_entries:
            main_entry = merged_entries[key]
            # If tmp has non-null generated_hypothesis, use it
            if tmp_entry.get('generated_hypothesis'):
                merged_entries[key] = tmp_entry
            # If main has null but tmp has non-null, use tmp
            elif not main_entry.get('generated_hypothesis') and tmp_entry.get('generated_hypothesis'):
                merged_entries[key] = tmp_entry
            # If both are null, keep tmp (which is more recent)
            elif not main_entry.get('generated_hypothesis') and not tmp_entry.get('generated_hypothesis'):
                merged_entries[key] = tmp_entry
        else:
            # New entry from tmp
            merged_entries[key] = tmp_entry
    
    # Sort merged entries by metadata_id and qid (both as integers) in ascending order
    def sort_key(entry):
        metadata_id = entry.get('metadata_id')
        qid = entry.get('qid')
        # Convert to int if they are strings or int; handle None values
        try:
            metadata_id = int(metadata_id) if metadata_id is not None else float('inf')
        except (ValueError, TypeError):
            metadata_id = float('inf')
        try:
            qid = int(qid) if qid is not None else float('inf')
        except (ValueError, TypeError):
            qid = float('inf')
        return (metadata_id, qid)
    
    sorted_entries = sorted(merged_entries.values(), key=sort_key)
    
    # Write sorted merged entries to main file
    with open(main_file, 'w') as f:
        for entry in sorted_entries:
            f.write(json.dumps(entry) + '\n')
    
    # Remove tmp file
    if tmp_file.exists():
        tmp_file.unlink()
    
    logger.info(f"Merged and sorted {tmp_file.name} into {main_file.name} (sorted by metadata_id, qid) and removed tmp file")


def validate_jsonl_files(output_path: Path) -> None:
    """Validate all JSONL files in the output folder for encoding errors.
    
    Attempts to fix encoding issues if possible, otherwise removes the corrupted file.
    
    Args:
        output_path: Path to the output folder containing JSONL files
    """
    logger.info("Validating JSONL files for encoding errors...")
    
    for file_path in output_path.glob("*.jsonl"):
        try:
            # Try to read the file with strict UTF-8 encoding
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = []
                for line in f:
                    if line.strip():
                        lines.append(line)
            logger.info(f"✓ {file_path.name}: Valid UTF-8 encoding ({len(lines)} lines)")
            
        except UnicodeDecodeError as e:
            logger.warning(f"✗ {file_path.name}: Unicode decoding error at position {e.start}")
            
            # Try to recover by reading with error handling
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    valid_lines = []
                    for line in f:
                        if line.strip():
                            try:
                                # Verify each line is valid JSON
                                json.loads(line)
                                valid_lines.append(line)
                            except json.JSONDecodeError:
                                logger.debug(f"  Skipping malformed JSON line in {file_path.name}")
                                continue
                
                if valid_lines:
                    # Rewrite file with valid lines only
                    with open(file_path, 'w', encoding='utf-8') as f:
                        for line in valid_lines:
                            f.write(line)
                    logger.info(f"  Fixed: {file_path.name} - kept {len(valid_lines)} valid lines")
                else:
                    # File is empty after recovery attempt
                    file_path.unlink()
                    logger.warning(f"  Discarded: {file_path.name} - no valid lines recovered")
                    
            except Exception as recovery_error:
                # Recovery failed, remove the file
                logger.error(f"  Failed to recover {file_path.name}: {recovery_error}")
                file_path.unlink()
                logger.warning(f"  Discarded: {file_path.name}")
        
        except Exception as e:
            logger.error(f"Error validating {file_path.name}: {e}")

    
def rename_evaluation_files(output_path: Path) -> None:
    existing_files = sorted(output_path.glob("evaluation*.jsonl"), key=lambda p: p.name)
    
    if existing_files:   
        evaluation_file = output_path / "evaluation.jsonl"
        new_name = output_path / f"evaluation_{len(existing_files)}.jsonl"
        evaluation_file.rename(new_name)
        logger.info(f"Renamed {evaluation_file.name} to {new_name.name}")


def evaluate_all(system_output_path:str, 
                 use_short_answer:False):
    """Evaluate all datasets in the output folder and compute overall score.
    
    Args:
        system_output_path: Path to the output folder
        use_short_answer: If True, use short_answer; if False, use full_answer
    """
    system_output_path=Path(system_output_path)
    
    # Rotate existing evaluation files to preserve previous runs
    rename_evaluation_files(system_output_path)
    
    total_score = 0
    total_questions = 0
    total_missing_predictions = 0
    dataset_results = []
    all_evaluation_results = []  # Collect all results for sorting
    
    for output in os.listdir(system_output_path):
        dataset_name = output.split(".")[0]
        if not output.endswith(".jsonl") or output.endswith("_tmp.jsonl") or output.startswith("evaluation"):
            continue
        answers = AG.from_jsonl(system_output_path/output, atype=Question)
        dataset_score, num_questions, num_missing_predictions, evaluation_results = evaluate_dataset(
            dataset_name,
            answers, 
            output_eval=system_output_path / "evaluation.jsonl",
            use_short_answer=use_short_answer
        )
        total_score += dataset_score
        total_questions += num_questions
        total_missing_predictions += num_missing_predictions
        dataset_results.append((dataset_name, dataset_score, num_questions, num_missing_predictions))
        all_evaluation_results.extend(evaluation_results)  # Collect for later sorting
    
    # Sort all evaluation results by metadata_id and qid before writing
    def sort_key(result):
        metadata_id = result.get('metadata_id')
        qid = result.get('qid')
        # Convert to int if they are strings or int; handle None values
        try:
            metadata_id = int(metadata_id) if metadata_id is not None else float('inf')
        except (ValueError, TypeError):
            metadata_id = float('inf')
        try:
            qid = int(qid) if qid is not None else float('inf')
        except (ValueError, TypeError):
            qid = float('inf')
        return (metadata_id, qid)
    
    sorted_results = sorted(all_evaluation_results, key=sort_key)
    
    # Write sorted results to evaluation file
    evaluation_file = system_output_path / "evaluation.jsonl"
    with open(evaluation_file, 'w') as f:
        for result in sorted_results:
            f.write(json.dumps(result) + '\n')
    logger.info(f"Wrote {len(sorted_results)} sorted evaluation results to {evaluation_file.name}")
    
    # Print overall results
    overall_avg_score = total_score / total_questions if total_questions > 0 else 0
    print("\n" + "="*60)
    print("OVERALL EVALUATION RESULTS")
    print("="*60)
    for dataset_name, score, count, num_missing_predictions in dataset_results:
        avg = score / count if count > 0 else 0
        avg_non_missing = score / (count - num_missing_predictions) if (count - num_missing_predictions) > 0 else 0
        print(f"  {dataset_name}: score={score:.2f}, questions={count}, avg={avg:.4f}, avg_non_missing={avg_non_missing:.4f}")
    print("-"*60)
    print(f"Total Questions Evaluated: {total_questions}")
    print(f"Total Missing Predictions: {total_missing_predictions}")
    print(f"Total Score: {total_score:.2f}")
    print(f"Overall Average Score: {overall_avg_score:.4f}")
    print(f"Hypothesis Matching Score: {overall_avg_score*100:.2f}")
    print("="*60 + "\n")


import argparse


class Config:
    """Module-level configuration for LLM provider and other settings."""
    llm_provider = None
    output_folder = None
    mode = None
    use_short_answer = True
    verbose = False


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
        help="Mode of operation: 'generation' runs discovery, 'evaluation' evaluates outputs, 'analysis' analyzes evaluation results"
    )
    parser.add_argument(
        "-s", "--use_short_answer",
        type=bool,
        default=True,
        help="(Optional) Use generated short answer as output hypothesis if True, use full_answer"
    )
    parser.add_argument(
        "-p", "--llm-provider",
        type=str,
        default=None,
        help="(Optional) Specify LLM provider to use (e.g., 'gemini', 'litellm_proxy_1', 'litellm_proxy_2', 'litellm_proxy_3')"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()
    
    # Set global config
    Config.llm_provider = args.llm_provider
    Config.output_folder = args.output_folder
    Config.mode = args.mode
    Config.use_short_answer = args.use_short_answer
    Config.verbose = args.verbose

    if Config.llm_provider:
        logger.info(f"Using LLM provider: {Config.llm_provider}")
    else:
        logger.info("Using default LLM provider (first available)")
    
    if args.mode == "generation":
        asyncio.run(execute_all_datasets(args.output_folder, selected_datasets=[args.selected_dataset] if args.selected_dataset else None))
    elif args.mode == "evaluation":
        evaluate_all(args.output_folder, use_short_answer=args.use_short_answer)
    elif args.mode == "analysis":
        analyze_directory(args.output_folder)
    else:
        logger.error(f"Unknown mode: {args.mode}. Choose from 'generation', 'evaluation', or 'analysis'")


if __name__ == "__main__":
    t0 = time.time()
    main()
    print(f"mode: {Config.mode}")
    print(f"output_folder: {Config.output_folder}")
    print(f"llm_provider: {Config.llm_provider}")
    print(f"Total time (s): {time.time() - t0}")

