import argparse
import logging
import random
import time
import json
import os

import numpy as np
import pandas as pd
from collections import defaultdict
from openai import OpenAI
from IPython import embed
import pstats
import cProfile

from eval.lm_utils import run_chatgpt_query_multi_turn, run_llm_query_multi_turn
from utils.arguments import Arguments
from utils.helpers import setup_logger, printj, round_sympy_expr, extract_variable, powerset, get_const_from_sympy, \
    safe_exp
from utils.openai_helpers import OPENAI_GEN_HYP, create_prompt, get_response
from utils.openai_semantic_gen_prompts import *

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s', datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

def get_score_from_answer(type, answer):
    """Score the answer from LLM response. Handles empty/invalid responses gracefully."""
    
    # Handle None or empty answer
    if not answer or (isinstance(answer, str) and not answer.strip()):
        logger.warning(f"Empty answer received for type: {type}")
        if type == "var":
            return {"p": -1.0, "r": -1.0, "f1": -1.0}
        else:
            return -1.0

    if type == "context":
        answer = answer.replace("Answer:", "").strip()
        if answer.startswith("A)"):
            return 1.0
        elif answer.startswith("B)"):
            return 0.0
        return -1.0

    elif type == "var":
        try:
            # Clean up markdown code blocks if present
            answer_clean = answer.strip().strip("```json").strip("```").strip()
            if not answer_clean:
                logger.warning("Empty answer for var type")
                return {
                    "p": -1.0,
                    "r": -1.0,
                    "f1": -1.0
                }
            var_json = json.loads(answer_clean)
            print(f"var_json:{var_json}")
            p=0.0
            r=0.0
            f1=0.0
            if var_json['sizeB']:
                p = var_json['intersection']/var_json['sizeB']
            if var_json['sizeA']:
                r = var_json['intersection']/var_json['sizeA']
            if p>0.0 and r>0.0:
                f1 = (2 * p * r)/(p + r)
            else:
                f1 = 0.0
            eval_rec =  {
                    "p": p,
                    "r": r,
                    "f1": f1,
                    "sizeA": var_json['sizeA'],
                    "sizeB": var_json['sizeB'],
                    "intersection": var_json['intersection'],
                    "explanation": var_json['explanation'],
            }
            print(f"var_eval: {eval_rec}")
            return eval_rec
        except Exception as e:
            logger.warning(f"Failed to parse var answer: {e}")
            return {
                    "p": -1.0,
                    "r": -1.0,
                    "f1": -1.0
                }
    elif type == "rel":
        try:
            # Clean up markdown code blocks if present
            answer_clean = answer.strip().strip("```json").strip("```").strip()
            if not answer_clean:
                logger.warning("Empty answer for rel type")
                return -1.0
            rel_json = json.loads(answer_clean)
            answer_str = rel_json["answer"].strip()
            if answer_str.startswith("A") or "very similar" in answer_str:
                return 1.0
            elif answer_str.startswith("B") or "similar but general than HypoA" in answer_str:
                return 0.5
            elif answer_str.startswith("C") or "different" in answer_str:
                return 0.0
            return -1.0
        except Exception as e:
            logger.warning(f"Failed to parse rel answer: {e}")
            return -1.0
    return -1.0

def ask_dimension_question(query, gold_hypo, gold_workflow, gen_hypo,
                           gen_workflow, dataset_meta, llm_used,
                            dimension, dataset_type, use_column_metadata=True, llm_object=None):
    dimension_question = ""
    answer = ""
    score = 0.0
    if dimension == "var":
        score = {
                    "p": -1.0,
                    "r": -1.0,
                    "f1": -1.0
                }
    num_tokens = 256
    num_retries = 1
    json_response = False

    messages = [
        {"role": "system",
         "content": "You are an AI assistant that helps evaluate a data-driven hypothesis. You are a helpful assistant who is not talkative. You only respond with the exact answer to a query without additional conversation."
         },
    ]
    if dimension == "context":
        dimension_question = """\
        Question: Is HypoB defined in the same context as HypoA?
        (Context refers to assumptions/stratification under which the hypotheses are defined.)
        Options: A) same   B) different
        What is your answer?"""
    elif dimension == "var":
        dimension_question = """\
        Question: For both HypoA and HypoB, what are the different variables found in the hypotheses? \
        Return your answer as a JSON object in the following format:
        ```json
        {{
        "sizeA": num of variables used in HypoA
        "sizeB": num of variables used in HypoB
        "intersection": num of variables common in HypoA and HypoB. Use *fuzzy matching* to determine intersection, accounting for paraphrases or slightly different surface forms
        "explanation": a short text explanation about the variables
        }}```
        Answer:"""
        num_tokens = 512
        num_retries = 1
        json_response = True
    elif dimension == "rel":
        dimension_question = """\
        Question: Does HypoB exhibit the same relation as HypoA?
        Compare using following example hierarchy of relationships (based on specificity): \
        "there exists a relationship" > "positive relationship" > "positive AND (linear OR quadratic)" > "positive AND linear".
        Options: A) very similar B) similar but general than HypoA C) different
        Return your answer as a JSON object in the following format:
        ```json
        {{
        "answer": one of the options from A) very similar B) similar but general than HypoA C) different
        "explanation": a short text explanation about the relationship comparison
        }}```
        Answer:"""
        num_tokens = 512
        num_retries = 1
        json_response = True

    datasets_json = prepare_dataset_metadata_json(dataset_meta, dataset_type=dataset_type, use_column_metadata=use_column_metadata)

    dimension_question_str = f"""\
        You are going to compare two natural-language hypotheses HypoA and HypoB accompanied with optional workflows: WorkflowA for HypoA and WorkflowB for HypoB. \
        Both the hypotheses answer the natural language query "QUERY" over the dataset(s) described by dataset description(s) and column description(s) below. \
        Compare HypoA and HypoB in terms of three aspects: Contexts, Variables, and Relations. \
        E.g., for the hypothesis "From 1995 to 2009, the number of sandhill cranes around the tundra (Indigilka River) surged by an astounding ~10X":
        * Contexts refer to stratification of the data under which the given hypothesis is True. E.g., "For all women", "From 1995 to 2009".
        * Variables refer to the set of variables (either dependent or independent) that are mentioned in the hypothesis. E.g., number of sandhill cranes, location.
        * Relations refer to the form of relation between the variables. E.g., "surged by ~10x".

        Answer following questions for a given pair of hypotheses, HypoA and HypoB, along with an explanation grounded on the QUERY and the DATASET(S).

        Here is the metadata for the task:
        ```json
        {{
        "datasets": {datasets_json},
        "query": {query},
        "HypoA": {gold_hypo},
        "WorkflowA": {gold_workflow},
        "HypoB": {gen_hypo},
        "WorkflowB": {gen_workflow}
        }}
        ```

        {dimension_question}"""

    messages.append(
        {"role": "user",
         "content": dimension_question_str
         }
    )
    response_text = ""
    for retry in range(num_retries):
        response = run_llm_query_multi_turn(
                messages=messages,
                model_name=llm_used,
                max_tokens=num_tokens,
                temperature=0,  # 0 for greedy best decoding
                json_response=json_response,
                llm_object=llm_object
        )
        if response != None:
            response_text = response.choices[0].message.content.strip()
            if response_text:  # Non-empty response
                break
        response_text = ""

    if response_text:
        answer = response_text
        score = get_score_from_answer(type=dimension, answer=answer)
    else:
        logger.warning(f"No valid response received for dimension: {dimension}")
        answer = ""
        if dimension == "var":
            score = {"p": -1.0, "r": -1.0, "f1": -1.0}
        elif dimension == "rel":
            score = -1.0
        else:
            score = -1.0

    return dimension_question, answer, score

def prepare_dataset_metadata_json(dataset_meta, dataset_type, use_column_metadata=True):
    if dataset_meta == None:
        return [{
            "dataset_description":"",
            "columns": [],
        }]
    datasets_json = []
    if dataset_type == "real":
        for d in dataset_meta["datasets"]:
            datasets_json.append(
                {
                    "dataset_description": d['description'],
                    "columns": [{"name": col["name"], "description": col["description"]} for col in
                    d["columns"]["raw"]] if use_column_metadata else []
                }
            )
    else:
        for d in dataset_meta["datasets"]:
            datasets_json.append(
                {
                    "dataset_description": d['description'],
                    "columns": [{"name": col["name"], "description": col["description"]} for col in
                    d["columns"]] if use_column_metadata else []
                }
            )
    return datasets_json


def get_sub_hypotheses(query, hypo, workflow, dataset_meta, llm_used, dataset_type, use_column_metadata=True, llm_object=None):
    extraction_prompt = f"""\
        Given a set of dataset columns, a ground-truth hypothesis, and the analysis workflow used, your task is to extract three dimensions that define the hypothesis: Context, Variables, and Relations. \
        Here are the definitions for these dimensions:
        - Contexts: Boundary conditions that limit the scope of a hypothesis. E.g., “for men over \
        the age of 30”, “in Asia and Europe”. If the context applies to the full dataset, then extract the context from the dataset_descrption.
        - Variables: Known concepts that interact in a meaningful way under a given context to \
        produce the hypothesis. E.g., gender, age, income, or "None" if there is no interacting variable.
        - Relations: Interactions between a given set of variables under a given context to produce \
        the hypothesis. E.g., “quadratic relationship”, “inversely proportional”, piecewise conditionals, \
        or "None" if there is no interacting relationship.
        Make sure to only use the information present in the hypothesis and the workflow. Do not add any new information. \
        For each dimension, be specific, and do not omit any important details.

        Here is the metadata for the task:
        ```json
        {{
        "datasets": %s,
        "hypothesis": "%s",
        "workflow": "%s"
        }}
        ```

        Return your answer as a JSON object in the following format:
        ```json
        {{
        "sub_hypo": [
            {{
                "text": the hypothesis in natural language,
                "context": a short text description of the context of the hypothesis,
                "variables": a list of columns involved in the hypothesis,
                "relations": a short text description of the relationship between the variables of the hypothesis
            }},
            ...
        ]
        }}```
        """
    # extraction_prompt = f"""\
    #     Given a set of dataset columns, a ground-truth hypothesis, and the analysis workflow used, your task is to extract the \
    #     set of sub-hypotheses that are present in the hypothesis such that each sub-hypothesis covers a separate context, is \
    #     self-sufficient, and operates on a coherent set of 3 dimensions: Context, Variables, and Relations. \
    #     Here are the definitions for these dimensions:
    #     - Contexts: Boundary conditions that limit the scope of a sub-hypothesis. E.g., “for men over \
    #     the age of 30”, “in Asia and Europe”. If the context applies to the full dataset, then extract the context from the dataset_descrption.
    #     - Variables: Known concepts that interact in a meaningful way under a given context to \
    #     produce the sub-hypothesis. E.g., gender, age, income, or "None" if there is no interacting variable.
    #     - Relations: Interactions between a given set of variables under a given context to produce \
    #     the sub-hypothesis. E.g., “quadratic relationship”, “inversely proportional”, piecewise conditionals, \
    #     or "None" if there is no interacting relationship.
    #     Make sure to only use the information present in the hypothesis and the workflow. Do not add any new information. \
    #     If no sub-hypotheses can be extracted, return an empty list.

    #     Here is the metadata for the task:
    #     ```json
    #     {{
    #     "datasets": %s,
    #     "hypothesis": "%s",
    #     "workflow": "%s"
    #     }}
    #     ```

    #     Return your answer as a JSON object in the following format:
    #     ```json
    #     {{
    #     "sub_hypo": [
    #         {{
    #             "text": the sub-hypothesis in natural language,
    #             "context": a short text description of the context of the sub-hypothesis,
    #             "variables": a list of columns involved in the sub-hypothesis,
    #             "relations": a short text description of the relationship between the variables of the sub-hypothesis,
    #             "explanation": a short text explanation for the breakdown of the sub-hypothesis
    #         }},
    #         ...
    #     ]
    #     }}```
    #     """
    datasets_json = prepare_dataset_metadata_json(dataset_meta, dataset_type, use_column_metadata=use_column_metadata)
    _prompt = extraction_prompt % (datasets_json, hypo, workflow)
    
    # Use CrewAI LLM if provided, otherwise fall back to OpenAI
    if llm_object is not None:
        sub_hypo_json = get_response(prompt=_prompt, llm_object=llm_object, max_retry=1)
    else:
        client = OpenAI()
        sub_hypo_json = get_response(client=client, prompt=_prompt, model=llm_used, max_retry=1)

    if sub_hypo_json != None:
        print(f"full hypothesis: {hypo}")
        print(f"sub_hypo_json: {sub_hypo_json}")
    else:
        sub_hypo_json = {
                 "sub_hypo": [],
        }

    sub_hypo_json['full_hypo'] = hypo

    return sub_hypo_json

def match_context_with_gpt(gold_hyp, gold_context, pred_hyp, pred_context, model='gpt-3.5-turbo', llm_object=None):
    prompt = f"""\
        Given a gold hypothesis, a gold context, a predicted hypothesis, and a predicted context, your task is \
        to determine if the predicted context semantically matches the ground-truth context. \
        Here is the definition for Context: Boundary conditions that limit the scope of a sub-hypothesis. E.g., “for men over the age of 30”, “in Asia and Europe”. If the context applies to the full dataset, then the context is derived from the dataset_descrption. \
        Here is the definition for Context: Boundary conditions that limit the scope of a sub-hypothesis. E.g., “for men over the age of 30”, “in Asia and Europe”. If the context applies to the full dataset, then the context is derived from the dataset_descrption. \
        If the predicted context matches the gold context, return true, otherwise return false.
        If both gold and predicted hypotheses are defined over the context of the full dataset, then also return true.
        If both gold and predicted hypotheses are defined over the context of the full dataset, then also return true.

        Here is the metadata for the task:
        ```json
        {{
            "gold_hypothesis": "{gold_hyp}",
            "gold_context": "{gold_context}",
            "predicted_hypothesis": "{pred_hyp}",
            "predicted_context": "{pred_context}"
        }}
        ```

        Return your answer as a JSON object in the following format:
        ```json
        {{
            "match": true or false
        }}
        ```"""

    # Use CrewAI LLM if provided, otherwise fall back to OpenAI
    if llm_object is not None:
        output = get_response(prompt=prompt, llm_object=llm_object)
    else:
        client = OpenAI()
        output = get_response(client=client, prompt=prompt, model=model)
    
    return output.get("match", False) if output else False


def is_matching_context(gold_hyp, gold_context, pred_hyp, pred_context, llm_used, llm_object=None):
    if gold_context == pred_context:
        return True
    if "None" in [gold_context, pred_context]:
        return False
    return match_context_with_gpt(gold_hyp, gold_context, pred_hyp, pred_context, model=llm_used, llm_object=llm_object)

def compare_hypothesis_similarity(query, gold_hypo, gen_hypo, dataset_meta, llm_used, dataset_type, use_column_metadata=True, llm_object=None):
    """
    Directly compare two hypotheses for semantic similarity.
    Returns a score of 1.0 (perfect match), 0.5 (partial match), or 0.0 (no match).
    """
    
    datasets_json = prepare_dataset_metadata_json(dataset_meta, dataset_type=dataset_type, use_column_metadata=use_column_metadata)
    
    comparison_prompt = f"""\
        You are an AI assistant that evaluates the semantic similarity between two hypotheses.
        You are precise, focused, and only respond with the exact answer to the query without additional conversation.
        
        Given a query and two natural language hypotheses (HypoA and HypoB), assess whether they are semantically equivalent.
        Consider whether HypoB adequately addresses the same research question as HypoA, accounting for the query context.
        
        Evaluation criteria:
        - A) Perfect match (1.0): HypoB conveys the same key findings, variables, and relationships as HypoA
        - B) Partial match (0.5): HypoB addresses the same question but is more general, less specific, or missing some dimensions
        - C) No match (0.0): HypoB addresses a different question or contradicts HypoA
        
        Here is the metadata for the task:
        ```json
        {{
        "datasets": {datasets_json},
        "query": "{query}",
        "HypoA (Ground Truth)": "{gold_hypo}",
        "HypoB (Generated)": "{gen_hypo}"
        }}
        ```
        
        Question: Are HypoA and HypoB semantically equivalent in the context of the given query and dataset(s)?
        
        Return your answer as a JSON object in the following format:
        ```json
        {{
        "category": one of "A) Perfect match", "B) Partial match", "C) No match"
        "score": 1.0 or 0.5 or 0.0
        "explanation": a short explanation of your assessment
        }}```
        
        Answer:"""
    
    messages = [
        {"role": "system",
         "content": "You are an AI assistant that evaluates hypothesis similarity. You are precise, objective, and only respond with the exact answer to queries without additional conversation."
         },
        {"role": "user",
         "content": comparison_prompt
         }
    ]
    
    num_tokens = 512
    json_response = True
    
    response = run_llm_query_multi_turn(
        messages=messages,
        model_name=llm_used,
        max_tokens=num_tokens,
        temperature=0,  # greedy decoding for consistent evaluation
        json_response=json_response,
        llm_object=llm_object
    )
    
    answer = ""
    score = 0.0
    explanation = ""
    
    if response is not None:
        response_text = response.choices[0].message.content.strip()
        if response_text:
            answer = response_text
            try:
                answer_clean = answer.strip().strip("```json").strip("```").strip()
                result_json = json.loads(answer_clean)
                
                score_value = result_json.get("score", 0.0)
                # Validate score is one of the allowed values
                if score_value in [1.0, 0.5, 0.0]:
                    score = score_value
                else:
                    logger.warning(f"Invalid score value: {score_value}, defaulting to 0.0")
                    score = 0.0
                
                explanation = result_json.get("explanation", "")
                
            except Exception as e:
                logger.warning(f"Failed to parse hypothesis comparison answer with JSON: {e}")
                # Emergency decoding: try regex matching for score extraction
                import re
                score_match = re.search(r'"?score"?\s*:\s*([\d.]+)', answer, re.IGNORECASE)
                if score_match:
                    try:
                        parsed_score = float(score_match.group(1))
                        if parsed_score in [1.0, 0.5, 0.0]:
                            score = parsed_score
                        else:
                            logger.warning(f"Regex-extracted score {parsed_score} not in allowed values, defaulting to 0.0")
                            score = 0.0
                        explanation = "Score extracted via regex emergency decoding"
                        logger.info(f"Successfully recovered score {score} via regex from malformed response")
                    except ValueError:
                        logger.warning(f"Failed to convert regex-extracted score to float")
                        score = 0.0
                        explanation = f"Error parsing response (JSON and regex failed): {str(e)}"
                else:
                    logger.warning(f"No score found via regex in response")
                    score = 0.0
                    explanation = f"Error parsing response (JSON and regex failed): {str(e)}"
    else:
        logger.warning("No response received for hypothesis comparison")
        score = 0.0
        explanation = "No response from LLM"
    
    return {
        "question": comparison_prompt,
        "answer": answer,
        "score": score,
        "explanation": explanation
    }

def run_eval_gold_vs_gen_NL_subhypo(query, gold_hypo, gold_workflow, gen_hypo, gen_workflow, dataset_meta, llm_used, context_score, dataset_type, use_column_metadata=True, llm_object=None):
    # GPT-4 based evaluation to evaluate generated hypothesis in terms of context, variables, relation

    eval_rec = {
        "query": query,
        "HypoA": gold_hypo,
        "WorkflowA": gold_workflow,
        "HypoB": gen_hypo,
        "WorkflowB": gen_workflow,
    }

    for dimension in ['var', 'rel']:
        question, answer, score = ask_dimension_question(query, gold_hypo, gold_workflow,
                               gen_hypo, gen_workflow, dataset_meta, llm_used,
                               dimension=dimension, dataset_type=dataset_type, use_column_metadata=use_column_metadata, llm_object=llm_object)

        eval_rec[dimension] = {
            "question": question,
            "answer": answer,
            "score": score
        }

    eval_rec['context'] = context_score
    eval_rec['accuracy_score'] = 1.0 * max(0, eval_rec['context']['score']) * max(0, eval_rec['var']['score']['f1']) * max(0, eval_rec['rel']['score'])

    return eval_rec


def run_eval_gold_vs_gen_NL_hypo_workflow(query, gold_hypo, gold_workflow, gen_hypo, gen_workflow, dataset_meta, llm_used, dataset_type, use_column_metadata=True, llm_object=None):
    # Input: Dataset Metadata, Query, Gold {Hg, Wg}, Predicted {Hp, Wp}
    # Output: eval_rec json includes final_score

    # Procedure:
        # Dataset Metadata, Query, Gold {Hg, Wg}, Pred {Hg, Wg}
            # Gold: [Hg1, Hg2] (compute on the fly) Hg1 is a NL form of subhypothesis
            # Predicted: [Hp1, Hp2] (compute on the fly)

        # Compute Intersection: [(Hg_i, Hp_j), …]  # tuples of (gold,pred) that matched with context (do this w/o explicit extraction)
        # # filter so that a gold context and a predicted context are only attached to one tuple
        # Compute recall_context (programmatically)

        # r_v_list = []
        # For (Hg_i, Hp_j) in the intersection:
        #             With Hg_i, Hp_j in NL, ask GPT4 → #variables and #intersection and a paragraph explanation and programmatically calculate f1_v
            # Hg_i, Hp_j in NL, ask GPT4 → matching score (0, 0.5 or 1) : A) very similar B) similar but general than HypoA C) different + explanation
            # 	r_v_list ← f1_v * score_r
        # accuracy_score = mean(r_v_list)
        # score =   [ recall_context * mean over predicted context(context_score * var_score *rel_score )]
    
    recall_context = 1.0
    eval_rec = {
        "query": query,
        "HypoA": gold_hypo,
        "WorkflowA": gold_workflow,
        "HypoB": gen_hypo,
        "WorkflowB": gen_workflow,
    }

    # Direct comparison of hypotheses at the natural language level
    # Assesses semantic similarity: 1.0 (perfect), 0.5 (partial), 0.0 (no match)
    hypothesis_comparison = compare_hypothesis_similarity(
        query=query,
        gold_hypo=gold_hypo,
        gen_hypo=gen_hypo,
        dataset_meta=dataset_meta,
        llm_used=llm_used,
        dataset_type=dataset_type,
        use_column_metadata=use_column_metadata,
        llm_object=llm_object
    )
    eval_rec["hypothesis_comparison"] = hypothesis_comparison

    gold_sub_hypo_json = get_sub_hypotheses(query=query,
                                       hypo=gold_hypo, workflow=gold_workflow,
                                       dataset_meta=dataset_meta, llm_used=llm_used, dataset_type=dataset_type, use_column_metadata=use_column_metadata, llm_object=llm_object)
    if len(gold_sub_hypo_json['sub_hypo']) == 0:
        gold_sub_hypo_json['sub_hypo'] = [{"text": gold_hypo, "context": "None", "variables": [], "relations": "", "explanation": "unable to segment"}]
    print(f"gold_sub_hypo_json: {gold_sub_hypo_json}")

    gen_sub_hypo_json = get_sub_hypotheses(query=query,
                                       hypo=gen_hypo, workflow=gen_workflow,
                                       dataset_meta=dataset_meta, llm_used=llm_used, dataset_type=dataset_type, use_column_metadata=use_column_metadata, llm_object=llm_object)
    if len(gen_sub_hypo_json['sub_hypo']) == 0:
        gen_sub_hypo_json['sub_hypo'] = [{"text": gen_hypo, "context": "None", "variables": [], "relations": "", "explanation": "unable to segment"}]
    print(f"gen_sub_hypo_json: {gen_sub_hypo_json}")

    eval_rec['gold_sub_hypo'] = gold_sub_hypo_json
    eval_rec['gen_sub_hypo'] = gen_sub_hypo_json

    gold_subh_covered = []
    gen_subh_to_gold_subh = dict()
    gen_gold_subh_to_context = dict()

    for p_id, gen_subh in enumerate(gen_sub_hypo_json['sub_hypo']):
        gen_subh_to_gold_subh[p_id] = -1

        for g_id, gold_subh in enumerate(gold_sub_hypo_json['sub_hypo']):
            if g_id in gold_subh_covered:
                continue

            # match context
            context_bool = is_matching_context(gold_subh["text"], gold_subh.get("context", ""), gen_subh["text"], gen_subh.get("context", ""), llm_used, llm_object=llm_object)
            if context_bool:
                context_score = 1.0
            else:
                context_score = 0.0

            if context_score == 1.0: # match only when context_score = 1.0
                gen_subh_to_gold_subh[p_id] = g_id
                gold_subh_covered.append(g_id)
                gen_gold_subh_to_context[f"P{p_id}||G{g_id}"] = {
                    "question": f"""Comapring: GoldH: {gold_subh["text"]}, GoldC: {gold_subh['context']}\nGenH: {gen_subh['text']}, GenC: {gen_subh['context']}""",
                    "answer": context_bool,
                    "score": context_score
                }
                break

    print(f"gen_subh_to_gold_subh: {gen_subh_to_gold_subh}")
    eval_rec['gen_subh_to_gold_subh'] = gen_subh_to_gold_subh
    eval_rec['gold_subh_covered'] = gold_subh_covered
    matched_gold_gen_subh_evals = dict()
    sum_accuracy_score = 0.0
    for p_id, g_id in gen_subh_to_gold_subh.items():
        if g_id >=0:
            key = f"P{p_id}||G{g_id}"
            context_score = gen_gold_subh_to_context[key]
            subh_eval_rec = run_eval_gold_vs_gen_NL_subhypo(query, gold_hypo, gold_workflow, gen_hypo, gen_workflow, dataset_meta, llm_used, context_score, dataset_type=dataset_type, use_column_metadata=use_column_metadata, llm_object=llm_object)
            sum_accuracy_score += subh_eval_rec['accuracy_score']
            matched_gold_gen_subh_evals[key] = subh_eval_rec

    eval_rec['matched_gold_gen_subh_evals'] = matched_gold_gen_subh_evals
    eval_rec['recall_context'] = len(gold_subh_covered)/len(gold_sub_hypo_json['sub_hypo']) if len(gold_sub_hypo_json['sub_hypo']) else 0.0
    mean_accuracy_score = sum_accuracy_score/len(gen_subh_to_gold_subh) if len(gen_subh_to_gold_subh) else 0.0
    eval_rec['mean_accuracy_score'] = mean_accuracy_score
    final_score = eval_rec['recall_context'] * mean_accuracy_score
    eval_rec['final_score'] = final_score
    print(f"eval_rec: {json.dumps(eval_rec, indent=2)}")

    return eval_rec
