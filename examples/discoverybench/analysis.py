"""
Analysis script for evaluation results.
Processes evaluation.json files and computes aggregated scores.
"""

import json
import os
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s', 
                    datefmt='%m/%d/%Y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_dimension_scores(eval_result: Dict) -> Tuple[List[float], List[float], List[float]]:
    """
    Extract dimension scores from a single evaluation result.
    
    Args:
        eval_result: A single evaluation result dict with matched_gold_gen_subh_evals
        
    Returns:
        Tuple of (var_scores, rel_scores, context_scores)
    """
    var_scores = []
    rel_scores = []
    context_scores = []
    
    matched_evals = eval_result.get('matched_gold_gen_subh_evals', {})
    
    if not matched_evals:
        return var_scores, rel_scores, context_scores
    
    for pair_key, pair_eval in matched_evals.items():
        # Extract var score (f1)
        if 'var' in pair_eval and 'score' in pair_eval['var']:
            var_score = pair_eval['var']['score']
            if isinstance(var_score, dict) and 'f1' in var_score:
                var_scores.append(var_score['f1'])
        
        # Extract rel score
        if 'rel' in pair_eval and 'score' in pair_eval['rel']:
            rel_score = pair_eval['rel']['score']
            if isinstance(rel_score, (int, float)):
                rel_scores.append(rel_score)
        
        # Extract context score
        if 'context' in pair_eval and 'score' in pair_eval['context']:
            context_score = pair_eval['context']['score']
            if isinstance(context_score, (int, float)):
                context_scores.append(context_score)
    
    return var_scores, rel_scores, context_scores


def compute_means(var_scores: List[float], rel_scores: List[float], 
                 context_scores: List[float]) -> Dict[str, float]:
    """
    Compute mean scores for each dimension.
    
    Args:
        var_scores: List of F1 scores for variables
        rel_scores: List of relationship scores
        context_scores: List of context scores
        
    Returns:
        Dict with mean_var_score, mean_rel_score, mean_context_score
    """
    result = {}
    
    if var_scores:
        result['mean_var_score'] = sum(var_scores) / len(var_scores)
    else:
        result['mean_var_score'] = 0.0
    
    if rel_scores:
        result['mean_rel_score'] = sum(rel_scores) / len(rel_scores)
    else:
        result['mean_rel_score'] = 0.0
    
    if context_scores:
        result['mean_context_score'] = sum(context_scores) / len(context_scores)
    else:
        result['mean_context_score'] = 0.0
    
    return result


def analyze_single_result(eval_result: Dict) -> Dict:
    """
    Analyze a single evaluation result and add aggregated scores.
    
    Args:
        eval_result: Single evaluation result dict
        
    Returns:
        Modified eval_result with added mean scores
    """
    var_scores, rel_scores, context_scores = extract_dimension_scores(eval_result)
    means = compute_means(var_scores, rel_scores, context_scores)
    
    eval_result.update(means)
    
    return eval_result


def analyze_evaluation_file(file_path: str) -> Dict:
    """
    Analyze a single evaluation file (NDJSON format).
    Groups results by dataset, question_type, and dataset+question_type.
    
    Args:
        file_path: Path to evaluation.json file
        
    Returns:
        Dict with overall statistics and per-group statistics
    """
    file_stats = {
        'file': os.path.basename(file_path),
        'num_problems': 0,
        'overall_mean_var_score': 0.0,
        'overall_mean_rel_score': 0.0,
        'overall_mean_context_score': 0.0,
        'overall_mean_final_score': 0.0,
        'mean_accuracy_scores': 0.0,
        'by_dataset': {},
        'by_question_type': {},
        'by_dataset_question_type': {},
    }
    
    all_var_scores = []
    all_rel_scores = []
    all_context_scores = []
    all_final_scores = []
    all_accuracy_scores = []
    
    # Grouping dictionaries
    groups_by_dataset = defaultdict(lambda: {
        'var_scores': [], 'rel_scores': [], 'context_scores': [], 'final_scores': [], 'accuracy_scores': [], 'num_problems': 0
    })
    groups_by_qtype = defaultdict(lambda: {
        'var_scores': [], 'rel_scores': [], 'context_scores': [], 'final_scores': [], 'accuracy_scores': [], 'num_problems': 0
    })
    groups_by_dataset_qtype = defaultdict(lambda: {
        'var_scores': [], 'rel_scores': [], 'context_scores': [], 'final_scores': [], 'accuracy_scores': [], 'num_problems': 0
    })
    
    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    eval_result = json.loads(line)
                    
                    # Add dimension means to this result
                    eval_result = analyze_single_result(eval_result)
                    
                    # Get grouping keys (only if they exist in the record)
                    has_dataset = 'dataset' in eval_result
                    has_question_type = 'question_type' in eval_result
                    dataset = eval_result.get('dataset')
                    question_type = eval_result.get('question_type')
                    
                    # Collect scores for overall stats
                    var_score = eval_result.get('mean_var_score', 0.0)
                    rel_score = eval_result.get('mean_rel_score', 0.0)
                    context_score = eval_result.get('mean_context_score', 0.0)
                    final_score = eval_result.get('final_score', 0.0)
                    accuracy_score = eval_result.get('mean_accuracy_score', 0.0)
                    
                    all_var_scores.append(var_score)
                    all_rel_scores.append(rel_score)
                    all_context_scores.append(context_score)
                    all_final_scores.append(final_score)
                    all_accuracy_scores.append(accuracy_score)
                    
                    # Collect scores for group stats (only if fields exist)
                    if has_dataset:
                        groups_by_dataset[dataset]['var_scores'].append(var_score)
                        groups_by_dataset[dataset]['rel_scores'].append(rel_score)
                        groups_by_dataset[dataset]['context_scores'].append(context_score)
                        groups_by_dataset[dataset]['final_scores'].append(final_score)
                        groups_by_dataset[dataset]['accuracy_scores'].append(accuracy_score)
                        groups_by_dataset[dataset]['num_problems'] += 1
                    
                    if has_question_type:
                        groups_by_qtype[question_type]['var_scores'].append(var_score)
                        groups_by_qtype[question_type]['rel_scores'].append(rel_score)
                        groups_by_qtype[question_type]['context_scores'].append(context_score)
                        groups_by_qtype[question_type]['final_scores'].append(final_score)
                        groups_by_qtype[question_type]['accuracy_scores'].append(accuracy_score)
                        groups_by_qtype[question_type]['num_problems'] += 1
                    
                    if has_dataset and has_question_type:
                        dataset_qtype_key = f"{dataset}_{question_type}"
                        groups_by_dataset_qtype[dataset_qtype_key]['var_scores'].append(var_score)
                        groups_by_dataset_qtype[dataset_qtype_key]['rel_scores'].append(rel_score)
                        groups_by_dataset_qtype[dataset_qtype_key]['context_scores'].append(context_score)
                        groups_by_dataset_qtype[dataset_qtype_key]['final_scores'].append(final_score)
                        groups_by_dataset_qtype[dataset_qtype_key]['accuracy_scores'].append(accuracy_score)
                        groups_by_dataset_qtype[dataset_qtype_key]['num_problems'] += 1
                    
                    file_stats['num_problems'] += 1
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON at line {line_num} in {file_path}: {e}")
                    continue
    
    except IOError as e:
        logger.error(f"Failed to read file {file_path}: {e}")
        return file_stats
    
    # Compute overall means
    if all_var_scores:
        file_stats['overall_mean_var_score'] = sum(all_var_scores) / len(all_var_scores)
    if all_rel_scores:
        file_stats['overall_mean_rel_score'] = sum(all_rel_scores) / len(all_rel_scores)
    if all_context_scores:
        file_stats['overall_mean_context_score'] = sum(all_context_scores) / len(all_context_scores)
    if all_final_scores:
        file_stats['overall_mean_final_score'] = sum(all_final_scores) / len(all_final_scores)
    if all_accuracy_scores:
        file_stats['mean_accuracy_scores'] = sum(all_accuracy_scores) / len(all_accuracy_scores)
    
    # Compute per-dataset statistics
    for dataset, scores in groups_by_dataset.items():
        file_stats['by_dataset'][dataset] = {
            'num_problems': scores['num_problems'],
            'mean_var_score': sum(scores['var_scores']) / len(scores['var_scores']) if scores['var_scores'] else 0.0,
            'mean_rel_score': sum(scores['rel_scores']) / len(scores['rel_scores']) if scores['rel_scores'] else 0.0,
            'mean_context_score': sum(scores['context_scores']) / len(scores['context_scores']) if scores['context_scores'] else 0.0,
            'mean_final_score': sum(scores['final_scores']) / len(scores['final_scores']) if scores['final_scores'] else 0.0,
            'mean_accuracy_score': sum(scores['accuracy_scores']) / len(scores['accuracy_scores']) if scores['accuracy_scores'] else 0.0,
        }
    
    # Compute per-question_type statistics
    for qtype, scores in groups_by_qtype.items():
        file_stats['by_question_type'][qtype] = {
            'num_problems': scores['num_problems'],
            'mean_var_score': sum(scores['var_scores']) / len(scores['var_scores']) if scores['var_scores'] else 0.0,
            'mean_rel_score': sum(scores['rel_scores']) / len(scores['rel_scores']) if scores['rel_scores'] else 0.0,
            'mean_context_score': sum(scores['context_scores']) / len(scores['context_scores']) if scores['context_scores'] else 0.0,
            'mean_final_score': sum(scores['final_scores']) / len(scores['final_scores']) if scores['final_scores'] else 0.0,
            'mean_accuracy_score': sum(scores['accuracy_scores']) / len(scores['accuracy_scores']) if scores['accuracy_scores'] else 0.0,
        }
    
    # Compute per-dataset-question_type statistics
    for dataset_qtype, scores in groups_by_dataset_qtype.items():
        file_stats['by_dataset_question_type'][dataset_qtype] = {
            'num_problems': scores['num_problems'],
            'mean_var_score': sum(scores['var_scores']) / len(scores['var_scores']) if scores['var_scores'] else 0.0,
            'mean_rel_score': sum(scores['rel_scores']) / len(scores['rel_scores']) if scores['rel_scores'] else 0.0,
            'mean_context_score': sum(scores['context_scores']) / len(scores['context_scores']) if scores['context_scores'] else 0.0,
            'mean_final_score': sum(scores['final_scores']) / len(scores['final_scores']) if scores['final_scores'] else 0.0,
            'mean_accuracy_score': sum(scores['accuracy_scores']) / len(scores['accuracy_scores']) if scores['accuracy_scores'] else 0.0,
        }
    
    logger.info(f"Analyzed {file_stats['num_problems']} problems from {os.path.basename(file_path)}")
    logger.info(f"  Overall mean_var_score: {file_stats['overall_mean_var_score']:.4f}")
    logger.info(f"  Overall mean_rel_score: {file_stats['overall_mean_rel_score']:.4f}")
    logger.info(f"  Overall mean_context_score: {file_stats['overall_mean_context_score']:.4f}")
    logger.info(f"  Overall mean_final_score: {file_stats['overall_mean_final_score']:.4f}")
    logger.info(f"  Datasets: {list(file_stats['by_dataset'].keys())}")
    logger.info(f"  Question types: {list(file_stats['by_question_type'].keys())}")
    
    return file_stats


def analyze_directory(output_dir: str) -> None:
    """
    Analyze all evaluation files in a directory.
    
    Args:
        output_dir: Directory containing evaluation.json files
    """
    output_path = Path(output_dir)
    
    if not output_path.exists():
        logger.error(f"Directory {output_dir} does not exist")
        return
    
    # Find all evaluation files
    eval_files = []
    
    # Look for evaluation.json
    eval_json = output_path / 'evaluation.json'
    if eval_json.exists():
        eval_files.append(eval_json)
    
    # Look for evaluation_{number}.json
    for file in output_path.glob('evaluation_*.json'):
        eval_files.append(file)
    
    if not eval_files:
        logger.warning(f"No evaluation files found in {output_dir}")
        return
    
    logger.info(f"Found {len(eval_files)} evaluation file(s)")
    
    all_file_stats = []
    
    # Analyze each file
    for eval_file in sorted(eval_files):
        logger.info(f"\nAnalyzing {eval_file.name}...")
        file_stats = analyze_evaluation_file(str(eval_file))
        all_file_stats.append(file_stats)
    
    # Write summary to analysis_summary.json
    summary_path = output_path / 'analysis_summary.json'
    
    # Per-file statistics only (no cross-file aggregation)
    summary_data = {
        'files': all_file_stats,
    }
    
    with open(summary_path, 'w') as f:
        json.dump(summary_data, f, indent=2)
    
    logger.info(f"\nAnalysis summary written to {summary_path}")
    
    # Log per-file statistics
    for file_stat in all_file_stats:
        logger.info(f"\n{file_stat['file']}:")
        logger.info(f"  problems: {file_stat['num_problems']}")
        logger.info(f"  mean_var_score: {file_stat['overall_mean_var_score']:.4f}")
        logger.info(f"  mean_rel_score: {file_stat['overall_mean_rel_score']:.4f}")
        logger.info(f"  mean_context_score: {file_stat['overall_mean_context_score']:.4f}")
        logger.info(f"  mean_final_score: {file_stat['overall_mean_final_score']:.4f}")


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python analysis.py <output_directory>")
        sys.exit(1)
    
    output_dir = sys.argv[1]
    analyze_directory(output_dir)
