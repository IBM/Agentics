import argparse
import os
from pathlib import Path
from pydantic import BaseModel, Field
from typing import Optional
import asyncio

from agentics import AG
from utils import get_header_csv_file, custom_instruction, crew_prompt_params

from dotenv import find_dotenv, load_dotenv
load_dotenv(find_dotenv())

MODEL_ID = os.environ["MODEL_ID"]


async def main(args):
    header = get_header_csv_file(args.csv_file)
    dataset = AG.from_csv(args.csv_file)
    dataset.crew_prompt_params = crew_prompt_params
    dataset = await dataset.self_transduction(header[:-1], [header[-1]], instructions=custom_instruction.format(target_field=args.impute_col))
    return dataset


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-file", type=str, default="buy_test_with_fewshots__k=1.csv")
    parser.add_argument("--impute-col", type=str, default="manufacturer")
    args = parser.parse_args()
    data_path = os.fspath(Path(__file__).resolve().parent.parent.parent / "data" / "data_wrangling")
    csv_file_name = args.csv_file.replace(".csv", "")
    args.csv_file = os.path.join(data_path, args.csv_file)
    model_id_to_name = {
        "watsonx/meta-llama/llama-4-maverick-17b-128e-instruct-fp8": "llama-4",
        "watsonx/meta-llama/llama-3-3-70b-instruct": "llama-3.3",
        "watsonx/openai/gpt-oss-120b": "gpt-oss"
    }
    results = asyncio.run(main(args))
    result_name = f"{model_id_to_name[MODEL_ID]}__{csv_file_name}"
    results.to_csv(os.path.join(data_path, f"{result_name}.csv"))
    results.to_jsonl(os.path.join(data_path, f"{result_name}.jsonl"))

