import argparse
import os
from pathlib import Path
from pydantic import BaseModel, Field
from typing import Optional
import asyncio

from agentics import AG

from dotenv import find_dotenv, load_dotenv
load_dotenv(find_dotenv())

MODEL_ID = os.environ["MODEL_ID"]

from utils import dataset_split, load_states, custom_instruction, crew_prompt_params, prompt_tempate_1_n

class Attribute(BaseModel):
    relation_name: Optional[str] = Field(None, description="relation name")
    relation_description: Optional[str] = Field(None, description="relation description")
    attribute_name: Optional[str] = Field(None, description="attribute name")
    attribute_description: Optional[str] = Field(None, description="attribute description")

class Attributes(BaseModel):
    attributes: Optional[list[Attribute]] = Field(None, description="list of Attribute states")


async def main(args):
    mimic_relation, omop_relation = dataset_split(args.data_split)
    mimic_states = load_states(args.mimic_file, Attribute, False, False, lambda x: x["relation_name"].lower()== mimic_relation)
    mimic_data = AG.from_states(mimic_states, Attribute)
    
    if args.matching_type == "1toN":
        omop_states = load_states(args.omop_file, Attributes, True, True, lambda x: x["relation_name"].lower()== omop_relation)
    elif args.matching_type == "1to1":
        omop_states = load_states(args.omop_file, Attributes, True, False, lambda x: x["relation_name"].lower()== omop_relation)
    else:
        raise NotImplemented
    num_target_schema = len(omop_states[0].attributes)
    omop_data = AG.from_states(omop_states, Attributes)
    print(f"len:{len(mimic_data)}")
    print(f"len:{len(omop_data)}")

    mimic_omop = mimic_data.product(omop_data)
    mimic_omop.crew_prompt_params = crew_prompt_params
    mimic_omop.prompt_template = prompt_tempate_1_n
    print(f"len:{len(mimic_omop)}")

    input_fields = mimic_omop.fields
    mimic_omop = mimic_omop.add_attribute(slot_name = "invertible", slot_type = list[bool], default_value=[False for _ in range(num_target_schema)], 
                                          description=f"list of true, false assignment from source to target attributes. the length of the list is the number of target schema {num_target_schema}")
    mimic_omop = mimic_omop.add_attribute(slot_name = "num_target_schema", slot_type = int, default_value=num_target_schema, description="the number of target schema")
    mimic_omop = await mimic_omop.self_transduction(input_fields + ["num_target_schema"], ["invertible"], instructions=custom_instruction)
    return mimic_omop


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mimic-file", type=str, default="mimic_attributes.jsonl")
    parser.add_argument("--omop-file", type=str, default="omop_attributes.jsonl")
    parser.add_argument("--data-split", choices=['AdCO', 'AdVD', 'AdVO', 'DiCO', 'LaMe', 'PaPe', 'PrDE', 'SeVD', 'TrVD'], default="AdCO")
    parser.add_argument("--matching-type", choices=['1to1', '1toN', 'Nto1', 'NtoM'], default="1to1")
    args = parser.parse_args()
    data_path = os.fspath(Path(__file__).resolve().parent.parent.parent / "data" / "schema_matching")
    args.mimic_file = os.path.join(data_path, args.mimic_file)
    args.omop_file = os.path.join(data_path, args.omop_file)
    model_id_to_name = {
        "watsonx/meta-llama/llama-4-maverick-17b-128e-instruct-fp8": "llama-4",
        "watsonx/meta-llama/llama-3-3-70b-instruct": "llama-3.3",
        "watsonx/openai/gpt-oss-120b": "gpt-oss"
    }

    mimic_omop = asyncio.run(main(args))
    mimic_omop.pretty_print()
    result_name = f"{model_id_to_name[MODEL_ID]}__{args.data_split}__{args.matching_type}"
    mimic_omop.to_jsonl(os.path.join(data_path, f"{result_name}.jsonl"))
