import json
import os
from pathlib import Path


def get_data_from_jsonl(jsonl_file):
    ret_dicts = []
    for each_line in open(jsonl_file):
        if each_line.strip():
            ret_dicts.append(json.loads(each_line.strip()))
    return ret_dicts


def dump_data_to_jsonl(list_of_dicts, jsonl_file, mode="w"):
    with open(jsonl_file, mode) as fp:
        for each_data in list_of_dicts:
            fp.write(json.dumps(each_data) + "\n")


def preprocess_schema_file(input_schema_file, output_schema_file):
    """read the schema in the table oriented format, and convert it to list of columns"""
    table_schema = get_data_from_jsonl(input_schema_file)
    column_schema = []
    cnt = 0
    for each_table in table_schema:
        table_id = each_table["table_id"]
        table_desc = each_table["table_desc"]
        for each_column in each_table["columns"]:
            column_name = each_column["column_name"]
            column_desc = each_column["column_desc"]
            column_schema.append(
                {
                    "id": cnt,
                    "relation_name": table_id,
                    "relation_description": table_desc,
                    "attribute_name": column_name,
                    "attribute_description": column_desc,
                }
            )
            cnt += 1
    dump_data_to_jsonl(column_schema, output_schema_file)


def preprocess_ground_truth(
    input_ground_truth, output_ground_truth, mimic_attribute_file, omop_attribute_file
):
    column_mapping = get_data_from_jsonl(input_ground_truth)
    mimic_attributes = get_data_from_jsonl(mimic_attribute_file)
    omop_attributes = get_data_from_jsonl(omop_attribute_file)

    id_mapping_from_mimic_to_omop = []
    for each_column_map in column_mapping:
        for mimic_col in mimic_attributes:
            if (
                mimic_col["relation_name"].lower()
                == each_column_map["mimic"]["table_id"].lower()
            ):
                if (
                    mimic_col["attribute_name"].lower()
                    == each_column_map["mimic"]["column_name"].lower()
                ):
                    mimic_id = mimic_col["id"]

                    for omop_col in omop_attributes:
                        if (
                            omop_col["relation_name"].lower()
                            == each_column_map["omop"]["table_id"].lower()
                        ):
                            if (
                                omop_col["attribute_name"].lower()
                                == each_column_map["omop"]["column_name"].lower()
                            ):
                                omop_id = omop_col["id"]
                                id_mapping_from_mimic_to_omop.append(
                                    {"mimic_id": mimic_id, "omop_id": omop_id}
                                )
                                break
    dump_data_to_jsonl(id_mapping_from_mimic_to_omop, output_ground_truth)


def dataset_split(split_name):
    split_name_to_relation_names = {
        "AdCO": ("admissions", "condition_occurrence"),
        "AdVD": ("admissions", "visit_detail"),
        "AdVO": ("admissions", "visit_occurrence"),
        "DiCO": ("diagnoses_icd", "condition_occurrence"),
        "LaMe": ("labevents", "measurement"),
        "PaPe": ("patients", "person"),
        "PrDE": ("prescriptions", "drug_exposure"),
        "SeVD": ("services", "visit_detail"),
        "TrVD": ("transfers", "visit_detail"),
    }
    assert split_name in split_name_to_relation_names
    mimic_relation_name = split_name_to_relation_names[split_name][0]
    omop_relation_name = split_name_to_relation_names[split_name][1]
    return mimic_relation_name, omop_relation_name


def load_states(
    jsonl_file_name,
    pydantic_type,
    is_target=False,
    single_state=False,
    filter_fn=lambda x: True,
):
    dataset = get_data_from_jsonl(jsonl_file_name)
    dataset = [each_data for each_data in dataset if filter_fn(each_data)]
    if is_target:
        if single_state:
            dataset = [{"attributes": dataset}]
        else:
            dataset = [{"attributes": [each_data]} for each_data in dataset]
    model_list = [pydantic_type.model_validate(each_data) for each_data in dataset]
    return model_list


crew_prompt_params = {
    "role": "Schema mathcer for relational schemas.",
    "goal": "Your task is to create semantic matches that specify how the elements of the source schema and the target schema semantically correspond to on another.",
    "backstory": "Two attributes semantically match if and only if there exists an invertible function that maps all values of one attribute to the other. First, I will input the name of an attribute from the source schema, a description of the attribute, the name of the relation it belongs to and a description of this relation. After that, I will input the same information of a sinlge relation and all its attributes from the target schema.",
    "expected_output": "The expected output is described by Pydantic Type, where it provides a list of True/False assignment to the candidate attribute mappings.",
}

prompt_tempate_1_n = """The attribute from the **source schema** is the following:
attribute_name:{attribute_name}
attribute_description:{attribute_description}
relation_name:{relation_name}
relation_description:{relation_description}

The **list of target schema** is given as a list of the Attributes:
attributes:{attributes}

The **total number of target schema is {num_target_schema}**.
This is the number of target attributes that you will check.
"""

custom_instruction = """Explain which of the target attributes semantically match to the source schema.
Let's work this out step by step to make sure we get it correct. After your explanation, give a final decision JSON-format like {{"invertible": []}}.
Scan the **list of target schema** one by one and check each target attributes.
The value is True if there is an invertible function that maps all values of the source attributes to the values of the target attributes;
False if ther is no such function. Do not mention an attribute if there is not enough information to decide.
As a default value, we will give an initial list filled-with False for all target schema. Change the value to True if the correct value is True.
Otherwise keep default value False.
"""


if __name__ == "__main__":
    data_path = os.fspath(
        Path(__file__).resolve().parent.parent.parent / "data" / "schema_matching"
    )

    preprocess_schema_file(
        input_schema_file=os.path.join(data_path, "mimic_schema.jsonl"),
        output_schema_file=os.path.join(data_path, "mimic_attributes.jsonl"),
    )

    preprocess_schema_file(
        input_schema_file=os.path.join(data_path, "omop_schema.jsonl"),
        output_schema_file=os.path.join(data_path, "omop_attributes.jsonl"),
    )

    preprocess_ground_truth(
        input_ground_truth=os.path.join(data_path, "ground_truth_schema_matches.jsonl"),
        output_ground_truth=os.path.join(data_path, "ground_truth_id_matches.jsonl"),
        mimic_attribute_file=os.path.join(data_path, "mimic_attributes.jsonl"),
        omop_attribute_file=os.path.join(data_path, "omop_attributes.jsonl"),
    )
