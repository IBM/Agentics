## Schema matching in Agentics

This example shows how to perform schema matching with agentics.

### Datasets
1. We postprocessed schema matching dataset from [Schema Matching with LLMs](https://github.com/UHasselt-DSI-Data-Systems-Lab/code-schema-matching-LLMs-artefacs)
2. Json files are stored under `data/schema_matching`
   1. mimic_schema
   2. omop_schema
   3. ground_truth
   

### Brief Description of Schema matching task
* Input:  mimic_schema and omop_schema. Both define tables and columns
* Output: matching the columns between two schema
  * Example
    ```
    {
        "mimic": {
        "table_id": "Patients",
        "column_name": "anchor_year"
        },
        "omop": {
        "table_id": "Person",
        "column_name": "year_of_birth"
        }
    }
    ```

