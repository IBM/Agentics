#!/usr/bin/env python3
"""
Generate Flink SQL CREATE TABLE statements for AGStream Manager channels
This script queries AGStream Manager and generates SQL statements that can be
executed in the Flink SQL client to register all Kafka topics as tables.
"""

import sys
from typing import Dict, List

import requests


def get_all_channels(manager_url: str = "http://localhost:5003") -> List[Dict]:
    """Fetch all registered channels from AGStream Manager."""
    try:
        response = requests.get(f"{manager_url}/api/channels", timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("channels", [])
    except Exception as e:
        print(f"✗ Error fetching channels: {e}", file=sys.stderr)
        sys.exit(1)


def python_type_to_flink_type(py_type: str) -> str:
    """Convert Python type string to Flink SQL type."""
    type_mapping = {
        "str": "STRING",
        "int": "BIGINT",
        "float": "DOUBLE",
        "bool": "BOOLEAN",
        "list": "ARRAY<STRING>",
        "dict": "MAP<STRING, STRING>",
        "datetime": "TIMESTAMP(3)",
    }
    return type_mapping.get(py_type, "STRING")


def generate_create_table_sql(
    channel: Dict,
    kafka_server: str = "kafka:29092",
    schema_registry_url: str = "http://karapace-schema-registry:8081",
) -> str:
    """Generate CREATE TABLE statement for a channel."""
    topic = channel["topic"]
    schema = channel["schema"]

    # Build field definitions
    fields = []
    for field_name, field_type in schema.items():
        flink_type = python_type_to_flink_type(field_type)
        fields.append(f"  `{field_name}` {flink_type}")

    fields_str = ",\n".join(fields)

    # Generate CREATE TABLE statement
    sql = f"""CREATE TABLE IF NOT EXISTS `{topic}` (
{fields_str}
) WITH (
  'connector' = 'kafka',
  'topic' = '{topic}',
  'properties.bootstrap.servers' = '{kafka_server}',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '{schema_registry_url}'
);"""
    return sql


def main():
    """Generate SQL statements for all channels."""
    manager_url = "http://localhost:5003"
    kafka_server = "kafka:29092"
    schema_registry_url = "http://karapace-schema-registry:8081"

    print("-- Flink SQL Table Initialization Script", file=sys.stderr)
    print("-- Generated from AGStream Manager channels", file=sys.stderr)
    print("", file=sys.stderr)

    channels = get_all_channels(manager_url)

    if not channels:
        print("✗ No channels found in AGStream Manager", file=sys.stderr)
        sys.exit(1)

    print(f"-- Found {len(channels)} channels", file=sys.stderr)
    print("", file=sys.stderr)

    # Generate SQL statements
    print("-- Copy and paste these statements into Flink SQL Client")
    print(
        "-- Or pipe this script output: ./init_flink_tables.py | docker exec -i flink-jobmanager ./bin/sql-client.sh"
    )
    print("")

    # Register core agentics UDFs
    print("-- Register Core Agentics UDFs")
    print("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap")
    print("AS 'ag_operators.agmap' LANGUAGE PYTHON;")
    print("")
    print("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agreduce")
    print("AS 'agreduce.agreduce' LANGUAGE PYTHON;")
    print("")
    print("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agsearch")
    print("AS 'agsearch.agsearch' LANGUAGE PYTHON;")
    print("")
    print("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS explode_search_results")
    print("AS 'explode_search_results.explode_search_results' LANGUAGE PYTHON;")
    print("")

    # Register persistent search functions
    print("-- Register AGPersist Search Functions")
    print("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS build_search_index")
    print("AS 'agpersist_search.build_search_index' LANGUAGE PYTHON;")
    print("")
    print("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS search_index_json")
    print("AS 'agpersist_search.search_index_json' LANGUAGE PYTHON;")
    print("")
    print("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS list_search_indexes")
    print("AS 'agpersist_search.list_search_indexes' LANGUAGE PYTHON;")
    print("")
    print("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS list_indexes")
    print("AS 'agpersist_search.list_indexes' LANGUAGE PYTHON;")
    print("")
    print("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS search_persisted_index")
    print("AS 'agpersist_search.search_persisted_index' LANGUAGE PYTHON;")
    print("")
    print("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS remove_search_index")
    print("AS 'agpersist_search.remove_search_index' LANGUAGE PYTHON;")
    print("")

    # Register AGGenerate function
    print("-- Register AGGenerate Function")
    print("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS aggenerate")
    print("AS 'aggenerate.aggenerate' LANGUAGE PYTHON;")
    print("")
    print("-- All functions registered!")
    print("")

    for channel in channels:
        sql = generate_create_table_sql(channel, kafka_server, schema_registry_url)
        print(sql)
        print("")

    print("-- All tables created! You can now query them:", file=sys.stderr)
    print("-- Example: SELECT * FROM Questions LIMIT 10;", file=sys.stderr)
    print(
        "-- Persistent Search: SELECT build_search_index(column, 'index_name') FROM table;",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()

# Made with Bob
