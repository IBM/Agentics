"""
PyFlink SQL Auto-Connector Example

This example demonstrates how to use FlinkSQLAutoConnector to automatically
discover and query all AGStream Manager channels using PyFlink SQL.

The connector:
1. Queries AGStream Manager for all registered channels
2. Automatically generates CREATE TABLE statements
3. Registers tables in PyFlink
4. Allows you to query topics by name

Prerequisites:
- AGStream Manager running (http://localhost:5003)
- Kafka running (localhost:9092)
- Schema Registry running (http://localhost:8081)
- PyFlink installed: pip install apache-flink
- Some channels registered in AGStream Manager
"""

from agentics.core import FlinkSQLAutoConnector, create_flink_sql_environment


def example_basic_usage():
    """Basic usage: Create environment and run a simple query."""
    print("\n" + "=" * 80)
    print("Example 1: Basic Usage")
    print("=" * 80)

    # Create connector
    connector = FlinkSQLAutoConnector(
        manager_url="http://localhost:5003",
        kafka_server="kafka:9092",
        schema_registry_url="http://schema-registry:8081",
        verbose=True,
    )

    # Get PyFlink environment with all tables registered
    table_env = connector.get_queryable_environment()

    # List available tables
    print("\n📋 Available tables:")
    tables = connector.list_available_tables()
    for table in tables:
        print(f"  - {table}")

    # Query a table (replace 'questions' with your actual topic name)
    if tables:
        topic = tables[0]
        print(f"\n🔍 Querying table '{topic}':")
        try:
            result = table_env.execute_sql(f"SELECT * FROM `{topic}` LIMIT 5")
            result.print()
        except Exception as e:
            print(f"  Note: {e}")
            print(f"  (This is expected if the topic is empty)")


def example_convenience_function():
    """Using the convenience function for quick setup."""
    print("\n" + "=" * 80)
    print("Example 2: Convenience Function")
    print("=" * 80)

    # Quick setup with convenience function
    table_env = create_flink_sql_environment(
        manager_url="http://localhost:5003",
        kafka_server="kafka:9092",
        schema_registry_url="http://schema-registry:8081",
    )

    # Now you can run any SQL query
    print("\n🔍 Running SQL query:")
    try:
        # Show all tables
        result = table_env.execute_sql("SHOW TABLES")
        result.print()
    except Exception as e:
        print(f"  Error: {e}")


def example_advanced_queries():
    """Advanced SQL queries with joins and aggregations."""
    print("\n" + "=" * 80)
    print("Example 3: Advanced SQL Queries")
    print("=" * 80)

    connector = FlinkSQLAutoConnector(
        manager_url="http://localhost:5003",
        kafka_server="kafka:9092",
        schema_registry_url="http://schema-registry:8081",
        verbose=True,
    )

    table_env = connector.get_queryable_environment()
    tables = connector.list_available_tables()

    if not tables:
        print("  No tables available for querying")
        return

    topic = tables[0]

    # Example 1: Aggregation query
    print(f"\n📊 Aggregation query on '{topic}':")
    try:
        sql = f"""
        SELECT COUNT(*) as total_count
        FROM `{topic}`
        """
        result = table_env.execute_sql(sql)
        result.print()
    except Exception as e:
        print(f"  Error: {e}")

    # Example 2: Filtering query
    print(f"\n🔍 Filtering query on '{topic}':")
    try:
        # Get schema to know what fields are available
        schema = connector.get_table_schema(topic)
        if schema:
            first_field = list(schema.keys())[0]
            sql = f"""
            SELECT *
            FROM `{topic}`
            WHERE `{first_field}` IS NOT NULL
            LIMIT 10
            """
            result = table_env.execute_sql(sql)
            result.print()
    except Exception as e:
        print(f"  Error: {e}")


def example_schema_inspection():
    """Inspect available tables and their schemas."""
    print("\n" + "=" * 80)
    print("Example 4: Schema Inspection")
    print("=" * 80)

    connector = FlinkSQLAutoConnector(
        manager_url="http://localhost:5003",
        kafka_server="kafka:9092",
        schema_registry_url="http://schema-registry:8081",
        verbose=False,  # Disable verbose for cleaner output
    )

    # Print all available tables with their schemas
    connector.print_available_tables()

    # Get schema for a specific table
    tables = connector.list_available_tables()
    if tables:
        topic = tables[0]
        schema = connector.get_table_schema(topic)
        print(f"\n📋 Schema for '{topic}':")
        for field_name, field_type in schema.items():
            flink_type = connector.python_type_to_flink_type(field_type)
            print(f"  {field_name}: {flink_type}")


def example_multi_topic_join():
    """Example of joining multiple topics."""
    print("\n" + "=" * 80)
    print("Example 5: Multi-Topic Join")
    print("=" * 80)

    connector = FlinkSQLAutoConnector(
        manager_url="http://localhost:5003",
        kafka_server="kafka:9092",
        schema_registry_url="http://schema-registry:8081",
        verbose=True,
    )

    table_env = connector.get_queryable_environment()
    tables = connector.list_available_tables()

    if len(tables) < 2:
        print("  Need at least 2 tables for join example")
        return

    topic1, topic2 = tables[0], tables[1]

    print(f"\n🔗 Joining '{topic1}' and '{topic2}':")
    try:
        # This is a simple example - adjust based on your actual schemas
        sql = f"""
        SELECT t1.*, t2.*
        FROM `{topic1}` t1
        JOIN `{topic2}` t2
        ON t1.id = t2.id
        LIMIT 10
        """
        result = table_env.execute_sql(sql)
        result.print()
    except Exception as e:
        print(f"  Note: {e}")
        print(f"  (Adjust the JOIN condition based on your actual schemas)")


def main():
    """Run all examples."""
    print("\n" + "=" * 80)
    print("PyFlink SQL Auto-Connector Examples")
    print("=" * 80)
    print("\nThis script demonstrates automatic discovery and querying of")
    print("AGStream Manager channels using PyFlink SQL.")
    print("\nMake sure AGStream Manager, Kafka, and Schema Registry are running!")

    try:
        # Run examples
        example_basic_usage()
        example_convenience_function()
        example_schema_inspection()
        example_advanced_queries()
        example_multi_topic_join()

        print("\n" + "=" * 80)
        print("✅ All examples completed!")
        print("=" * 80)

    except ImportError as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure PyFlink is installed:")
        print("  pip install apache-flink")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure:")
        print("  1. AGStream Manager is running (http://localhost:5003)")
        print("  2. Kafka is running (localhost:9092)")
        print("  3. Schema Registry is running (http://localhost:8081)")
        print("  4. Some channels are registered in AGStream Manager")


if __name__ == "__main__":
    main()


# Made with Bob
