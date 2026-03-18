"""
FlinkSQLAutoConnector - Automatic PyFlink SQL table registration from AGStream Manager

This module provides automatic discovery and registration of Kafka topics as Flink SQL tables
by querying the AGStream Manager for channel schemas.
"""

import sys
from typing import Dict, List, Optional

import requests


class FlinkSQLAutoConnector:
    """
    Automatically connects PyFlink SQL to all AGStream Manager channels.

    Queries the AGStream Manager API to discover all registered channels and their schemas,
    then automatically generates and executes CREATE TABLE statements in Flink SQL.
    This allows you to query Kafka topics using their topic names as table names.

    Example:
        >>> connector = FlinkSQLAutoConnector(
        ...     manager_url="http://localhost:5003",
        ...     kafka_server="kafka:9092",
        ...     schema_registry_url="http://schema-registry:8081"
        ... )
        >>>
        >>> # Get a Flink environment with all tables pre-registered
        >>> table_env = connector.get_queryable_environment()
        >>>
        >>> # Query any topic by name!
        >>> result = table_env.execute_sql("SELECT * FROM questions LIMIT 10")
        >>> result.print()
    """

    def __init__(
        self,
        manager_url: str = "http://localhost:5003",
        kafka_server: str = "kafka:9092",
        schema_registry_url: str = "http://schema-registry:8081",
        verbose: bool = True,
    ):
        """
        Initialize the FlinkSQLAutoConnector.

        Args:
            manager_url: AGStream Manager service URL
            kafka_server: Kafka bootstrap server address
            schema_registry_url: Schema Registry URL
            verbose: Print progress messages
        """
        self.manager_url = manager_url.rstrip("/")
        self.kafka_server = kafka_server
        self.schema_registry_url = schema_registry_url
        self.verbose = verbose

    def _log(self, message: str):
        """Print message if verbose mode is enabled."""
        if self.verbose:
            sys.stderr.write(f"{message}\n")
            sys.stderr.flush()

    def get_all_channels(self) -> List[Dict]:
        """
        Fetch all registered channels from AGStream Manager.

        Returns:
            List of channel dictionaries with topic, atype_name, and schema fields
        """
        try:
            response = requests.get(f"{self.manager_url}/api/channels", timeout=10)
            response.raise_for_status()
            data = response.json()
            return data.get("channels", [])
        except Exception as e:
            self._log(f"✗ Error fetching channels from AGStream Manager: {e}")
            raise

    def python_type_to_flink_type(self, py_type: str) -> str:
        """
        Convert Python type string to Flink SQL type.

        Args:
            py_type: Python type as string (e.g., "str", "int", "float")

        Returns:
            Flink SQL type string
        """
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

    def generate_create_table_sql(self, channel: Dict) -> str:
        """
        Generate CREATE TABLE statement for a channel.

        Args:
            channel: Channel metadata from AGStream Manager
                {
                    "topic": "questions",
                    "atype_name": "Question",
                    "schema": {
                        "text": "str",
                        "timestamp": "int",
                        "category": "str"
                    }
                }

        Returns:
            Flink SQL CREATE TABLE statement
        """
        topic = channel["topic"]
        schema = channel["schema"]

        # Use lowercase table name to match topic name (Flink SQL is case-sensitive)
        # This ensures queries like "SELECT * FROM questions" work correctly
        table_name = topic.lower()

        # Build field definitions
        fields = []
        for field_name, field_type in schema.items():
            flink_type = self.python_type_to_flink_type(field_type)
            fields.append(f"  `{field_name}` {flink_type}")

        fields_str = ",\n".join(fields)

        # Generate CREATE TABLE statement
        sql = f"""CREATE TABLE IF NOT EXISTS `{table_name}` (
{fields_str}
) WITH (
  'connector' = 'kafka',
  'topic' = '{topic}',
  'properties.bootstrap.servers' = '{self.kafka_server}',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '{self.schema_registry_url}'
)"""
        return sql

    def initialize_all_tables(self) -> List[str]:
        """
        Generate CREATE TABLE statements for all registered channels.

        Returns:
            List of SQL statements
        """
        channels = self.get_all_channels()
        sql_statements = []

        self._log(f"\n📋 Found {len(channels)} channels in AGStream Manager")

        for channel in channels:
            sql = self.generate_create_table_sql(channel)
            sql_statements.append(sql)
            self._log(f"  ✓ Generated CREATE TABLE for '{channel['topic']}'")

        return sql_statements

    def get_queryable_environment(self):
        """
        Create a PyFlink TableEnvironment with all AGStream Manager channels
        registered as tables.

        Returns:
            PyFlink TableEnvironment ready for SQL queries

        Raises:
            ImportError: If PyFlink is not installed
        """
        try:
            from pyflink.table import EnvironmentSettings, TableEnvironment
        except ImportError:
            raise ImportError(
                "PyFlink is required for FlinkSQLAutoConnector. "
                "Install it with: pip install apache-flink"
            )

        # Create Flink Table Environment
        self._log("\n🚀 Creating PyFlink TableEnvironment...")
        env_settings = EnvironmentSettings.in_streaming_mode()
        table_env = TableEnvironment.create(env_settings)

        # Add Kafka connector JAR
        self._add_kafka_connector_jar(table_env)

        # Generate and execute CREATE TABLE statements
        sql_statements = self.initialize_all_tables()

        self._log(f"\n📝 Registering {len(sql_statements)} tables in Flink...")

        for idx, sql in enumerate(sql_statements, 1):
            try:
                table_env.execute_sql(sql)
                self._log(f"  [{idx}/{len(sql_statements)}] ✓ Table registered")
            except Exception as e:
                self._log(f"  [{idx}/{len(sql_statements)}] ✗ Error: {e}")
                # Continue with other tables even if one fails

        self._log(f"\n✅ Successfully registered {len(sql_statements)} tables")
        self._log("\nYou can now query topics by name:")
        self._log(
            "  table_env.execute_sql('SELECT * FROM your_topic LIMIT 10').print()"
        )
        self._log("")

        return table_env

    def _add_kafka_connector_jar(self, table_env):
        """Add Kafka connector and Avro format JARs to Flink environment."""
        from pathlib import Path

        # Look for both Kafka connector and Avro format JARs in flink-lib directory
        # Try multiple possible locations
        kafka_jar_name = "flink-sql-connector-kafka-3.3.0-1.18.jar"
        avro_jar_name = "flink-sql-avro-confluent-registry-1.18.1.jar"

        possible_base_paths = [
            Path.cwd() / "tools" / "agstream_manager" / "flink-lib",
            Path(__file__).parent.parent.parent.parent
            / "tools"
            / "agstream_manager"
            / "flink-lib",
            Path.home() / "tools" / "agstream_manager" / "flink-lib",
        ]

        kafka_jar_path = None
        avro_jar_path = None

        for base_path in possible_base_paths:
            if not kafka_jar_path and (base_path / kafka_jar_name).exists():
                kafka_jar_path = base_path / kafka_jar_name
            if not avro_jar_path and (base_path / avro_jar_name).exists():
                avro_jar_path = base_path / avro_jar_name
            if kafka_jar_path and avro_jar_path:
                break

        # Build JAR list
        jar_paths = []
        if kafka_jar_path:
            jar_paths.append(f"file://{kafka_jar_path}")
            self._log(f"✓ Loaded Kafka connector: {kafka_jar_path}")
        else:
            self._log(
                "⚠️  Kafka connector JAR not found in tools/agstream_manager/flink-lib/"
            )

        if avro_jar_path:
            jar_paths.append(f"file://{avro_jar_path}")
            self._log(f"✓ Loaded Avro format: {avro_jar_path}")
        else:
            self._log(
                "⚠️  Avro Confluent Registry JAR not found in tools/agstream_manager/flink-lib/"
            )

        if not jar_paths:
            self._log("⚠️  No connector JARs found!")
            self._log(
                "   Download them with: tools/agstream_manager/scripts/download_flink_avro.sh"
            )
            self._log(
                f"   Or place {kafka_jar_name} and {avro_jar_name} in tools/agstream_manager/flink-lib/"
            )
        else:
            # Set all JARs in pipeline.jars (semicolon-separated)
            table_env.get_config().get_configuration().set_string(
                "pipeline.jars", ";".join(jar_paths)
            )

    def list_available_tables(self) -> List[str]:
        """
        Get a list of all available table names (topics).

        Returns:
            List of topic names
        """
        channels = self.get_all_channels()
        return [channel["topic"] for channel in channels]

    def get_table_schema(self, topic: str) -> Optional[Dict]:
        """
        Get the schema for a specific topic.

        Args:
            topic: Topic name

        Returns:
            Schema dictionary or None if not found
        """
        channels = self.get_all_channels()
        for channel in channels:
            if channel["topic"] == topic:
                return channel["schema"]
        return None

    def print_available_tables(self):
        """Print a formatted list of all available tables with their schemas."""
        channels = self.get_all_channels()

        if not channels:
            self._log("No channels found in AGStream Manager")
            return

        self._log(f"\n{'='*80}")
        self._log(f"Available Tables ({len(channels)} total)")
        self._log(f"{'='*80}\n")

        for channel in channels:
            self._log(f"📊 Table: {channel['topic']}")
            self._log(f"   Type: {channel['atype_name']}")
            self._log(f"   Fields:")
            for field_name, field_type in channel["schema"].items():
                flink_type = self.python_type_to_flink_type(field_type)
                self._log(f"     - {field_name}: {flink_type}")
            self._log("")


# Convenience function for quick setup
def create_flink_sql_environment(
    manager_url: str = "http://localhost:5003",
    kafka_server: str = "kafka:9092",
    schema_registry_url: str = "http://schema-registry:8081",
    verbose: bool = True,
):
    """
    Convenience function to quickly create a PyFlink SQL environment
    with all AGStream Manager channels registered.

    Args:
        manager_url: AGStream Manager service URL
        kafka_server: Kafka bootstrap server address
        schema_registry_url: Schema Registry URL
        verbose: Print progress messages

    Returns:
        PyFlink TableEnvironment ready for SQL queries

    Example:
        >>> from agentics.core import create_flink_sql_environment
        >>>
        >>> table_env = create_flink_sql_environment()
        >>> result = table_env.execute_sql("SELECT * FROM questions LIMIT 10")
        >>> result.print()
    """
    connector = FlinkSQLAutoConnector(
        manager_url=manager_url,
        kafka_server=kafka_server,
        schema_registry_url=schema_registry_url,
        verbose=verbose,
    )
    return connector.get_queryable_environment()


# Made with Bob
