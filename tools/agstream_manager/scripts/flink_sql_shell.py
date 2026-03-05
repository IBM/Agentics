#!/usr/bin/env python3
"""
PyFlink SQL Shell for AGStream Manager
Interactive SQL shell to query Kafka topics using PyFlink Table API
"""

import os
import sys
from pathlib import Path

try:
    from pyflink.table import EnvironmentSettings, TableEnvironment
    from pyflink.table.expressions import col
except ImportError:
    print("❌ PyFlink not installed!")
    print("Install with: pip install apache-flink")
    sys.exit(1)

try:
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError
except ImportError:
    print("❌ kafka-python not installed!")
    print("Install with: pip install kafka-python")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("❌ requests not installed!")
    print("Install with: pip install requests")
    sys.exit(1)


class FlinkSQLShell:
    def __init__(
        self,
        kafka_bootstrap="localhost:9092",
        schema_registry_url="http://localhost:8081",
    ):
        """Initialize PyFlink SQL Shell"""
        self.kafka_bootstrap = kafka_bootstrap
        self.schema_registry_url = schema_registry_url

        # Create streaming table environment
        env_settings = EnvironmentSettings.in_streaming_mode()
        self.table_env = TableEnvironment.create(env_settings)

        # Configure for better compatibility
        self._configure_environment()

        # Add Kafka connector JAR
        self._add_kafka_connector()

        print("=" * 70)
        print("🚀 PyFlink SQL Shell for AGStream Manager")
        print("=" * 70)
        print(f"Kafka Bootstrap: {self.kafka_bootstrap}")
        print(f"Schema Registry: {self.schema_registry_url}")
        print("=" * 70)
        print()

    def _configure_environment(self):
        """Configure Flink environment for better compatibility"""
        config = self.table_env.get_config().get_configuration()

        # Set parallelism to 1 for simpler execution
        config.set_string("parallelism.default", "1")

        # Configure for better error handling
        config.set_string("table.exec.sink.not-null-enforcer", "drop")

        # Set reasonable timeouts
        config.set_string("table.exec.resource.default-parallelism", "1")

    def _add_kafka_connector(self):
        """Add Kafka connector JAR to Flink"""
        # Look for Kafka connector JAR in flink-lib directory
        project_root = Path(__file__).parent.parent.parent.parent
        jar_path = (
            project_root / "flink-lib" / "flink-sql-connector-kafka-3.3.0-1.18.jar"
        )

        if jar_path.exists():
            self.table_env.get_config().get_configuration().set_string(
                "pipeline.jars", f"file://{jar_path}"
            )
            print(f"✓ Loaded Kafka connector: {jar_path}")
        else:
            print(f"⚠️  Kafka connector JAR not found at: {jar_path}")
            print("   Some features may not work without it.")
        print()

    def discover_kafka_topics(self):
        """Discover available Kafka topics"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_bootstrap, client_id="flink-sql-shell"
            )
            topics = admin_client.list_topics()
            # Filter out internal topics
            user_topics = [t for t in topics if not t.startswith("_")]
            admin_client.close()
            return user_topics
        except Exception as e:
            print(f"⚠️  Could not discover Kafka topics: {e}")
            return []

    def get_schema_from_registry(self, topic_name):
        """Get schema for a topic from Schema Registry"""
        try:
            # Try to get schema for topic-value subject
            response = requests.get(
                f"{self.schema_registry_url}/subjects/{topic_name}-value/versions/latest"
            )
            if response.status_code == 200:
                schema_data = response.json()
                schema_str = schema_data.get("schema", "{}")
                import json

                schema = json.loads(schema_str)
                return schema
            return None
        except Exception as e:
            return None

    def auto_register_topics(self):
        """Auto-discover and register all Kafka topics"""
        print("🔍 Discovering Kafka topics...")
        topics = self.discover_kafka_topics()

        if not topics:
            print("   No topics found")
            return

        print(f"   Found {len(topics)} topic(s)")
        print()

        registered = 0
        for topic in topics:
            # Try to get schema from registry
            schema = self.get_schema_from_registry(topic)

            if schema and schema.get("type") == "record":
                # Extract fields from Avro schema
                fields = {}
                for field in schema.get("fields", []):
                    field_name = field["name"]
                    field_type = field["type"]

                    # Map Avro types to SQL types
                    if isinstance(field_type, str):
                        if field_type == "string":
                            sql_type = "STRING"
                        elif field_type == "int":
                            sql_type = "INT"
                        elif field_type == "long":
                            sql_type = "BIGINT"
                        elif field_type == "float":
                            sql_type = "FLOAT"
                        elif field_type == "double":
                            sql_type = "DOUBLE"
                        elif field_type == "boolean":
                            sql_type = "BOOLEAN"
                        else:
                            sql_type = "STRING"
                    else:
                        sql_type = "STRING"

                    fields[field_name] = sql_type

                # Register with Avro format
                if self.register_kafka_topic(
                    topic, format_type="avro-confluent", schema_fields=fields
                ):
                    registered += 1
            else:
                # Register with JSON format and generic schema
                if self.register_kafka_topic(
                    topic, format_type="json", schema_fields={"data": "STRING"}
                ):
                    registered += 1

        print()
        print(f"✓ Registered {registered}/{len(topics)} topics")
        print()

    def register_kafka_topic(self, topic_name, format_type="json", schema_fields=None):
        """
        Register a Kafka topic as a Flink table

        Args:
            topic_name: Name of the Kafka topic
            format_type: 'json', 'avro', or 'avro-confluent'
            schema_fields: Dict of field_name: field_type (e.g., {'id': 'INT', 'name': 'STRING'})
        """
        if schema_fields:
            fields_sql = ", ".join(
                [f"{name} {dtype}" for name, dtype in schema_fields.items()]
            )
        else:
            # Default schema - will need to be customized
            fields_sql = "data STRING"

        if format_type == "avro-confluent":
            format_config = f"""
                'format' = 'avro-confluent',
                'avro-confluent.url' = '{self.schema_registry_url}'
            """
        elif format_type == "avro":
            format_config = "'format' = 'avro'"
        else:
            format_config = "'format' = 'json'"

        sql = f"""
            CREATE TABLE {topic_name} (
                {fields_sql}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic_name}',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap}',
                'properties.group.id' = 'flink-sql-shell',
                'scan.startup.mode' = 'earliest-offset',
                {format_config}
            )
        """

        try:
            self.table_env.execute_sql(sql)
            print(f"✓ Registered topic '{topic_name}' as table")
            return True
        except Exception as e:
            error_msg = str(e)
            if "already exists" in error_msg.lower():
                print(f"ℹ️  Table '{topic_name}' already registered")
            else:
                print(f"✗ Failed to register topic '{topic_name}': {e}")
            return False

    def execute_sql(self, sql_query):
        """Execute a SQL query and print results"""
        try:
            # Check if it's a SELECT query
            if sql_query.strip().upper().startswith("SELECT"):
                # For SELECT queries, use execute_sql which handles streaming better
                print("⏳ Executing query...")
                print("   Note: This will show results as they arrive (streaming mode)")
                print("   Press Ctrl+C to stop")
                print()

                result = self.table_env.execute_sql(sql_query)

                # Print results - this will stream them
                with result.collect() as results:
                    count = 0
                    print("📊 Results:")
                    print("-" * 80)
                    for row in results:
                        print(row)
                        count += 1
                        if count >= 100:  # Limit to 100 rows
                            print()
                            print(f"... (showing first 100 rows, press Ctrl+C to stop)")
                            break

                    if count == 0:
                        print("📭 No results found (topic may be empty)")
                    else:
                        print("-" * 80)
                        print(f"Total rows shown: {count}")

                return True
            else:
                # For non-SELECT queries (DDL, etc.)
                result = self.table_env.execute_sql(sql_query)
                result.print()
                return True
        except KeyboardInterrupt:
            print("\n⚠️  Query cancelled by user")
            return False
        except Exception as e:
            print(f"✗ Error executing query: {e}")
            return False

    def show_tables(self):
        """Show all registered tables"""
        print("\n📋 Registered Tables:")
        self.execute_sql("SHOW TABLES")

    def describe_table(self, table_name):
        """Describe table schema"""
        print(f"\n📊 Schema for '{table_name}':")
        self.execute_sql(f"DESCRIBE {table_name}")

    def query_table(self, table_name, limit=10):
        """Query a table with limit"""
        print(f"\n🔍 Querying '{table_name}' (limit {limit}):")
        self.execute_sql(f"SELECT * FROM {table_name} LIMIT {limit}")

    def run_interactive(self):
        """Run interactive SQL shell"""
        # Auto-register topics on startup
        self.auto_register_topics()

        print("📝 Interactive SQL Shell")
        print("Commands:")
        print("  - Type SQL queries (e.g., SELECT * FROM my_topic LIMIT 10)")
        print("  - 'show tables' - List all tables")
        print("  - 'describe <table>' - Show table schema")
        print("  - 'register <topic>' - Register a Kafka topic")
        print("  - 'discover' - Re-discover and register Kafka topics")
        print("  - 'help' - Show this help")
        print("  - 'exit' or 'quit' - Exit shell")
        print()

        while True:
            try:
                import readline  # Enable command line editing

                query = input("flink-sql> ").strip()

                if not query:
                    continue

                if query.lower() in ["exit", "quit"]:
                    print("👋 Goodbye!")
                    break

                if query.lower() == "help":
                    self.show_help()
                    continue

                if query.lower() == "show tables":
                    self.show_tables()
                    continue

                if query.lower().startswith("describe "):
                    table_name = query.split()[1]
                    self.describe_table(table_name)
                    continue

                if query.lower().startswith("register "):
                    topic_name = query.split()[1]
                    self.register_kafka_topic(topic_name)
                    continue

                if query.lower() == "discover":
                    self.auto_register_topics()
                    continue

                # Execute as SQL query
                self.execute_sql(query)

            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except EOFError:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"✗ Error: {e}")

    def show_help(self):
        """Show help message"""
        print(
            """
╔══════════════════════════════════════════════════════════════════════╗
║                    PyFlink SQL Shell - Help                          ║
╚══════════════════════════════════════════════════════════════════════╝

📋 COMMANDS:
  show tables              - List all registered tables
  describe <table>         - Show table schema
  register <topic>         - Register a Kafka topic as a table
  help                     - Show this help message
  exit, quit               - Exit the shell

📝 SQL QUERIES:
  SELECT * FROM my_topic LIMIT 10
  SELECT field1, COUNT(*) FROM my_topic GROUP BY field1
  SELECT * FROM my_topic WHERE field2 > 100

💡 EXAMPLES:
  flink-sql> show tables
  flink-sql> register Q4
  flink-sql> describe Q4
  flink-sql> SELECT * FROM Q4 LIMIT 5

🔗 KAFKA CONNECTION:
  Bootstrap: {self.kafka_bootstrap}
  Schema Registry: {self.schema_registry_url}
        """
        )


def main():
    """Main entry point"""
    # Get configuration from environment or use defaults
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

    # Create shell
    shell = FlinkSQLShell(kafka_bootstrap, schema_registry)

    # Run interactive mode
    shell.run_interactive()


if __name__ == "__main__":
    main()

# Made with Bob
