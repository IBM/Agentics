from .agentics import AG

# Optional streaming imports (require Kafka and Flink dependencies)
try:
    from .streaming import (
        AGStream,
        AGStreamSQL,
        FlinkListenerManager,
        FlinkSQLAutoConnector,
        ListenerInfo,
        ListenerManager,
        create_flink_sql_environment,
    )
except ImportError as e:
    # Streaming features not available without required dependencies
    # Install with: pip install kafka-python confluent-kafka apache-flink
    AGStreamSQL = None
    FlinkListenerManager = None
    FlinkSQLAutoConnector = None
    create_flink_sql_environment = None
    ListenerInfo = None
    ListenerManager = None
    AGStream = None
