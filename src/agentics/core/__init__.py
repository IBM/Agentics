# Optional MCP tools imports (require crewai-tools and mcp packages)
try:
    from .mcp_tools import (
        clear_mcp_cache,
        get_default_mcp_connection,
        get_mcp_connection_info,
        get_mcp_tools,
        mcp_tools,
    )
except ImportError:
    # MCP features not available without required dependencies
    # Install with: pip install crewai-tools mcp
    get_mcp_tools = None
    get_default_mcp_connection = None
    clear_mcp_cache = None
    get_mcp_connection_info = None
    mcp_tools = None

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
