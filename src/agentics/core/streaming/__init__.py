"""
AGStream - Streaming extensions for Agentics

This module provides Kafka-based streaming capabilities for Agentics.
Requires kafka-python and related dependencies.

Note: These imports will fail gracefully if streaming dependencies are not installed.
"""

# Try to import streaming modules, but don't fail if dependencies are missing
try:
    from .agstream_sql import AGStreamSQL
    from .flink_listener_manager import FlinkListenerManager
    from .flink_sql_connector import FlinkSQLAutoConnector, create_flink_sql_environment
    from .listener_manager import ListenerInfo, ListenerManager
    from .streaming import AGStream
    from .streaming_utils import *

    __all__ = [
        "AGStreamSQL",
        "FlinkListenerManager",
        "FlinkSQLAutoConnector",
        "create_flink_sql_environment",
        "ListenerInfo",
        "ListenerManager",
        "AGStream",
    ]
except ImportError as e:
    # Streaming dependencies not available
    # This is expected if kafka-python, apache-flink, etc. are not installed
    import warnings

    warnings.warn(
        f"AGStream streaming features not available: {e}. "
        "Install streaming dependencies to use these features.",
        ImportWarning,
    )
    __all__ = []

# Made with Bob
