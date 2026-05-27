"""
MCP Tools Integration for Agentics

This module provides utilities for integrating Model Context Protocol (MCP) servers
with agentics, supporting both local (stdio) and remote (HTTP/SSE) connections.

Features:
- Auto-detect connection type (local file path vs HTTP URL)
- Thread-local caching for efficient connection reuse
- Support for both stdio and HTTP/SSE transports
- Graceful error handling and automatic cleanup

Usage:
    from agentics.core.mcp_tools import get_mcp_tools

    # Local MCP server
    tools = get_mcp_tools("path/to/mcp_server.py")

    # Remote MCP server
    tools = get_mcp_tools("http://mcp-server:8080/sse")

    # Use with AG
    ag = AG(atype=MyModel, tools=tools)
"""

import atexit
import os
import sys
import threading
import time
from typing import Any, Optional

# Thread-local storage for MCP connections
_thread_local = threading.local()


def get_mcp_tools(
    mcp_connection: Optional[str] = None,
    use_cache: bool = True,
    timeout: int = 30,
    verbose: bool = False,
) -> list[Any]:
    """
    Get MCP tools from either a local or remote MCP server.

    This function automatically detects whether the connection is to a local
    MCP server (file path) or a remote server (HTTP URL) and establishes the
    appropriate connection. Connections are cached per thread for efficiency.

    Args:
        mcp_connection: Either:
            - File path to local MCP server (e.g., "path/to/server.py")
            - HTTP URL to remote MCP server (e.g., "http://mcp-server:8080/sse")
            - None to use MCP_SERVER_URL environment variable
        use_cache: Whether to cache the connection (default: True)
        timeout: Connection timeout in seconds (default: 30)
        verbose: Print debug information (default: False)

    Returns:
        List of CrewAI-compatible tools from the MCP server

    Raises:
        ValueError: If connection string is invalid or server is unreachable
        ImportError: If required MCP packages are not installed

    Examples:
        >>> # Local development
        >>> tools = get_mcp_tools("examples/mcp_server_example.py")
        >>> ag = AG(atype=MyModel, tools=tools)

        >>> # Production (remote)
        >>> tools = get_mcp_tools("http://mcp-server:8080/sse")
        >>> ag = AG(atype=MyModel, tools=tools)

        >>> # Use environment variable
        >>> os.environ["MCP_SERVER_URL"] = "http://mcp-server:8080/sse"
        >>> tools = get_mcp_tools()
    """
    # Get connection string from parameter or environment
    if mcp_connection is None:
        mcp_connection = os.getenv("MCP_SERVER_URL")
        if not mcp_connection:
            raise ValueError(
                "No MCP connection specified. Either pass mcp_connection parameter "
                "or set MCP_SERVER_URL environment variable."
            )

    # Initialize cache if needed
    if not hasattr(_thread_local, "mcp_cache"):
        _thread_local.mcp_cache = {}

    # Return cached connection if available
    if use_cache and mcp_connection in _thread_local.mcp_cache:
        cache_entry = _thread_local.mcp_cache[mcp_connection]
        if verbose:
            print(
                f"[MCP] Using cached connection to {mcp_connection} "
                f"(age: {time.time() - cache_entry['created_at']:.1f}s)",
                file=sys.stderr,
            )
        return cache_entry["tools"]

    # Import required packages
    try:
        from crewai_tools import MCPServerAdapter
    except ImportError:
        raise ImportError(
            "crewai-tools package is required for MCP integration. "
            "Install it with: pip install crewai-tools"
        )

    # Detect connection type and create appropriate parameters
    is_remote = mcp_connection.startswith("http://") or mcp_connection.startswith(
        "https://"
    )

    if is_remote:
        # Remote HTTP/SSE connection
        if verbose:
            print(
                f"[MCP] Connecting to remote MCP server: {mcp_connection}",
                file=sys.stderr,
            )

        try:
            from mcp.client.sse import sse_client

            # For remote connections, we need to use SSE client
            # Note: MCPServerAdapter may need to be configured differently for SSE
            # This is a placeholder - actual implementation depends on crewai-tools version
            server_params = {
                "url": mcp_connection,
                "timeout": timeout,
            }

            if verbose:
                print(
                    f"[MCP] Using SSE transport with timeout={timeout}s",
                    file=sys.stderr,
                )

        except ImportError:
            raise ImportError(
                "mcp package with SSE support is required for remote connections. "
                "Install it with: pip install mcp"
            )
    else:
        # Local stdio connection
        if verbose:
            print(f"[MCP] Starting local MCP server: {mcp_connection}", file=sys.stderr)

        # Verify file exists
        if not os.path.exists(mcp_connection):
            raise ValueError(
                f"MCP server file not found: {mcp_connection}\n"
                f"Please ensure the file exists and the path is correct."
            )

        try:
            from mcp import StdioServerParameters

            server_params = StdioServerParameters(
                command="python3",
                args=[mcp_connection],
                env={"UV_PYTHON": "3.12", **os.environ},
            )

            if verbose:
                print(f"[MCP] Using stdio transport", file=sys.stderr)

        except ImportError:
            raise ImportError(
                "mcp package is required for local MCP servers. "
                "Install it with: pip install mcp"
            )

    # Create MCP adapter and extract tools list
    try:
        adapter = MCPServerAdapter(server_params)
        adapter.__enter__()  # Start the connection

        # Get the tools list from the adapter
        # adapter.tools returns a ToolCollection which is list-like
        tools = adapter.tools

        if verbose:
            tool_names = [tool.name for tool in tools]
            print(
                f"[MCP] ✓ Connected successfully. Tools: {tool_names}",
                file=sys.stderr,
            )

        # Cache the connection
        if use_cache:
            _thread_local.mcp_cache[mcp_connection] = {
                "adapter": adapter,
                "tools": tools,
                "created_at": time.time(),
                "connection_type": "remote" if is_remote else "local",
            }

            if verbose:
                print(f"[MCP] Connection cached for reuse", file=sys.stderr)

        return tools

    except Exception as e:
        error_msg = f"Failed to connect to MCP server at {mcp_connection}: {e}"
        if verbose:
            print(f"[MCP] ✗ {error_msg}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
        raise ValueError(error_msg) from e


def get_default_mcp_connection() -> Optional[str]:
    """
    Get the default MCP connection from environment variables.

    Checks the following environment variables in order:
    1. MCP_SERVER_URL - Primary configuration
    2. MCP_SERVER_PATH - Alternative for local servers

    Returns:
        MCP connection string or None if not configured

    Example:
        >>> os.environ["MCP_SERVER_URL"] = "http://mcp-server:8080/sse"
        >>> connection = get_default_mcp_connection()
        >>> tools = get_mcp_tools(connection)
    """
    return os.getenv("MCP_SERVER_URL") or os.getenv("MCP_SERVER_PATH")


def clear_mcp_cache(connection: Optional[str] = None):
    """
    Clear cached MCP connections.

    Args:
        connection: Specific connection to clear, or None to clear all

    Example:
        >>> # Clear specific connection
        >>> clear_mcp_cache("http://mcp-server:8080/sse")

        >>> # Clear all connections
        >>> clear_mcp_cache()
    """
    if not hasattr(_thread_local, "mcp_cache"):
        return

    if connection:
        # Clear specific connection
        if connection in _thread_local.mcp_cache:
            cache_entry = _thread_local.mcp_cache[connection]
            try:
                cache_entry["adapter"].__exit__(None, None, None)
            except:
                pass
            del _thread_local.mcp_cache[connection]
    else:
        # Clear all connections
        for cache_entry in _thread_local.mcp_cache.values():
            try:
                cache_entry["adapter"].__exit__(None, None, None)
            except:
                pass
        _thread_local.mcp_cache.clear()


def cleanup_mcp_connections():
    """
    Clean up all MCP connections on shutdown.

    This function is automatically registered with atexit to ensure
    proper cleanup of MCP connections when the program exits.
    """
    if hasattr(_thread_local, "mcp_cache"):
        for connection, cache_entry in _thread_local.mcp_cache.items():
            try:
                cache_entry["adapter"].__exit__(None, None, None)
            except:
                pass
        _thread_local.mcp_cache.clear()


# Register cleanup handler
atexit.register(cleanup_mcp_connections)


def get_mcp_connection_info() -> dict[str, Any]:
    """
    Get information about current MCP connections.

    Returns:
        Dictionary with connection information including:
        - connection_count: Number of active connections
        - connections: List of connection details

    Example:
        >>> info = get_mcp_connection_info()
        >>> print(f"Active connections: {info['connection_count']}")
    """
    if not hasattr(_thread_local, "mcp_cache"):
        return {"connection_count": 0, "connections": []}

    connections = []
    for connection, cache_entry in _thread_local.mcp_cache.items():
        connections.append(
            {
                "connection": connection,
                "type": cache_entry["connection_type"],
                "tool_count": len(cache_entry["tools"]),
                "age_seconds": time.time() - cache_entry["created_at"],
            }
        )

    return {
        "connection_count": len(connections),
        "connections": connections,
    }


# Convenience function for common use case
def mcp_tools(connection: Optional[str] = None, **kwargs) -> list[Any]:
    """
    Convenience alias for get_mcp_tools with shorter name.

    Args:
        connection: MCP connection string (file path or URL)
        **kwargs: Additional arguments passed to get_mcp_tools

    Returns:
        List of MCP tools

    Example:
        >>> from agentics.core.mcp_tools import mcp_tools
        >>> tools = mcp_tools("http://mcp-server:8080/sse")
    """
    return get_mcp_tools(connection, **kwargs)


__all__ = [
    "get_mcp_tools",
    "get_default_mcp_connection",
    "clear_mcp_cache",
    "cleanup_mcp_connections",
    "get_mcp_connection_info",
    "mcp_tools",
]

# Made with Bob
