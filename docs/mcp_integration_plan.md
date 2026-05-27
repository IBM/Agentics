# MCP Integration Plan for Agentics

## Overview

Add comprehensive MCP (Model Context Protocol) support to agentics that works with both local and remote MCP servers, with efficient caching for production use.

## Architecture

### 1. Core MCP Module (`src/agentics/core/mcp_tools.py`)

**Purpose**: Unified MCP connection management supporting both local and remote servers

**Key Features**:
- Auto-detect connection type (local file path vs HTTP URL)
- Thread-local caching for connection reuse
- Support for both stdio and HTTP/SSE transports
- Graceful error handling and cleanup

**API**:
```python
def get_mcp_tools(
    mcp_connection: str,
    use_cache: bool = True,
    timeout: int = 30
) -> list[Tool]:
    """
    Get MCP tools from local or remote server.
    
    Args:
        mcp_connection: Either file path or HTTP URL
        use_cache: Whether to cache the connection
        timeout: Connection timeout in seconds
    
    Returns:
        List of CrewAI-compatible tools
    """
```

### 2. Connection Types

#### Local (Stdio)
- **Pattern**: File path (e.g., `"path/to/server.py"`)
- **Use case**: Development, testing, single-machine deployments
- **Implementation**: `StdioServerParameters` + subprocess

#### Remote (HTTP/SSE)
- **Pattern**: HTTP URL (e.g., `"http://mcp-server:8080/sse"`)
- **Use case**: Production, distributed systems, Flink clusters
- **Implementation**: SSE client + HTTP transport

### 3. Caching Strategy

**Thread-Local Storage**:
```python
_thread_local = threading.local()
_thread_local.mcp_cache = {
    'connection_string': {
        'adapter': MCPServerAdapter,
        'tools': [Tool, ...],
        'created_at': timestamp
    }
}
```

**Benefits**:
- One MCP connection per thread/worker
- Reused across multiple function calls
- Automatic cleanup on thread termination

### 4. Integration Points

#### A. Transducible Functions
```python
from agentics.core.mcp_tools import get_mcp_tools

@transducible(
    tools=get_mcp_tools("http://mcp-server:8080/sse"),
    max_iter=15,
    reasoning=True
)
async def my_function(input: InputModel) -> OutputModel:
    return Transduce(input)
```

#### B. make_transducible_function
```python
my_func = make_transducible_function(
    InputModel=Input,
    OutputModel=Output,
    tools=get_mcp_tools("path/to/mcp_server.py"),
    max_iter=10
)
```

#### C. AG Direct Usage
```python
from agentics.core.mcp_tools import get_mcp_tools

tools = get_mcp_tools("http://mcp-server:8080/sse")
ag = AG(
    atype=MyModel,
    tools=tools,
    max_iter=15
)
```

#### D. Flink UDFs (ml_generate_table)
```python
@udtf(result_types=[DataTypes.STRING()])
def ml_generate_table(prompt, schema=None, instructions=None, mcp_server=None):
    tools = []
    if mcp_server:
        tools = get_mcp_tools(mcp_server)
    
    ag = AG(atype=model_class, tools=tools)
    # ... rest of implementation
```

### 5. Environment Variable Support

**Configuration**:
```bash
# .env file
MCP_SERVER_URL=http://mcp-server:8080/sse
MCP_SERVER_TIMEOUT=30
MCP_CACHE_ENABLED=true
```

**Usage**:
```python
def get_default_mcp_connection():
    return os.getenv("MCP_SERVER_URL", "http://localhost:8080/sse")

# Auto-use environment variable if no connection specified
tools = get_mcp_tools(mcp_connection or get_default_mcp_connection())
```

### 6. Error Handling

**Connection Failures**:
- Retry logic with exponential backoff
- Fallback to no-tools mode if MCP unavailable
- Clear error messages for debugging

**Tool Execution Failures**:
- Graceful degradation
- Timeout handling
- Error logging without crashing

### 7. Cleanup and Resource Management

**Automatic Cleanup**:
```python
import atexit

def cleanup_mcp_connections():
    """Clean up all MCP connections on shutdown."""
    if hasattr(_thread_local, "mcp_cache"):
        for conn_data in _thread_local.mcp_cache.values():
            try:
                conn_data['adapter'].__exit__(None, None, None)
            except:
                pass
        _thread_local.mcp_cache.clear()

atexit.register(cleanup_mcp_connections)
```

## Implementation Steps

### Phase 1: Core Module
1. Create `src/agentics/core/mcp_tools.py`
2. Implement connection detection logic
3. Add thread-local caching
4. Implement cleanup handlers

### Phase 2: Integration
1. Update `ml_generate_table` in agstream-manager
2. Add MCP parameter support
3. Update documentation

### Phase 3: Examples
1. Create local MCP example
2. Create remote MCP example
3. Create Flink SQL examples

### Phase 4: Testing
1. Unit tests for connection types
2. Integration tests with real MCP servers
3. Performance tests for caching

## Usage Examples

### Example 1: Local Development
```python
from agentics.core.mcp_tools import get_mcp_tools
from agentics.core.transducible_functions import make_transducible_function

# Use local MCP server
tools = get_mcp_tools("examples/mcp_server_example.py")

research_func = make_transducible_function(
    InputModel=Entity,
    OutputModel=Report,
    tools=tools,
    instructions="Research the entity using web search"
)

result = await research_func(Entity(name="Anthropic"))
```

### Example 2: Production (Remote)
```python
# Use remote MCP server
tools = get_mcp_tools("http://mcp-server:8080/sse")

research_func = make_transducible_function(
    InputModel=Entity,
    OutputModel=Report,
    tools=tools,
    instructions="Research the entity using web search"
)

# Process batch - connection reused for all entities
results = await research_func([
    Entity(name="Anthropic"),
    Entity(name="OpenAI"),
    Entity(name="Google DeepMind"),
])
```

### Example 3: Flink SQL
```sql
-- Set MCP server in environment or pass as parameter
SELECT 
    JSON_VALUE(T.result, '$.product_name') as product_name,
    JSON_VALUE(T.result, '$.review') as review
FROM products,
LATERAL TABLE(ml_generate_table(
    description,
    'STRUCT<product_name STRING, review STRING>',
    NULL,
    'http://mcp-server:8080/sse'  -- Remote MCP server
)) AS T(result);
```

## Benefits

1. **Flexibility**: Works in both development and production
2. **Performance**: Connection caching eliminates startup overhead
3. **Scalability**: Remote MCP servers scale independently
4. **Simplicity**: Same API for local and remote
5. **Production-Ready**: Designed for distributed Flink deployments

## Migration Path

### For Existing Code
```python
# Before (local only)
with MCPServerAdapter(StdioServerParameters(...)) as tools:
    ag = AG(atype=Model, tools=tools)

# After (local or remote)
tools = get_mcp_tools("http://mcp-server:8080/sse")
ag = AG(atype=Model, tools=tools)
```

### For New Code
```python
# Just use get_mcp_tools everywhere
from agentics.core.mcp_tools import get_mcp_tools

tools = get_mcp_tools(os.getenv("MCP_SERVER_URL"))
```

## Next Steps

1. Switch to code mode
2. Implement `src/agentics/core/mcp_tools.py`
3. Update `ml_generate_table` UDF
4. Create examples and tests
5. Update documentation
