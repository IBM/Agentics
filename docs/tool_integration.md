# 🔌 Tool Integration

Transducible functions can use external tools to enhance their capabilities through the Model Context Protocol (MCP). This enables LLM-powered workflows to access real-time data, execute code, query databases, and interact with external services.

---

## What Are Tools?

Tools extend transducible functions with external capabilities:

- **Web search** - Retrieve real-time information from the internet
- **Database queries** - Access structured data from SQL/NoSQL databases
- **API calls** - Integrate with external services and APIs
- **Code execution** - Run computations and scripts
- **File operations** - Read/write files and process documents
- **Custom functions** - Any Python function you define

---

## Using MCP Tools

MCP (Model Context Protocol) provides a standard way to expose tools to LLMs, making them discoverable and callable during transductions.

### Defining an MCP Server

First, create an MCP server with your tools (see `examples/mcp_server_example.py`):

```python
from ddgs import DDGS
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Search")

@mcp.tool()
def web_search(query: str, max_results: int = 5) -> list[str]:
    """Search the web using DuckDuckGo.
    
    Args:
        query: Search query with optional operators
        max_results: Number of results to return (5-20)
    
    Returns:
        List of search result snippets with titles and URLs
    """
    results = DDGS().text(query, max_results=max_results)
    return [f"{r['title']}\n{r['body']}\n{r['href']}" for r in results]

if __name__ == "__main__":
    mcp.run(transport="stdio")
```

### Using Tools in Transductions

There are several ways to import and use MCP tools in your transductions:

#### Option 1: Connect to Remote/External MCP Server

Use `MCPServerAdapter` from `crewai_tools` to connect to a remote or external MCP server (e.g., a server provided by a third party or running on another machine):

```python
import os
from pydantic import BaseModel
from typing import Optional
from mcp import StdioServerParameters
from crewai_tools import MCPServerAdapter
from agentics.core.transducible_functions import transducible, Transduce

class ResearchQuery(BaseModel):
    topic: Optional[str] = None
    focus_area: Optional[str] = None

class ResearchReport(BaseModel):
    summary: Optional[str] = None
    key_findings: Optional[list[str]] = None
    sources: Optional[list[str]] = None

# Configure connection to remote MCP server
# The server could be:
# - A third-party MCP server (e.g., from a service provider)
# - An MCP server running on another machine
# - A pre-existing MCP server script
server_params = StdioServerParameters(
    command="python3",
    args=["path/to/remote/mcp_server.py"],  # Path to the MCP server
    env={"UV_PYTHON": "3.12", **os.environ},
)

# Connect to the remote MCP server
with MCPServerAdapter(server_params) as server_tools:
    print(f"Available tools from remote server: {[tool.name for tool in server_tools]}")
    
    @transducible(
        tools=server_tools,  # Use tools from remote MCP server
        reasoning=True,
        max_iter=5
    )
    async def research_topic(state: ResearchQuery) -> ResearchReport:
        """Research a topic using tools from remote MCP server."""
        return Transduce(state)
    
    # Execute
    query = ResearchQuery(
        topic="Agentics framework",
        focus_area="practical applications"
    )
    report = await research_topic(query)
```

**Example: Connecting to a Third-Party MCP Server**

```python
# Example: Connect to a hypothetical weather MCP server
weather_server_params = StdioServerParameters(
    command="npx",  # MCP servers can be in any language
    args=["-y", "@weather/mcp-server"],  # npm package
)

with MCPServerAdapter(weather_server_params) as weather_tools:
    print(f"Weather tools: {[tool.name for tool in weather_tools]}")
    
    @transducible(tools=weather_tools, reasoning=True)
    async def get_weather_report(location: Location) -> WeatherReport:
        return Transduce(location)
```

**Note:** The `MCPServerAdapter` handles the connection lifecycle - it starts the server process when entering the context and stops it when exiting. You don't manage the server manually.

#### Option 2: Use CrewAI Tools

Agentics supports CrewAI tools, which provide a standardized interface for tool integration:

```python
from pydantic import BaseModel
from typing import Optional
from agentics.core.transducible_functions import transducible, Transduce
from crewai_tools import tool

class ResearchQuery(BaseModel):
    topic: Optional[str] = None
    focus_area: Optional[str] = None

class ResearchReport(BaseModel):
    summary: Optional[str] = None
    key_findings: Optional[list[str]] = None
    sources: Optional[list[str]] = None

# Define CrewAI tool
@tool("Web Search Tool")
def web_search(query: str, max_results: int = 5) -> list[str]:
    """Search the web using DuckDuckGo.
    
    Args:
        query: Search query
        max_results: Number of results to return
    
    Returns:
        List of search results with title, snippet, and URL
    """
    from ddgs import DDGS
    results = DDGS().text(query, max_results=max_results)
    return [f"{r['title']}\n{r['body']}\n{r['href']}" for r in results]

@transducible(
    tools=[web_search],  # Pass CrewAI tool
    reasoning=True,
    max_iter=5
)
async def research_topic(state: ResearchQuery) -> ResearchReport:
    """Research a topic using web search and synthesize findings."""
    return Transduce(state)

# Use it
query = ResearchQuery(
    topic="Agentics framework",
    focus_area="practical applications"
)
report = await research_topic(query)
```

You can also use pre-built CrewAI tools:

```python
from crewai_tools import SerperDevTool, WebsiteSearchTool

# Use existing CrewAI tools
search_tool = SerperDevTool()
website_tool = WebsiteSearchTool()

@transducible(
    tools=[search_tool, website_tool],
    reasoning=True,
    max_iter=5
)
async def research_with_multiple_tools(state: ResearchQuery) -> ResearchReport:
    """Research using multiple CrewAI tools."""
    return Transduce(state)
```

#### Option 3: Launch MCP Server as Subprocess

You can launch the MCP server as a separate subprocess and connect to it. This approach starts the server automatically when needed:

```python
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# Launch MCP server as subprocess
# The server_params specify how to start the server process
server_params = StdioServerParameters(
    command="python",  # Command to run
    args=["mcp_server.py"],  # Server script to execute
)

# stdio_client launches the server and manages the connection
async with stdio_client(server_params) as (read, write):
    async with ClientSession(read, write) as session:
        # Initialize connection and discover available tools
        await session.initialize()
        tools = await session.list_tools()
        
        # Use tools in transduction
        @transducible(
            tools=tools,
            reasoning=True,
            max_iter=5
        )
        async def research_topic(state: ResearchQuery) -> ResearchReport:
            return Transduce(state)
        
        # Execute
        query = ResearchQuery(topic="Agentics framework")
        report = await research_topic(query)
        
# Server process is automatically terminated when exiting the context
```

**When to use each option:**
- **Option 1** (Remote MCP Server): Best for connecting to third-party or external MCP servers; use when you want to leverage existing MCP services or servers running elsewhere
- **Option 2** (CrewAI tools): Best for leveraging the CrewAI ecosystem and pre-built tools; simplest for getting started with existing tools
- **Option 3** (MCP subprocess with ClientSession): Lower-level approach for advanced use cases; gives more control over server lifecycle and communication; use for custom server management

---

## Tool Usage Patterns

### Pattern 1: Information Retrieval

Enrich data with external sources:

```python
@transducible(tools=[web_search, database_query])
async def enrich_data(state: BasicInfo) -> EnrichedInfo:
    """Enrich basic info with external data sources."""
    return Transduce(state)
```

### Pattern 2: Verification

Verify claims against external sources:

```python
@transducible(tools=[fact_checker, web_search])
async def verify_claims(state: Claims) -> VerifiedClaims:
    """Verify claims against external sources."""
    return Transduce(state)
```

### Pattern 3: Multi-Step Reasoning

Solve complex problems requiring multiple tools:

```python
@transducible(
    tools=[web_search, calculator, code_executor],
    reasoning=True,
    max_iter=10,
    verbose_agent=True  # See tool calls
)
async def solve_complex_problem(state: Problem) -> Solution:
    """Solve problems requiring multiple tools and reasoning steps."""
    return Transduce(state)
```

---

## Creating Custom Tools

Define your own tools following the MCP pattern:

```python
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("CustomTools")

@mcp.tool()
def calculate_metrics(data: dict) -> dict:
    """Calculate statistical metrics from data.
    
    Args:
        data: Dictionary with numeric values
    
    Returns:
        Dictionary with mean, median, std, etc.
    """
    import statistics
    values = list(data.values())
    return {
        "mean": statistics.mean(values),
        "median": statistics.median(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0
    }

@mcp.tool()
def fetch_database_record(record_id: str) -> dict:
    """Fetch a record from the database.
    
    Args:
        record_id: Unique identifier for the record
    
    Returns:
        Record data as dictionary
    """
    # Your database logic here
    return db.get(record_id)
```

---

## Tool Configuration

Control how tools are used in your transductions:

### Limiting Tool Usage

```python
# Limit tool usage
@transducible(
    tools=[expensive_api_tool],
    max_iter=3,  # Maximum 3 tool calls
    timeout=120  # 2 minute timeout including tool calls
)
async def controlled_tool_usage(state: Input) -> Output:
    return Transduce(state)
```

### Verbose Tool Logging

```python
# Verbose tool logging
@transducible(
    tools=[web_search],
    verbose_agent=True,  # Log each tool call
    provide_explanation=True  # Include tool usage in explanation
)
async def logged_tool_usage(state: Input) -> Output:
    return Transduce(state)

result, explanation = await logged_tool_usage(input_data)
print(f"Tools used: {explanation.tools_called}")
```

---

## Best Practices

1. **Provide clear tool descriptions** - Help the LLM understand when to use each tool
2. **Limit tool iterations** - Prevent infinite loops with `max_iter`
3. **Handle tool failures gracefully** - Tools may timeout or return errors
4. **Use reasoning mode** - Enable `reasoning=True` for complex tool orchestration
5. **Monitor tool usage** - Use `verbose_agent=True` during development
6. **Cache tool results** - Avoid redundant API calls for the same queries

### Example: Robust Tool Usage

```python
@transducible(
    tools=[web_search, database_query],
    reasoning=True,
    max_iter=5,
    timeout=300,
    verbose_agent=True,
    provide_explanation=True
)
async def robust_research(state: Query) -> Report:
    """Research with fallback strategies if tools fail."""
    return Transduce(state)

try:
    report, explanation = await robust_research(query)
    print(f"Used tools: {explanation.tools_called}")
    print(f"Tool call count: {len(explanation.tool_calls)}")
except TimeoutError:
    print("Research timed out - try simpler query or increase timeout")
```

---

## See Also

- 👉 [Transducible Functions](transducible_functions.md) - Core concepts and basic usage
- 👉 [Optimization](optimization.md) - Performance tuning and batch processing
- 👉 [Examples](../examples/mcp_server_example.py) - Complete MCP server example
- 👉 [Index](index.md)