# Entity Research with MCP Web Search Tool

This example demonstrates how to use MCP (Model Context Protocol) tools with agentics to perform automated research on a list of entities and generate comprehensive summary reports.

## Overview

The example shows how to:
1. Connect to an MCP server that provides web search capabilities
2. Create a transducible function that uses MCP tools
3. Process multiple entities in parallel (batch processing)
4. Generate structured research reports with citations

## Files

- [`entity_research_with_mcp.py`](entity_research_with_mcp.py) - Main example script
- [`mcp_server_example.py`](mcp_server_example.py) - MCP server providing web search tool

## Setup

### 1. Install Dependencies

```bash
pip install -e .
pip install crewai-tools mcp ddgs
```

### 2. Configure Environment

Create a `.env` file in the project root:

```bash
# LLM Provider (choose one)
OPENAI_API_KEY=your_openai_key_here
# OR
ANTHROPIC_API_KEY=your_anthropic_key_here

# MCP Server Path (optional, defaults to examples/mcp_server_example.py)
MCP_SERVER_PATH=examples/mcp_server_example.py
```

### 3. Run the Example

```bash
python examples/entity_research_with_mcp.py
```

## How It Works

### 1. Define Input/Output Models

```python
class Entity(BaseModel):
    """Entity to research"""
    name: str
    category: Optional[str]

class EntityReport(BaseModel):
    """Research report"""
    entity_name: str
    summary: str
    key_facts: list[str]
    recent_developments: Optional[str]
    sources: list[str]
```

### 2. Connect to MCP Server

```python
from crewai_tools import MCPServerAdapter
from mcp import StdioServerParameters

server_params = StdioServerParameters(
    command="python3",
    args=["examples/mcp_server_example.py"],
)

with MCPServerAdapter(server_params) as mcp_tools:
    # Tools are now available
    print([tool.name for tool in mcp_tools])
```

### 3. Create Transducible Function with MCP Tools

```python
from agentics.core.transducible_functions import make_transducible_function

research_entity = make_transducible_function(
    InputModel=Entity,
    OutputModel=EntityReport,
    instructions="""
    Research the entity using web search tools.
    Gather key facts, recent developments, and cite sources.
    """,
    tools=mcp_tools,        # MCP tools injected here
    max_iter=15,            # Allow multiple tool calls
    reasoning=True,         # Enable planning
    batch_size=3,           # Process in parallel
)
```

### 4. Process Entities

```python
entities = [
    Entity(name="Anthropic", category="company"),
    Entity(name="Claude AI", category="technology"),
    Entity(name="Model Context Protocol", category="technology"),
]

# Batch processing - all entities researched in parallel
reports = await research_entity(entities)
```

## Key Features

### MCP Tool Integration

The example uses [`make_transducible_function()`](../src/agentics/core/transducible_functions.py:536) to create a function that:
- Accepts MCP tools via the `tools` parameter
- Allows the LLM to call web search during transduction
- Gathers real-time information from the web

### Batch Processing

Multiple entities are processed in parallel:
- `batch_size=3` processes up to 3 entities concurrently
- Efficient for researching multiple entities
- Results maintain order of input

### Reasoning & Planning

With `reasoning=True`, the agent:
- Plans its research strategy
- Decides which search queries to use
- Iteratively gathers information
- `max_iter=15` allows up to 15 tool calls per entity

### Structured Output

The `EntityReport` model ensures:
- Consistent report format
- Type-safe results
- Easy to parse and process further

## Example Output

```
================================================================================
Entity Research with MCP Web Search Tool
================================================================================

📋 Researching 3 entities:
  1. Anthropic (company)
  2. Claude AI (technology)
  3. Model Context Protocol (technology)

🔧 Connected to MCP server with tools: ['web_search']

🔍 Starting research...

================================================================================
RESEARCH REPORTS
================================================================================

────────────────────────────────────────────────────────────────────────────────
Report #1: Anthropic
────────────────────────────────────────────────────────────────────────────────

📂 Category: company

📝 Summary:
   Anthropic is an AI safety company founded in 2021 that develops advanced
   AI systems with a focus on safety and reliability. They created Claude,
   a large language model designed to be helpful, harmless, and honest.

✨ Key Facts:
   1. Founded by former OpenAI researchers including Dario and Daniela Amodei
   2. Raised over $7 billion in funding from investors including Google and Amazon
   3. Focuses on Constitutional AI and AI safety research
   4. Claude models compete with GPT-4 and other leading LLMs

📰 Recent Developments:
   Released Claude 3 family of models in 2024, including Claude 3 Opus,
   the most capable model in their lineup.

🎯 Relevance:
   Leading AI safety research company shaping the future of responsible AI development

🔗 Sources:
   1. https://www.anthropic.com
   2. https://en.wikipedia.org/wiki/Anthropic
   3. https://techcrunch.com/tag/anthropic/
```

## Customization

### Research Different Entities

Modify the `entities` list:

```python
entities = [
    Entity(name="Python", category="programming language"),
    Entity(name="React", category="framework"),
    Entity(name="Docker", category="technology"),
]
```

### Adjust Research Depth

Control how thorough the research is:

```python
research_entity = make_transducible_function(
    # ... other params ...
    max_iter=20,        # More tool calls = deeper research
    reasoning=True,     # Better planning
    timeout=600,        # Longer timeout for complex research
)
```

### Custom Instructions

Tailor the research focus:

```python
instructions="""
Research the entity focusing on:
1. Technical specifications and capabilities
2. Market position and competitors
3. Future roadmap and developments
4. Community adoption and ecosystem

Provide detailed analysis with citations.
"""
```

## Advanced Usage

### With Custom MCP Tools

You can use any MCP server:

```python
# Use a different MCP server
server_params = StdioServerParameters(
    command="python3",
    args=["path/to/your/custom_mcp_server.py"],
)
```

### With Multiple Tool Types

Combine different tools:

```python
# MCP tools + other CrewAI tools
from crewai_tools import FileReadTool

all_tools = list(mcp_tools) + [FileReadTool()]

research_entity = make_transducible_function(
    # ...
    tools=all_tools,
)
```

### Sequential Processing

For entities that need to be processed in order:

```python
# Process one at a time
for entity in entities:
    report = await research_entity(entity)
    print(report.model_dump_json(indent=2))
```

## Related Examples

- [`agentics_web_search_report.py`](agentics_web_search_report.py) - Single query web search
- [`mcp_server_example.py`](mcp_server_example.py) - Creating MCP servers
- [`transduction_with_code.py`](transduction_with_code.py) - Transducible functions basics

## Troubleshooting

**Error: "Import crewai_tools could not be resolved"**
```bash
pip install crewai-tools
```

**Error: "MCP_SERVER_PATH not found"**
- Ensure the path in `.env` points to a valid MCP server
- Or use the default: `examples/mcp_server_example.py`

**Error: "No LLM provider available"**
- Set `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` in `.env`

**Timeout errors**
- Increase `timeout` parameter
- Reduce `max_iter` if research is too deep
- Check internet connection for web search

## Learn More

- [MCP Documentation](https://modelcontextprotocol.io/)
- [Agentics Transducible Functions](../docs/transducible_functions.md)
- [Tool Integration Guide](../docs/tool_integration.md)
