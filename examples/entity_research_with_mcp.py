"""
Example: Entity Research with MCP Web Search Tool

This example demonstrates how to use MCP tools with agentics to:
1. Take a list of entities (companies, people, topics, etc.)
2. Use web search to gather information about each entity
3. Generate a comprehensive summary report for each

SETUP:
1. Ensure the MCP server is available at the path specified in .env
2. Set OPENAI_API_KEY or ANTHROPIC_API_KEY in .env
3. Run: python examples/entity_research_with_mcp.py
"""

import asyncio
import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from agentics import AG
from agentics.core.mcp_tools import get_mcp_tools

load_dotenv()


# Input Model: Entity to research
class Entity(BaseModel):
    """An entity to research (company, person, topic, etc.)"""

    name: str = Field(..., description="Name of the entity to research")
    category: Optional[str] = Field(
        None, description="Category (e.g., 'company', 'person', 'technology')"
    )


# Output Model: Research Report
class EntityReport(BaseModel):
    """Comprehensive research report about an entity"""

    entity_name: str = Field(..., description="Name of the entity researched")
    category: Optional[str] = Field(None, description="Category of the entity")

    summary: str = Field(
        ..., description="2-3 sentence executive summary of the entity"
    )

    key_facts: list[str] = Field(
        default_factory=list, description="List of 3-5 key facts about the entity"
    )

    recent_developments: Optional[str] = Field(
        None, description="Recent news or developments (if any)"
    )

    relevance: Optional[str] = Field(
        None, description="Why this entity is significant or relevant"
    )

    sources: list[str] = Field(
        default_factory=list, description="URLs of sources used for research"
    )


async def main():
    """Main function demonstrating entity research with MCP tools"""

    # Setup MCP server connection - try multiple possible locations
    possible_paths = [
        os.getenv("MCP_SERVER_PATH"),  # User-specified local path
        os.getenv("MCP_SERVER_URL"),  # User-specified remote URL
        os.path.join(os.path.dirname(__file__), "mcp_server_example.py"),  # Same dir
        "mcp/DDG_search_tool_mcp.py",  # Alternative location
        "examples/mcp_server_example.py",  # From project root
    ]

    mcp_connection = None
    for path in possible_paths:
        if path and (path.startswith("http") or os.path.exists(path)):
            mcp_connection = path
            break

    if not mcp_connection:
        print(f"❌ Error: MCP server not found in any of these locations:")
        for path in possible_paths:
            if path:
                print(f"   - {path}")
        print(f"\nPlease:")
        print(f"1. Set MCP_SERVER_PATH (local) or MCP_SERVER_URL (remote) in .env")
        print(f"2. Or ensure mcp_server_example.py exists in examples/")
        return

    print("=" * 80)
    print("Entity Research with MCP Web Search Tool")
    print("=" * 80)
    print(f"\n📁 Using MCP connection: {mcp_connection}")

    # Define entities to research
    entities = [
        Entity(name="Anthropic", category="company"),
        Entity(name="Claude AI", category="technology"),
        Entity(name="Model Context Protocol", category="technology"),
    ]

    print(f"\n📋 Researching {len(entities)} entities:")
    for i, entity in enumerate(entities, 1):
        print(f"  {i}. {entity.name} ({entity.category})")

    # Get MCP tools using the new module (handles both local and remote)
    try:
        mcp_tools = get_mcp_tools(mcp_connection, verbose=True)
        # MCPServerAdapter doesn't support len() or iteration, just use it directly
        print(f"\n🔧 Connected to MCP server successfully")
    except Exception as e:
        print(f"\n❌ Error connecting to MCP server: {e}")
        return

    print("\n🔍 Starting research...\n")

    # Process all entities using AG with MCP tools
    reports = await (
        AG(
            atype=EntityReport,
            tools=mcp_tools,
            max_iter=15,  # Allow multiple tool calls for thorough research
            reasoning=True,  # Enable planning for better research strategy
            verbose_agent=False,
            description="""Research the given entity using web search tools.

Steps:
1. Use web_search to find information about the entity
2. Search for recent news and developments
3. Identify key facts and significance
4. Compile a comprehensive report with sources

Guidelines:
- Use 2-3 search queries to gather comprehensive information
- Focus on factual, verifiable information
- Include recent developments (within last year if available)
- Cite sources by including URLs
- Be concise but thorough
""",
        )
        << entities
    )

    # Display results
    print("\n" + "=" * 80)
    print("RESEARCH REPORTS")
    print("=" * 80)

    for i, report in enumerate(reports, 1):
        print(f"\n{'─' * 80}")
        print(f"Report #{i}: {report.entity_name}")
        print(f"{'─' * 80}")

        if report.category:
            print(f"\n📂 Category: {report.category}")

        print(f"\n📝 Summary:")
        print(f"   {report.summary}")

        if report.key_facts:
            print(f"\n✨ Key Facts:")
            for j, fact in enumerate(report.key_facts, 1):
                print(f"   {j}. {fact}")

        if report.recent_developments:
            print(f"\n📰 Recent Developments:")
            print(f"   {report.recent_developments}")

        if report.relevance:
            print(f"\n🎯 Relevance:")
            print(f"   {report.relevance}")

        if report.sources:
            print(f"\n🔗 Sources:")
            for j, source in enumerate(report.sources, 1):
                print(f"   {j}. {source}")

    print("\n" + "=" * 80)
    print("✅ Research complete!")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

# Made with Bob
