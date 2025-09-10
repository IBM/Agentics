"""required: export MCP_SERVER_PATH mcp/DDG_search_tool_mcp.py"""

import os

from crewai import Agent, Crew, Task
from crewai_tools import MCPServerAdapter

from agentics.core.llm_connections import get_llm_provider
from mcp import StdioServerParameters  # For Stdio Server

server_params = StdioServerParameters(
    command="python3",
    args=[os.getenv("MCP_SERVER_PATH")],
    env={"UV_PYTHON": "3.12", **os.environ},
)
# server_params = {"url": "https://docs.mcp.cloudflare.com/sse"}
# server_params = {"url": "https://localhost:3000/sse"}

with MCPServerAdapter(server_params) as server_tools:
    print(
        f"Available tools from Stdio MCP server: {[tool.name for tool in server_tools]}"
    )
    tools = server_tools

    doc_agent = Agent(
        role="Doc Searcher",
        goal="Find answers to questions from the user using the available MCP tool.",
        backstory="A helpful assistant for extensive web search reports.",
        tools=tools,
        reasoning=True,
        reasoning_steps=2,
        # memory=True,
        verbose=True,
        llm=get_llm_provider(),
    )

    doc_task = Task(
        description="""Your task is to perform an extensive web search about
        the following question {question} and return a document providing answers to 
        the questions that explore several interesting aspects, each of them supported 
        by pertinent information from web search.  """,
        expected_output="""A structured document in which each section answer a specific aspect of the question.
        in a very detailed and accurate manner. Please include supporting passages to justify it""",
        agent=doc_agent,
        markdown=True,
    )

    crew = Crew(
        agents=[doc_agent],
        tasks=[doc_task],
        verbose=True,
    )

    result = crew.kickoff(inputs={"question": input("Ask your question> ")})
    print("\nFinal Output:\n", result)
