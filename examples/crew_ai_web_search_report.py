import os
from typing import Optional

import yaml
from crewai import Agent, Crew, Task
from crewai_tools import MCPServerAdapter
from pydantic import BaseModel, Field

from agentics import AG
from mcp import StdioServerParameters  # For Stdio Server


## Define a Pydantic type to structure the output of the crew
class Citation(BaseModel):
    url: Optional[str] = None
    authors: Optional[list[str]] = None
    title: Optional[str] = None
    relevant_text: Optional[str] = None


## Nested types can be used
class WebSearchReport(BaseModel):
    title: Optional[str] = None
    abstract: Optional[str] = None
    full_report: Optional[str] = Field(
        None,
        description="""Markdown reporting full report of the findings made from web search.""",
    )
    citations: list[Citation] = Field([], description="Citations to relevant sources")


## Connect to community MCP servers using uvx
fetch_params = StdioServerParameters(
    command="uvx",
    args=["mcp-server-fetch"],
    env={"UV_PYTHON": "3.12", **os.environ},
)
## Connect to your MCP server by providing the path
search_params = StdioServerParameters(
    command="python3",
    args=[os.getenv("MCP_SERVER_PATH")],
    env={"UV_PYTHON": "3.12", **os.environ},
)
with (
    MCPServerAdapter(fetch_params) as fetch_tools,
    MCPServerAdapter(search_params) as search_tools,
):
    print(
        f"Available tools from Stdio MCP server: {[tool.name for tool in fetch_tools]}"
    )
    print(
        f"Available tools from Stdio MCP server: {[tool.name for tool in search_tools]}"
    )
    tools = fetch_tools + search_tools

    doc_agent = Agent(
        role="Search Agent",
        goal="Find answers to questions from the user using the available MCP tool.",
        backstory="A helpful assistant for extensive web search reports.",
        tools=tools,
        reasoning=False,  ## when reasoning is true a plan is generated
        reasoning_steps=10,  ## maximum number of steps that will be executed in the plan
        memory=True,  ## Set true to provide context of conversation
        verbose=True,
        llm=AG.get_llm_provider(),  ## OpenAI is recommended for reasoning tasks. Try out your own model
    )

    doc_task = Task(
        description="""Your task is to perform an extensive web search about
        the following question {question} and return a document providing answers to 
        the questions that explore several interesting aspects, each of them supported 
        by pertinent information from web search.""",
        expected_output="""A structured document in which each section answer a specific aspect of the question.
        in a very detailed and accurate manner. Please include supporting passages to justify it""",
        agent=doc_agent,
        output_pydantic=WebSearchReport,  ## This will generate output in the specified type format
    )
    crew = Crew(
        agents=[doc_agent],
        tasks=[doc_task],
        verbose=True,
    )
    result = crew.kickoff(
        inputs={"question": """Make a literature report on Large Language Models"""}
    )

    (
        print(yaml.dump(result.pydantic.model_dump(), sort_keys=False))
        if result.pydantic
        else None
    )
