import os

from crewai import Agent, Crew, Process, Task
from crewai.tools import tool
from crewai_tools import MCPServerAdapter
from ddgs import DDGS

from agentics import AG
from mcp import StdioServerParameters  # For Stdio Server

## Connect to MCP servers.

server_params_search = StdioServerParameters(
    command="uvx",
    args=["mcp-server-time"],
    env={"UV_PYTHON": "3.12", **os.environ},
)
server_params_fetch = StdioServerParameters(
    command="uvx",
    args=["mcp-server-fetch"],
    env={"UV_PYTHON": "3.12", **os.environ},
)


## Define a Crew AI tool to get news for a given date using the DDGS search engine
@tool("web_search")
def web_search(query: str) -> str:
    """return spippets of text extracted from duck duck go search for the given
        query :  using DDGS search operators
        max_results: number of snippets to be returned, usually 5 - 20
    DDGS search operators Guidelines in the table below:
    Query example	Result
    cats dogs	Results about cats or dogs
    "cats and dogs"	Results for exact term "cats and dogs". If no results are found, related results are shown.
    cats -dogs	Fewer dogs in results
    cats +dogs	More dogs in results
    cats filetype:pdf	PDFs about cats. Supported file types: pdf, doc(x), xls(x), ppt(x), html
    dogs site:example.com	Pages about dogs from example.com
    cats -site:example.com	Pages about cats, excluding example.com
    intitle:dogs	Page title includes the word "dogs"
    inurl:cats	Page url includes the word "cats"""
    return str(DDGS().text(query, max_results=10))


# Connect to MCP servers.
with (
    MCPServerAdapter(server_params_search) as server_search,
    MCPServerAdapter(server_params_fetch) as server_fetch,
):
    print(
        f"Available tools from Stdio MCP server: {[tool.name for tool in server_search]}"
    )
    print(
        f"Available tools from Stdio MCP server: {[tool.name for tool in server_fetch]}"
    )

    # Create a conversational agent with a friendly role/goal
    chat_agent = Agent(
        role="Helpful Assistant",
        goal="Have a natural multi-turn conversation",
        backstory="You are a friendly assistant that remembers context and asks for clarification when needed.",
        memory=True,  # enable memory for conversational context
        # reasoning=True,
        llm=AG.get_llm_provider(),  ## OpenAI is reccomended for agents using multiple tools
    )

    # Define a simple Task that represents a single AI response
    task = Task(
        ## Nothe that the desciption is a chat template
        description="Respond appropriately to user's message, maintaining context. User> {input}",
        expected_output="A conversational reply",
        agent=chat_agent,
        tools=[web_search] + server_search + server_fetch,
    )

    crew = Crew(
        agents=[chat_agent],
        tasks=[task],
        process=Process.sequential,
        verbose=True,
        memory=True,  # ensure conversation state persists across turns
    )

    ## Try out this example to illustrate math, search and reasining capabilities:
    ## What was the time in Rome when the first plane crashed the twin towers?
    print(f"Agent started with the following tools : {[x.name for x in task.tools]}")
    # Example conversation loop
    conversation = ""
    while user_input := input("User: ").strip():
        result = crew.kickoff(inputs={"input": conversation + user_input})
        print("AI:", result)
        conversation += f"User>{user_input}\nAI>{result}\n"
