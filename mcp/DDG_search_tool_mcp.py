"""A simple search MCP server exposes Duck Duck GO search apis as tools"""

from ddgs import DDGS

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Search")


@mcp.tool()
def web_search(query: str, max_results: int) -> list[str]:
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
    inurl:cats	Page url includes the word "cats"
    """
    search_results = DDGS().text(query, max_results=max_results)
    return [f'{x["title"]}\n{x["body"]}\n{x["href"]}' for x in search_results]


if __name__ == "__main__":
    mcp.run(transport="stdio")
