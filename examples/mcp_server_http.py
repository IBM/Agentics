"""HTTP MCP Server for Web Search

This server exposes Duck Duck Go search APIs as MCP tools via HTTP/SSE transport.
Suitable for production deployments where the MCP server runs as a separate service.

Usage:
    python examples/mcp_server_http.py

The server will start on http://localhost:8000 with SSE endpoint at /sse
"""

from ddgs import DDGS
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Search")


@mcp.tool()
def web_search(query: str, max_results: int = 10) -> list[str]:
    """Return snippets of text extracted from Duck Duck Go search for the given query.

    Args:
        query: Search query using DDGS search operators
        max_results: Number of snippets to return (default: 10, usually 5-20)

    DDGS Search Operators:
    - cats dogs: Results about cats or dogs
    - "cats and dogs": Exact phrase match
    - cats -dogs: Fewer dogs in results
    - cats +dogs: More dogs in results
    - cats filetype:pdf: PDFs about cats (supports: pdf, doc(x), xls(x), ppt(x), html)
    - dogs site:example.com: Pages from specific site
    - cats -site:example.com: Exclude specific site
    - intitle:dogs: Page title includes word
    - inurl:cats: Page URL includes word

    Returns:
        List of formatted search results with title, body, and URL
    """
    try:
        search_results = DDGS().text(query, max_results=max_results)
        return [f'{x["title"]}\n{x["body"]}\n{x["href"]}' for x in search_results]
    except Exception as e:
        return [f"Search error: {str(e)}"]


if __name__ == "__main__":
    # Run with SSE transport on HTTP
    print("=" * 80)
    print("Starting MCP HTTP Server")
    print("=" * 80)
    print("\nServer Configuration:")
    print("  - Transport: SSE (Server-Sent Events)")
    print("  - Default endpoint: http://localhost:8000/sse")
    print("\nAvailable Tools:")
    print("  - web_search: Duck Duck Go web search")
    print("\nPress Ctrl+C to stop the server")
    print("=" * 80 + "\n")

    # FastMCP handles host/port via environment or defaults
    # Set MCP_SERVER_HOST and MCP_SERVER_PORT env vars to customize
    mcp.run(transport="sse")

# Made with Bob
