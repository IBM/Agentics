"""
AGsearch - Aggregate UDF for Vector Search

Performs vector search on accumulated table rows using AG's built-in vector store,
returning the top-k most relevant results based on a user query. This is the
streaming SQL equivalent of the AG.search() method.

Usage:
    -- Search reviews for mentions of "great service"
    SELECT agsearch('great service', 10) as relevant_reviews
    FROM pr;

    -- Search with custom max results
    SELECT agsearch('bug report', 5) as bug_reports
    FROM issues;

Note: This uses AG's vector store with sentence transformers for embeddings.
The first call will download the embedding model, which may take time.
"""

import json
import os
import sys
import threading
from typing import List

from dotenv import load_dotenv
from pydantic import BaseModel, Field
from pyflink.table import DataTypes
from pyflink.table.udf import AggregateFunction, TableFunction, udaf, udtf

from agentics import AG

# Load environment variables
for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✓ Loaded .env from {env_path}", file=sys.stderr)
        break


#######################
### Helper Functions ###
#######################


# Define TextItem model at module level
class TextItem(BaseModel):
    text: str = Field(description="The text content")
    index: int = Field(default=0, description="Original row index")


# Thread-local storage for AG instances
_thread_local = threading.local()


def get_ag_instance():
    """Get or create AG instance using thread-local storage."""
    if not hasattr(_thread_local, "ag_instance"):
        # Create AG instance
        _thread_local.ag_instance = AG(atype=TextItem)
        print("DEBUG agsearch: Created new AG instance", file=sys.stderr)

    return _thread_local.ag_instance


#######################
### Accumulator Class ###
#######################


class SearchAccumulator:
    """Accumulator for collecting all rows before search."""

    def __init__(self):
        self.rows: List[str] = []
        self.query: str | None = None
        self.max_k: int = 10


#######################
### Aggregate Function ###
#######################


class AgSearchFunction(AggregateFunction):
    """
    Aggregate function that collects all rows and performs vector search.

    This is a UDAF (User-Defined Aggregate Function) that:
    1. Accumulates all input rows
    2. On get_value(), builds a vector index and performs search
    3. Returns the top-k most relevant results as JSON array
    """

    def create_accumulator(self):
        """Create a new accumulator."""
        return SearchAccumulator()

    def accumulate(
        self,
        acc: SearchAccumulator,
        input_data: str,
        query: str,
        max_k: int = 10,
    ):
        """
        Accumulate a row.

        Args:
            acc: The accumulator
            input_data: The input data from this row
            query: The search query
            max_k: Maximum number of results to return (default: 10)
        """
        if input_data:
            acc.rows.append(input_data)

        # Store parameters (they should be the same for all rows)
        if acc.query is None:
            acc.query = query
            acc.max_k = max_k if max_k and max_k > 0 else 10

    def get_value(self, acc: SearchAccumulator) -> str:
        """
        Compute the search results using AG's vector store.

        Returns:
            JSON string containing array of top-k results with scores
        """
        if not acc.rows or not acc.query:
            return json.dumps([])

        try:
            print(
                f"DEBUG agsearch: Searching {len(acc.rows)} rows for query: '{acc.query}'",
                file=sys.stderr,
            )

            # Get AG instance
            ag = get_ag_instance()

            # Clear previous states and rebuild
            ag.states = []

            # Add all rows to AG
            for i, row_data in enumerate(acc.rows):
                ag.append(TextItem(text=row_data, index=i))

            print(
                f"DEBUG agsearch: Built AG with {len(ag.states)} states",
                file=sys.stderr,
            )

            # Build vector index
            print("DEBUG agsearch: Building vector index...", file=sys.stderr)
            ag.build_index()
            print("DEBUG agsearch: Vector index built", file=sys.stderr)

            # Perform search
            print(
                f"DEBUG agsearch: Performing search with k={acc.max_k}",
                file=sys.stderr,
            )
            search_results = ag.search(acc.query, k=acc.max_k)

            print(
                f"DEBUG agsearch: Search returned {len(search_results.states)} results",
                file=sys.stderr,
            )

            # Format results
            results = []
            for state in search_results.states:
                if hasattr(state, "text") and hasattr(state, "index"):
                    # Type assertion for the linter
                    text_state: TextItem = state  # type: ignore
                    results.append(
                        {
                            "text": text_state.text,
                            "index": text_state.index,
                        }
                    )

            print(
                f"DEBUG agsearch: Formatted {len(results)} results",
                file=sys.stderr,
            )

            # Return as JSON array
            return json.dumps(results)

        except Exception as e:
            print(f"Error in agsearch.get_value: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            return json.dumps({"error": str(e)})

    def merge(self, acc1: SearchAccumulator, acc2: SearchAccumulator):
        """
        Merge two accumulators (for parallel processing).

        Args:
            acc1: First accumulator (will be modified)
            acc2: Second accumulator (will be merged into acc1)
        """
        acc1.rows.extend(acc2.rows)
        if acc1.query is None:
            acc1.query = acc2.query
            acc1.max_k = acc2.max_k

    def get_result_type(self):
        """Return the result type (STRING for JSON output)."""
        return DataTypes.STRING()

    def get_accumulator_type(self):
        """Return the accumulator type."""
        return DataTypes.ROW(
            [
                DataTypes.FIELD("rows", DataTypes.ARRAY(DataTypes.STRING())),
                DataTypes.FIELD("query", DataTypes.STRING()),
                DataTypes.FIELD("max_k", DataTypes.INT()),
            ]
        )


# Register the UDAF
agsearch = udaf(
    f=AgSearchFunction(),
    result_type=DataTypes.STRING(),
    accumulator_type=DataTypes.ROW(
        [
            DataTypes.FIELD("rows", DataTypes.ARRAY(DataTypes.STRING())),
            DataTypes.FIELD("query", DataTypes.STRING()),
            DataTypes.FIELD("max_k", DataTypes.INT()),
        ]
    ),
    name="agsearch",
)

#######################
### Table Function for Exploding Results ###
#######################


@udtf(result_types=[DataTypes.STRING(), DataTypes.INT()])
def explode_search_results(json_array_string):
    """
    Table function to explode agsearch JSON array results into multiple rows.

    Takes a JSON array string from agsearch and yields one row per search result.

    Args:
        json_array_string: JSON array string from agsearch, e.g.:
                          '[{"text": "...", "index": 0}, {"text": "...", "index": 1}]'

    Yields:
        Tuple of (text, index) for each search result

    Usage:
        SELECT text, idx
        FROM (
            SELECT agsearch(customer_review, 'great service', 10) as results FROM pr
        ),
        LATERAL TABLE(explode_search_results(results)) AS T(text, idx);

    Example:
        -- Get all search results as separate rows
        SELECT
            text,
            idx,
            LENGTH(text) as text_length
        FROM (
            SELECT agsearch(customer_review, 'excellent product', 5) as results
            FROM pr
        ),
        LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
        ORDER BY idx;
    """
    if not json_array_string:
        return

    try:
        # Parse the JSON array
        results = json.loads(json_array_string)

        # Handle error responses
        if isinstance(results, dict) and "error" in results:
            print(f"Error in search results: {results['error']}", file=sys.stderr)
            return

        # Yield each result as a row
        if isinstance(results, list):
            for result in results:
                if isinstance(result, dict) and "text" in result and "index" in result:
                    yield result["text"], result["index"]

    except json.JSONDecodeError as e:
        print(f"Error parsing JSON in explode_search_results: {e}", file=sys.stderr)
        print(f"Input was: {json_array_string[:200]}...", file=sys.stderr)
    except Exception as e:
        print(f"Error in explode_search_results: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)


# Made with Bob
