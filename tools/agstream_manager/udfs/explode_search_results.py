"""
Explode Search Results - Table UDF for AGsearch
Converts agsearch JSON array results into multiple rows
"""

import json
import sys

from pyflink.table import DataTypes
from pyflink.table.udf import udtf


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
