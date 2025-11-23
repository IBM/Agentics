# The following program exemplify the use of self transduction from an input csv file
# of movies, to generate tweets asynchronously for each of them

import asyncio
import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel

from agentics import AG

load_dotenv()


async def main():
    ## Step 1. Load the Dataset
    movies = AG.from_csv(
        "data/categorization_example/movies.csv",
        max_rows=10,
    )
    movies.llm = AG.get_llm_provider()
    ## Step 2. add attribute tweet to be used as a target for transduction
    extended_movies = movies.add_attribute(
        "tweet", slot_type="str", description="Tweet used to advertise the movie"
    )

    # Step 3. Transduce input data into the new tweet field
    categorized_movies = await extended_movies.self_transduction(
        source_fields=["description", "movie_name", "genre"],
        target_fields=["tweet"],
        instructions="Generate a tweet to advertise the release of the input movie",
    )

    categorized_movies.pretty_print()


asyncio.run(main())
