import asyncio
from typing import Optional

from pydantic import BaseModel, Field

import agentics.core.transducible_functions


class Movie(BaseModel):
    movie_name: Optional[str] = None
    description: Optional[str] = None
    year: Optional[int] = None


class Genre(BaseModel):
    genre: Optional[str] = Field(None, description="Provide one category only")


# Create movie instance
movies = [
    Movie(
        movie_name="The Godfather",
        description="The aging patriarch of an organized crime dynasty transfers control of his clandestine empire to his reluctant son.",
        year=1972,
    ),
    Movie(movie_name="The Shawshank Redemption"),
]
# Create transduction function using << operator
classify_genre = Genre << Movie

# Execute transduction
result = asyncio.run(classify_genre(movies))
print(result)
