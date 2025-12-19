import asyncio
from typing import Optional

from pydantic import BaseModel, Field

from agentics.core.transducible_functions import Transduce, transducible


class Movie(BaseModel):
    movie_name: Optional[str] = None
    description: Optional[str] = None
    year: Optional[int] = None


class Genre(BaseModel):
    genre: Optional[str] = Field(None, description="Provide one category only")


@transducible(provide_explanation=True)
async def classify_genre(state: Movie) -> Genre:
    """Classify the genre of the source Movie"""
    return Transduce(state)


genre, explanation = asyncio.run(
    classify_genre(
        Movie(
            movie_name="The Godfather",
            description="The aging patriarch of an organized crime dynasty transfers control of his clandestine empire to his reluctant son.",
            year=1972,
        )
    )
)

print(genre.model_dump_json(indent=2))
print(explanation.model_dump_json(indent=2))
