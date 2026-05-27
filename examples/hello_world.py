import asyncio
from typing import Optional

from pydantic import BaseModel, Field

from agentics import AG


class Movie(BaseModel):
    movie_name: Optional[str] = None
    description: Optional[str] = None
    year: Optional[int] = None


class Genre(BaseModel):
    genre: Optional[str] = Field(None, description="Provide one category only")


async def main():
    # Create movie instances using AG
    movies = AG.from_states(
        [
            Movie(
                movie_name="The Godfather",
                description="The aging patriarch of an organized crime dynasty transfers control of his clandestine empire to his reluctant son.",
                year=1972,
            ),
            Movie(
                movie_name="The Shawshank Redemption",
                description="Two imprisoned men bond over a number of years, finding solace and eventual redemption through acts of common decency.",
                year=1994,
            ),
        ]
    )

    # Add genre attribute
    extended_movies = movies.add_attribute(
        "genre",
        slot_type="str",
        description="Movie genre (Drama, Crime, Action, Comedy, etc.)",
    )

    # Perform self-transduction to fill in the genre
    result = await extended_movies.self_transduction(
        source_fields=["movie_name", "description"],
        target_fields=["genre"],
        instructions="Classify the genre of the movie. Choose ONE genre from: Drama, Crime, Action, Comedy, Thriller, Romance, Sci-Fi, Horror.",
    )

    # Print results
    print("\nResults:")
    for i, movie in enumerate(result.states, 1):
        print(f"{i}. {movie.movie_name}: {movie.genre}")

    return result


if __name__ == "__main__":
    asyncio.run(main())

# Made with Bob
