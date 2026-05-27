#!/usr/bin/env python3
"""Test AG LLM resolution"""

import asyncio

from crewai.llms.base_llm import BaseLLM
from pydantic import BaseModel

from agentics import AG


class Movie(BaseModel):
    movie_name: str


class Genre(BaseModel):
    genre: str


async def main():
    # Create AG instance
    movies = AG.from_states(
        [Movie(movie_name="The Matrix"), Movie(movie_name="Inception")]
    )

    print(f"AG instance created")
    print(f"AG.llm type: {type(movies.llm)}")
    print(f"AG.llm value: {movies.llm}")
    print(f"Is BaseLLM: {isinstance(movies.llm, BaseLLM)}")

    # Try to access llm through property
    print(f"\nAccessing through __dict__:")
    print(f"'llm' in __dict__: {'llm' in movies.__dict__}")
    if "llm" in movies.__dict__:
        print(f"__dict__['llm']: {movies.__dict__['llm']}")


if __name__ == "__main__":
    asyncio.run(main())

# Made with Bob
