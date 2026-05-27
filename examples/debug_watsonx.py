"""
Debug WatsonX transduction step by step.
This script adds verbose logging to see exactly what's happening.
"""

import asyncio
import logging
from typing import Optional

from pydantic import BaseModel, Field

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Import after logging setup
from agentics import AG
from agentics.core.transducible_functions import make_transducible_function


class Movie(BaseModel):
    movie_name: Optional[str] = None
    description: Optional[str] = None
    year: Optional[int] = None


class Genre(BaseModel):
    genre: Optional[str] = Field(None, description="Provide one category only")


async def test_step_by_step():
    print("=" * 80)
    print("STEP-BY-STEP WATSONX DEBUGGING")
    print("=" * 80)

    # Step 1: Check LLM provider
    print("\n[STEP 1] Checking LLM Provider...")
    from agentics.core.llm_connections import get_available_llms

    llms = get_available_llms()
    print(f"Available LLMs: {list(llms.keys())}")

    if "watsonx" in llms:
        watsonx_llm = llms["watsonx"]
        print(f"✓ WatsonX LLM found: {watsonx_llm}")
        print(f"  Type: {type(watsonx_llm)}")
        print(
            f"  Model: {watsonx_llm.model if hasattr(watsonx_llm, 'model') else 'N/A'}"
        )
    else:
        print("✗ WatsonX LLM not found!")
        return

    # Step 2: Create a simple transducible function
    print("\n[STEP 2] Creating transducible function...")
    try:
        classify_genre = make_transducible_function(
            InputModel=Movie,
            OutputModel=Genre,
            instructions="Classify the genre of the movie. Return only one genre category.",
            verbose_transduction=True,  # Enable verbose output
            verbose_agent=True,
        )
        print("✓ Function created successfully")
    except Exception as e:
        print(f"✗ Error creating function: {e}")
        import traceback

        traceback.print_exc()
        return

    # Step 3: Test with a single movie
    print("\n[STEP 3] Testing with single movie...")
    test_movie = Movie(
        movie_name="The Godfather",
        description="The aging patriarch of an organized crime dynasty transfers control of his clandestine empire to his reluctant son.",
        year=1972,
    )

    print(f"Input: {test_movie.model_dump_json(indent=2)}")

    try:
        print("\n[EXECUTING TRANSDUCTION - Watch for errors below]")
        print("-" * 80)
        result = await classify_genre(test_movie)
        print("-" * 80)
        print(f"\n✓ Result received: {result}")
        print(f"  Type: {type(result)}")
        print(f"  Content: {result.model_dump_json(indent=2)}")

        if result.genre:
            print(f"\n✅ SUCCESS! Genre: {result.genre}")
        else:
            print(f"\n⚠️  WARNING: Result has no genre (None)")

    except Exception as e:
        print(f"\n✗ Error during transduction: {e}")
        import traceback

        traceback.print_exc()

    # Step 4: Test the << operator (like hello_world.py)
    print("\n\n[STEP 4] Testing << operator (like hello_world.py)...")
    try:
        classify_genre_op = Genre << Movie
        print(f"✓ Created function via << operator: {classify_genre_op}")

        result_op = await classify_genre_op(test_movie)
        print(f"Result: {result_op}")
        print(f"Type: {type(result_op)}")

        if hasattr(result_op, "genre"):
            print(f"Genre: {result_op.genre}")
        else:
            print(f"Result content: {result_op}")

    except Exception as e:
        print(f"✗ Error with << operator: {e}")
        import traceback

        traceback.print_exc()


async def test_direct_llm_call():
    """Test calling the LLM directly to see if it works"""
    print("\n\n" + "=" * 80)
    print("DIRECT LLM TEST")
    print("=" * 80)

    from agentics.core.llm_connections import get_available_llms

    llms = get_available_llms()
    if "watsonx" not in llms:
        print("✗ WatsonX not available")
        return

    watsonx_llm = llms["watsonx"]
    print(f"Testing direct call to: {watsonx_llm}")

    try:
        # Try a simple call
        from crewai import Agent, Crew, Task

        agent = Agent(
            role="Genre Classifier",
            goal="Classify movie genres",
            backstory="You are an expert at classifying movies.",
            llm=watsonx_llm,
            verbose=True,
        )

        task = Task(
            description="What genre is The Godfather? Answer with just one word.",
            expected_output="A single genre word",
            agent=agent,
        )

        crew = Crew(agents=[agent], tasks=[task], verbose=True)

        print("\nExecuting direct LLM call via CrewAI...")
        result = crew.kickoff()
        print(f"\n✓ Direct LLM call succeeded!")
        print(f"Result: {result}")

    except Exception as e:
        print(f"\n✗ Direct LLM call failed: {e}")
        import traceback

        traceback.print_exc()


async def main():
    await test_step_by_step()
    await test_direct_llm_call()

    print("\n" + "=" * 80)
    print("DEBUGGING COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

# Made with Bob
