## This script exemplify the most basic use of Agentics as a pydantic transducer from
## list of strings.

import asyncio
import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel

from agentics import Agentics as AG
from agentics.core.llm_connections import available_llms, get_llm_provider

load_dotenv()

# Define output type


class Answer(BaseModel):
    answer: Optional[str] = None
    justification: Optional[str] = None
    confidence: Optional[float] = None


async def main():

    # Collect input text
    input_questions = [
        "What is the capital of Italy?",
        "This is my first time I work with Agentics",
        "What videogames inspire suicide?",
    ]

    """
    Transduce input strings into objects of type Answer.
    You can customize this providing different llms and instructions.
    """

    answers = await (
        AG(
            atype=Answer,
            llm=get_llm_provider(),  # Select your LLM from list of available options
            instructions="""Provide an Answer for the following input text 
                        only if it contains an appropriate question that do not contain
                        violent or adult language """,
        )
        << input_questions
    )

    print(answers.pretty_print())


if __name__ == "__main__":
    if len(available_llms) > 0:
        asyncio.run(main())
    else:
        print("Please set API key in your .env file.")
