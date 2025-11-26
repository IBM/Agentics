## This script exemplify the most basic use of Agentics as a pydantic transducer from
## list of strings.

import asyncio
import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel

from agentics import AG

load_dotenv()

# Define output type


class Answer(BaseModel):
    answer: Optional[str] = None
    confidence: Optional[float] = None


async def main():

    # Collect input text
    input_questions = ["What is the capital of Italy?", "Why agentics is so cool?"]
    answers = await (AG(atype=Answer) << input_questions)

    answers.pretty_print()


if __name__ == "__main__":
    if AG.get_llm_provider():
        asyncio.run(main())
    else:
        print("Please set API key in your .env file.")
