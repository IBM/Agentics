## This script exemplify the most basic use of Agentics as a pydantic transducer from
## list of strings.

import asyncio
import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel

from agentics import AG

load_dotenv()

## Define output type


class Answer(BaseModel):
    answer: Optional[str] = None
    confidence: Optional[float] = None


async def main():

    ## Collect input text

    input_questions = [
        "What is the capital of Italy?",
        "When is the end of the world expected",
    ]

    ## Transduce input strings into objects of type Answer.
    ## You can customize this providing different llms and instructions.

    answers = await (AG(atype=Answer) << input_questions)

    print(answers.pretty_print())


if __name__ == "__main__":
    if AG.get_llm_provider():
        asyncio.run(main())
    else:
        print("Please set API key in your .env file.")
