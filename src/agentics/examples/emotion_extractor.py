from agentics.core.agentics import Agentics as AG
from pydantic import BaseModel, Field
from typing import Optional
from agentics.core.llm_connections import openai_llm, ollama_llm
import os

import asyncio


class Emotion(BaseModel):
    emotion_category: Optional[str] = Field(
        None,
        description="This is the type of the recognized emotion such as joy, sadnees, and fear. Return None if no emotion has been spotted.",
    )
    text_passage: Optional[str] = Field(
        None,
        description="This is the text passage where the above emotion has been mentioned. Return None if the above category is None",
    )
    confidence: Optional[float] = Field(
        0, description="This is confidence of your assessment on the above information"
    )


class EmotionDector(BaseModel):
    emotions_in_text: Optional[list[Emotion]]
    full_text: Optional[str] = Field(
        None, description="The original passage of text copied verbatim from the SOURCE"
    )


def split_into_chunks(text, chunk_size=200):
    return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]


async def main():
    emotion_detector = AG(atype=EmotionDector, 
                          llm = ollama_llm,
                          transduction_logs_path="/tmp/emotion_extractor.logs",
                          batch_size_transduction=1)
    text = None
    with open(os.path.join(os.getcwd(), "agentics/data/emotion_detection/The_Brothers_Karamazov.txt")) as f:
        text = f.read()
    emotion_detector.verbose_transduction = True
    emotions = await (emotion_detector << split_into_chunks(text)[:10])
    emotions.to_csv("/tmp/The_Brothers_Karamazov_emotions.json")
    emotions.pretty_print()


asyncio.run(main())
