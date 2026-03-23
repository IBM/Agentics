#!/usr/bin/env python3
"""
Add test questions to the Q topic for joke generation testing
"""

import os
import sys

# Add parent directory to path to import agentics
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from pydantic import BaseModel

from agentics.core.streaming import AGStream


class Question(BaseModel):
    text: str


# Create AGStream instance for Q topic
ag = AGStream(atype=Question, input_topic="Q")

# Add test questions
test_questions = [
    Question(text="Why did the chicken cross the road?"),
    Question(text="What is the meaning of life?"),
    Question(text="How does a computer work?"),
    Question(text="Why do programmers prefer dark mode?"),
    Question(text="What's the best programming language?"),
    Question(text="Why did the developer go broke?"),
    Question(text="How do you comfort a JavaScript bug?"),
    Question(text="Why do Java developers wear glasses?"),
]

print("Adding test questions to Q topic...")
ag.states = test_questions
msg_ids = ag.produce()

print(f"\n✓ Added {len(msg_ids)} questions to Q topic")
print(
    f"Message IDs: {msg_ids[:3]}..." if len(msg_ids) > 3 else f"Message IDs: {msg_ids}"
)
print("\nNow you can generate jokes in Flink SQL with:")
print("  SELECT agmap('Joke', text) FROM Q LIMIT 3;")

# Made with Bob
