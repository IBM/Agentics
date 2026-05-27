#!/usr/bin/env python3
"""Test script to check LLM type"""

from crewai import LLM
from crewai.llms.base_llm import BaseLLM

from agentics.core.llm_connections import get_llm_provider

# Get the LLM
llm = get_llm_provider()

print(f"LLM object: {llm}")
print(f"LLM type: {type(llm)}")
print(f"LLM class name: {llm.__class__.__name__}")
print(f"Is instance of BaseLLM: {isinstance(llm, BaseLLM)}")
print(f"Is instance of LLM: {isinstance(llm, LLM)}")
print(f"LLM MRO: {type(llm).__mro__}")

# Check if it has the expected attributes
print(f"\nHas 'model' attribute: {hasattr(llm, 'model')}")
if hasattr(llm, "model"):
    print(f"Model: {llm.model}")

# Made with Bob
