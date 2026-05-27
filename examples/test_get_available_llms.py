#!/usr/bin/env python3
"""Test script to check get_available_llms"""

import os

from agentics.core.llm_connections import get_available_llms, get_llm_provider

print("Environment variables:")
print(
    f"  WATSONX_APIKEY: {os.getenv('WATSONX_APIKEY')[:10]}..."
    if os.getenv("WATSONX_APIKEY")
    else "  WATSONX_APIKEY: None"
)
print(f"  WATSONX_URL: {os.getenv('WATSONX_URL')}")
print(f"  WATSONX_PROJECTID: {os.getenv('WATSONX_PROJECTID')}")
print(f"  MODEL_ID: {os.getenv('MODEL_ID')}")
print(f"  SELECTED_LLM: {os.getenv('SELECTED_LLM')}")
print()

print("Calling get_available_llms()...")
try:
    llms = get_available_llms()
    print(f"Available LLMs: {list(llms.keys())}")
    print(f"Number of LLMs: {len(llms)}")

    for name, llm in llms.items():
        print(f"\n{name}:")
        print(f"  Type: {type(llm)}")
        print(f"  Has model attr: {hasattr(llm, 'model')}")
        if hasattr(llm, "model"):
            print(f"  Model: {llm.model}")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback

    traceback.print_exc()

print("\n" + "=" * 80)
print("Calling get_llm_provider()...")
try:
    llm = get_llm_provider()
    print(f"LLM: {llm}")
    print(f"Type: {type(llm)}")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback

    traceback.print_exc()

# Made with Bob
