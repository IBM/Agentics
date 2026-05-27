"""
Simple test script to verify transduction is working correctly.
This helps diagnose issues with LLM configuration.
"""

import asyncio
import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from agentics import AG
from agentics.core.transducible_functions import make_transducible_function

load_dotenv()

print(os.getenv("WATSONX_APIKEY"))


class SimpleInput(BaseModel):
    text: Optional[str] = None


class SimpleOutput(BaseModel):
    result: Optional[str] = Field(None, description="The processed result")


async def test_basic_transduction():
    """Test basic transduction to verify LLM is working"""
    print("=" * 70)
    print("Testing Basic Transduction")
    print("=" * 70)

    # Check which LLM is configured
    # Create a simple transducible function
    print("\n" + "-" * 70)
    print("Creating transducible function...")

    try:
        simple_transform = make_transducible_function(
            InputModel=SimpleInput,
            OutputModel=SimpleOutput,
            instructions="Simply echo back the input text in the result field. Just copy it verbatim.",
            name="simple_echo",
        )
        print("✓ Transducible function created successfully")
    except Exception as e:
        print(f"✗ Error creating transducible function: {e}")
        return False

    # Test with a simple input
    print("\n" + "-" * 70)
    print("Testing transduction...")

    test_input = SimpleInput(text="Hello, World!")
    print(f"Input: {test_input.text}")

    try:
        result = await simple_transform(test_input)
        print(f"\n✓ Transduction completed!")
        print(f"Result type: {type(result)}")
        print(f"Result: {result.model_dump_json(indent=2)}")

        # Check if result is valid
        if result.result and result.result != "None":
            print("\n✅ SUCCESS: Transduction is working correctly!")
            return True
        else:
            print("\n⚠️  WARNING: Transduction returned empty or None result")
            print("This suggests the LLM call may be failing silently.")
            return False

    except Exception as e:
        print(f"\n✗ Error during transduction: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_with_verbose():
    """Test with verbose mode to see what's happening"""
    print("\n\n" + "=" * 70)
    print("Testing with Verbose Mode")
    print("=" * 70)

    class Input(BaseModel):
        content: Optional[str] = None

    class Output(BaseModel):
        summary: Optional[str] = None

    try:
        verbose_transform = make_transducible_function(
            InputModel=Input,
            OutputModel=Output,
            instructions="Create a one-sentence summary of the input",
            verbose_transduction=True,  # Enable verbose output
            verbose_agent=True,
        )

        test_input = Input(content="The quick brown fox jumps over the lazy dog")
        print(f"\nInput: {test_input.content}")
        print("\nExecuting with verbose output...\n")

        result = await verbose_transform(test_input)
        print(f"\nResult: {result.model_dump_json(indent=2)}")

        return result.summary is not None

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


async def main():
    """Run all tests"""
    print("\n🔍 Agentics Transduction Diagnostic Tool\n")

    # Test 1: Basic transduction
    test1_passed = await test_basic_transduction()

    # Test 2: Verbose mode (only if test 1 failed)
    if not test1_passed:
        print("\n\nRunning verbose test to diagnose the issue...")
        test2_passed = await test_with_verbose()
    else:
        test2_passed = True

    # Summary
    print("\n\n" + "=" * 70)
    print("DIAGNOSTIC SUMMARY")
    print("=" * 70)

    if test1_passed and test2_passed:
        print("✅ All tests passed! Transduction is working correctly.")
        print("\nYou can now run the full examples:")
        print("  python examples/transduction_with_code.py")
    else:
        print("❌ Tests failed. Please check:")
        print("\n1. Your .env file has a valid API key configured")
        print("2. The LLM provider is accessible")
        print("3. Check the error messages above for specific issues")
        print("\nFor WatsonX users:")
        print("  - Verify WATSONX_APIKEY, WATSONX_PROJECTID, WATSONX_URL")
        print("  - Ensure MODEL_ID is set correctly")
        print("\nFor OpenAI users:")
        print("  - Verify OPENAI_API_KEY is valid")
        print(
            "  - Check if OPENAI_BASE_URL is set correctly (if using custom endpoint)"
        )


if __name__ == "__main__":
    asyncio.run(main())

# Made with Bob
