"""
Example: Multiply integers by 2 using Agentics (AG) with parallel processing

This script demonstrates how to use AG (Agentics) tables with transducible functions
to process a list of integers in parallel, multiplying each by 2.

SETUP REQUIRED:
Before running this script, ensure you have set up your LLM provider:
1. Create a .env file in the project root with your API key:
   OPENAI_API_KEY=your_key_here
   OR
   ANTHROPIC_API_KEY=your_key_here
2. The script will use the default LLM provider from your environment
"""

import asyncio
from typing import Optional

from pydantic import BaseModel, Field

from agentics import AG
from agentics.core.transducible_functions import make_transducible_function


async def example_1_basic_multiplication():
    """Example 1: Basic multiplication using AG with transducible functions"""
    print("\n🔢 Example 1: Multiply integers by 2 using AG table")
    print("-" * 70)

    # Define input model - each row contains one number
    class NumberInput(BaseModel):
        value: Optional[int] = Field(None, description="A single integer to multiply")

    # Define output model - result of multiplication
    class NumberOutput(BaseModel):
        original: Optional[int] = Field(None, description="Original value")
        multiplied: Optional[int] = Field(None, description="Value multiplied by 2")

    # Create the transducible function with custom code
    function_code = """
from typing import Optional
from pydantic import BaseModel, Field

class NumberInput(BaseModel):
    value: Optional[int] = Field(None, description="A single integer to multiply")

class NumberOutput(BaseModel):
    original: Optional[int] = Field(None, description="Original value")
    multiplied: Optional[int] = Field(None, description="Value multiplied by 2")

async def multiply_by_two(state: NumberInput) -> NumberOutput:
    '''Multiply the input value by 2'''
    if state.value is not None:
        return NumberOutput(
            original=state.value,
            multiplied=state.value * 2
        )
    return NumberOutput(original=None, multiplied=None)
"""

    multiplier = make_transducible_function(
        function_code=function_code,
        instructions="Multiply the input value by 2",
    )

    # Get the models
    NumberInput = multiplier.input_model
    NumberOutput = multiplier.target_model

    # Create AG table with one number per row
    input_numbers = [1, 2, 3, 4, 5, 10, 15, 20]

    # Create AG from list of states (each state is one number)
    numbers_ag = AG.from_states([NumberInput(value=num) for num in input_numbers])

    print(f"\nInput AG table ({len(numbers_ag)} rows):")
    for i, state in enumerate(numbers_ag.states, 1):
        print(f"  Row {i}: value={state.value}")

    # Process all numbers in parallel using the transducible function
    # Pass the list of states to process them all at once
    results = await multiplier(numbers_ag.states)

    print(f"\nResults (processed in parallel):")
    for i, result in enumerate(results, 1):
        print(f"  Row {i}: {result.original} × 2 = {result.multiplied}")

    return results


async def example_2_using_ag_transduction():
    """Example 2: Using AG's built-in transduction with custom function"""
    print("\n\n🔧 Example 2: Using AG transduction with custom code")
    print("-" * 70)

    # Define models
    class Number(BaseModel):
        value: Optional[int] = None

    class DoubledNumber(BaseModel):
        original: Optional[int] = None
        doubled: Optional[int] = None

    # Create function code
    function_code = """
from typing import Optional
from pydantic import BaseModel

class Number(BaseModel):
    value: Optional[int] = None

class DoubledNumber(BaseModel):
    original: Optional[int] = None
    doubled: Optional[int] = None

async def double_number(state: Number) -> DoubledNumber:
    '''Double the input number'''
    if state.value is not None:
        return DoubledNumber(original=state.value, doubled=state.value * 2)
    return DoubledNumber(original=None, doubled=None)
"""

    doubler = make_transducible_function(
        function_code=function_code,
        instructions="Double the input number",
    )

    # Get models
    Number = doubler.input_model
    DoubledNumber = doubler.target_model

    # Create AG table
    numbers = [3, 6, 9, 12, 15, 18]
    numbers_ag = AG.from_states([Number(value=n) for n in numbers])

    print(f"\nInput: {[n.value for n in numbers_ag.states]}")

    # Process in parallel
    results = await doubler(numbers_ag.states)

    print(f"\nResults:")
    for result in results:
        print(f"  {result.original} → {result.doubled}")

    return results


async def example_3_large_batch():
    """Example 3: Process a large batch of numbers in parallel"""
    print("\n\n📊 Example 3: Large batch processing (100 numbers)")
    print("-" * 70)

    class SingleNumber(BaseModel):
        n: Optional[int] = None

    class Result(BaseModel):
        input: Optional[int] = None
        output: Optional[int] = None

    function_code = """
from typing import Optional
from pydantic import BaseModel

class SingleNumber(BaseModel):
    n: Optional[int] = None

class Result(BaseModel):
    input: Optional[int] = None
    output: Optional[int] = None

async def process(state: SingleNumber) -> Result:
    '''Multiply by 2'''
    if state.n is not None:
        return Result(input=state.n, output=state.n * 2)
    return Result(input=None, output=None)
"""

    processor = make_transducible_function(
        function_code=function_code,
        instructions="Multiply by 2",
    )

    SingleNumber = processor.input_model
    Result = processor.target_model

    # Create 100 numbers
    numbers = list(range(1, 101))
    numbers_ag = AG.from_states([SingleNumber(n=num) for num in numbers])

    print(f"\nProcessing {len(numbers_ag)} numbers in parallel...")

    # Process all at once
    results = await processor(numbers_ag.states)

    # Show first 10 and last 10 results
    print(f"\nFirst 10 results:")
    for i in range(10):
        print(f"  {results[i].input} × 2 = {results[i].output}")

    print(f"\n... (80 more results) ...\n")

    print(f"Last 10 results:")
    for i in range(-10, 0):
        print(f"  {results[i].input} × 2 = {results[i].output}")

    # Verify all results
    all_correct = all(r.output == r.input * 2 for r in results)
    print(f"\n✅ All {len(results)} results are correct: {all_correct}")

    return results


async def example_4_with_statistics():
    """Example 4: Process numbers and calculate statistics"""
    print("\n\n📈 Example 4: Process with statistics")
    print("-" * 70)

    class InputNumber(BaseModel):
        value: Optional[int] = None

    class OutputWithStats(BaseModel):
        original: Optional[int] = None
        doubled: Optional[int] = None
        is_even: Optional[bool] = None
        is_positive: Optional[bool] = None

    function_code = """
from typing import Optional
from pydantic import BaseModel

class InputNumber(BaseModel):
    value: Optional[int] = None

class OutputWithStats(BaseModel):
    original: Optional[int] = None
    doubled: Optional[int] = None
    is_even: Optional[bool] = None
    is_positive: Optional[bool] = None

async def process_with_stats(state: InputNumber) -> OutputWithStats:
    '''Multiply by 2 and add statistics'''
    if state.value is not None:
        return OutputWithStats(
            original=state.value,
            doubled=state.value * 2,
            is_even=state.value % 2 == 0,
            is_positive=state.value > 0
        )
    return OutputWithStats()
"""

    processor = make_transducible_function(
        function_code=function_code,
        instructions="Multiply by 2 and calculate statistics",
    )

    InputNumber = processor.input_model
    OutputWithStats = processor.target_model

    # Create AG with mixed positive and negative numbers
    numbers = [-5, -2, 0, 3, 7, 10, 15, 20]
    numbers_ag = AG.from_states([InputNumber(value=n) for n in numbers])

    print(f"\nInput numbers: {numbers}")

    # Process in parallel
    results = await processor(numbers_ag.states)

    print(f"\nResults with statistics:")
    for result in results:
        print(
            f"  {result.original:3d} × 2 = {result.doubled:3d} | "
            f"Even: {result.is_even} | Positive: {result.is_positive}"
        )

    # Calculate summary statistics
    even_count = sum(1 for r in results if r.is_even)
    positive_count = sum(1 for r in results if r.is_positive)

    print(f"\nSummary:")
    print(f"  Total numbers: {len(results)}")
    print(f"  Even numbers: {even_count}")
    print(f"  Positive numbers: {positive_count}")

    return results


async def main():
    """Run all examples"""
    print("=" * 70)
    print("Multiply by 2 using Agentics (AG) - Parallel Processing")
    print("=" * 70)

    try:
        # Run each example
        await example_1_basic_multiplication()
        await example_2_using_ag_transduction()
        await example_3_large_batch()
        await example_4_with_statistics()

        print("\n" + "=" * 70)
        print("✅ All examples completed successfully!")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure you have:")
        print("1. Set up your .env file with OPENAI_API_KEY or ANTHROPIC_API_KEY")
        print("2. Installed all required dependencies: pip install -e .")
        raise


if __name__ == "__main__":
    asyncio.run(main())

# Made with Bob
