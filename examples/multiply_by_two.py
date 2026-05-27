"""
Example: Multiply integers by 2 using transducible functions

This script demonstrates how to use make_transducible_function() with custom
Python code to process a list of integers and multiply each by 2.

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

from agentics.core.transducible_functions import make_transducible_function


async def example_1_multiply_with_code():
    """Example 1: Multiply integers by 2 using function_code"""
    print("\n🔢 Example 1: Multiply integers by 2 using function_code")
    print("-" * 70)

    # Define the complete function code with custom logic
    function_code = """
from typing import Optional
from pydantic import BaseModel, Field
from agentics.core.transducible_functions import Transduce

class IntegerList(BaseModel):
    numbers: Optional[list[int]] = Field(None, description="List of integers to process")

class MultipliedList(BaseModel):
    original: Optional[list[int]] = Field(None, description="Original list of integers")
    multiplied: Optional[list[int]] = Field(None, description="List of integers multiplied by 2")
    count: Optional[int] = Field(None, description="Number of integers processed")

async def multiply_by_two(state: IntegerList) -> MultipliedList:
    '''Multiply each integer in the list by 2'''
    # Custom processing: multiply each number by 2
    if state.numbers:
        multiplied_numbers = [num * 2 for num in state.numbers]

        # Create output with both original and multiplied values
        result = MultipliedList(
            original=state.numbers,
            multiplied=multiplied_numbers,
            count=len(state.numbers)
        )
        return result

    # If no numbers provided, return empty result
    return MultipliedList(original=[], multiplied=[], count=0)
"""

    # Create the transducible function
    multiplier = make_transducible_function(
        function_code=function_code,
        instructions="Multiply each integer in the input list by 2 and return the results.",
        name="multiply_by_two",
    )

    # Get the models from the created function
    IntegerList = multiplier.input_model
    MultipliedList = multiplier.target_model

    # Test with a list of integers
    input_data = IntegerList(numbers=[1, 2, 3, 4, 5])
    result = await multiplier(input_data)

    print(f"\nInput: {input_data.numbers}")
    print(f"\nResult:")
    print(result.model_dump_json(indent=2))

    return result


async def example_2_multiply_simple():
    """Example 2: Simpler version using InputModel and OutputModel"""
    print("\n\n🔢 Example 2: Multiply integers by 2 (simpler version)")
    print("-" * 70)

    class NumberInput(BaseModel):
        numbers: Optional[list[int]] = Field(None, description="List of integers")

    class NumberOutput(BaseModel):
        multiplied: Optional[list[int]] = Field(
            None, description="Numbers multiplied by 2"
        )

    # Create transducible function with custom code in function_code
    function_code = """
from typing import Optional
from pydantic import BaseModel, Field

class NumberInput(BaseModel):
    numbers: Optional[list[int]] = Field(None, description="List of integers")

class NumberOutput(BaseModel):
    multiplied: Optional[list[int]] = Field(None, description="Numbers multiplied by 2")

async def process_numbers(state: NumberInput) -> NumberOutput:
    '''Multiply each number by 2'''
    if state.numbers:
        result = [n * 2 for n in state.numbers]
        return NumberOutput(multiplied=result)
    return NumberOutput(multiplied=[])
"""

    multiplier = make_transducible_function(
        function_code=function_code,
        instructions="Multiply each number by 2",
    )

    # Get models
    NumberInput = multiplier.input_model
    NumberOutput = multiplier.target_model

    # Test with different inputs
    test_cases = [
        [1, 2, 3, 4, 5],
        [10, 20, 30],
        [7, 14, 21, 28],
    ]

    last_result = None
    for numbers in test_cases:
        input_data = NumberInput(numbers=numbers)
        last_result = await multiplier(input_data)
        print(f"\nInput:  {input_data.numbers}")
        print(f"Output: {last_result.multiplied}")

    return last_result


async def example_3_batch_multiply():
    """Example 3: Process multiple lists at once"""
    print("\n\n📋 Example 3: Batch processing - multiply multiple lists")
    print("-" * 70)

    class NumberList(BaseModel):
        numbers: Optional[list[int]] = None

    class MultipliedNumbers(BaseModel):
        result: Optional[list[int]] = None

    function_code = """
from typing import Optional
from pydantic import BaseModel

class NumberList(BaseModel):
    numbers: Optional[list[int]] = None

class MultipliedNumbers(BaseModel):
    result: Optional[list[int]] = None

async def multiply_list(state: NumberList) -> MultipliedNumbers:
    '''Multiply each number by 2'''
    if state.numbers:
        return MultipliedNumbers(result=[n * 2 for n in state.numbers])
    return MultipliedNumbers(result=[])
"""

    multiplier = make_transducible_function(
        function_code=function_code,
        instructions="Multiply each number in the list by 2",
    )

    # Get models
    NumberList = multiplier.input_model
    MultipliedNumbers = multiplier.target_model

    # Process multiple lists at once
    input_lists = [
        NumberList(numbers=[1, 2, 3]),
        NumberList(numbers=[5, 10, 15]),
        NumberList(numbers=[100, 200, 300]),
    ]

    results = await multiplier(input_lists)

    print(f"\nProcessed {len(results)} lists:")
    for i, (input_list, result) in enumerate(zip(input_lists, results), 1):
        print(f"\n{i}. Input:  {input_list.numbers}")
        print(f"   Output: {result.result}")

    return results


async def example_4_with_validation():
    """Example 4: Multiply with validation and error handling"""
    print("\n\n✅ Example 4: Multiply with validation")
    print("-" * 70)

    function_code = """
from typing import Optional
from pydantic import BaseModel, Field, field_validator

class ValidatedInput(BaseModel):
    numbers: Optional[list[int]] = Field(None, description="List of positive integers")

    @field_validator('numbers')
    @classmethod
    def validate_positive(cls, v):
        if v and any(n < 0 for n in v):
            raise ValueError("All numbers must be positive")
        return v

class ValidatedOutput(BaseModel):
    original: Optional[list[int]] = None
    doubled: Optional[list[int]] = None
    sum_original: Optional[int] = None
    sum_doubled: Optional[int] = None

async def multiply_with_stats(state: ValidatedInput) -> ValidatedOutput:
    '''Multiply by 2 and calculate statistics'''
    if state.numbers:
        doubled = [n * 2 for n in state.numbers]
        return ValidatedOutput(
            original=state.numbers,
            doubled=doubled,
            sum_original=sum(state.numbers),
            sum_doubled=sum(doubled)
        )
    return ValidatedOutput(original=[], doubled=[], sum_original=0, sum_doubled=0)
"""

    multiplier = make_transducible_function(
        function_code=function_code,
        instructions="Multiply positive integers by 2 and provide statistics",
    )

    # Get models
    ValidatedInput = multiplier.input_model
    ValidatedOutput = multiplier.target_model

    # Test with valid input
    input_data = ValidatedInput(numbers=[5, 10, 15, 20])
    result = await multiplier(input_data)

    print(f"\nInput: {input_data.numbers}")
    print(f"\nResult:")
    print(result.model_dump_json(indent=2))

    # Test with invalid input (negative numbers)
    print("\n\nTesting with negative numbers (should fail validation):")
    try:
        invalid_input = ValidatedInput(numbers=[1, -2, 3])
        print("❌ Validation should have failed!")
    except ValueError as e:
        print(f"✅ Validation worked: {e}")

    return result


async def main():
    """Run all examples"""
    print("=" * 70)
    print("Multiply Integers by 2 - Transducible Functions Examples")
    print("=" * 70)

    try:
        # Run each example
        await example_1_multiply_with_code()
        await example_2_multiply_simple()
        await example_3_batch_multiply()
        await example_4_with_validation()

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
