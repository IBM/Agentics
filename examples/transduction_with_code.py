"""
Example: Running transductions using Python code as an argument

This script demonstrates how to use make_transducible_function() to create
transducible functions from Python code strings.

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


async def example_1_simple_transduction():
    """Example 1: Simple transduction without custom code"""
    print("\n📝 Example 1: Simple transduction (InputModel → OutputModel)")
    print("-" * 70)

    # Define models
    class GenericInput(BaseModel):
        content: Optional[str] = None

    class Email(BaseModel):
        to: Optional[str] = None
        subject: Optional[str] = None
        body: Optional[str] = None

    # Create transducible function without custom code
    # This creates a pure LLM-based transformation
    email_writer = make_transducible_function(
        InputModel=GenericInput,
        OutputModel=Email,
        instructions="Write a professional email based on the content. Be concise and formal.",
        name="email_writer",
    )

    # Use the function
    input_data = GenericInput(content="We need to discuss the Q4 budget")
    result = await email_writer(input_data)

    print(f"\nInput: {input_data.content}")
    print(f"\nGenerated Email:")
    print(result.model_dump_json(indent=2))

    return result


async def example_2_with_function_code():
    """Example 2: Using function_code parameter with custom logic"""
    print("\n\n🔧 Example 2: Using function_code parameter")
    print("-" * 70)

    # Complete function code including model definitions
    # This allows you to add custom pre/post-processing logic
    function_code = """
from typing import Optional
from pydantic import BaseModel
from agentics.core.transducible_functions import Transduce

class TextInput(BaseModel):
    text: Optional[str] = None

class TextOutput(BaseModel):
    result: Optional[str] = None
    word_count: Optional[int] = None

async def process_text(state: TextInput) -> TextOutput:
    '''Process the input text and add metadata'''
    # Custom pre-processing: convert to uppercase
    if state.text:
        state.text = state.text.upper()

    # Transduce triggers the LLM transformation
    return Transduce(state)
"""

    text_processor = make_transducible_function(
        function_code=function_code,
        instructions="Convert the text to uppercase and count the words. Return both the processed text and word count.",
    )

    # Get the models from the created function
    TextInput = text_processor.input_model
    TextOutput = text_processor.target_model

    # Use the function
    input_data = TextInput(text="hello world this is a test")
    result = await text_processor(input_data)

    print(f"\nInput: {input_data.text}")
    print(f"\nProcessed Output:")
    print(result.model_dump_json(indent=2))

    return result


async def example_3_summarization():
    """Example 3: Content summarization"""
    print("\n\n📊 Example 3: Content summarization")
    print("-" * 70)

    class Content(BaseModel):
        text: Optional[str] = None

    class Summary(BaseModel):
        summary: Optional[str] = Field(None, description="A brief summary")
        key_points: Optional[list[str]] = Field(
            None, description="Key points extracted"
        )

    summarizer = make_transducible_function(
        InputModel=Content,
        OutputModel=Summary,
        instructions="""
        Create a concise summary of the input content.
        Extract 2-3 key points as a list.
        """,
        name="content_summarizer",
    )

    input_data = Content(
        text="""
        The quarterly meeting revealed strong performance in sales,
        with a 25% increase compared to last quarter. However,
        operational costs have risen by 15%, primarily due to
        supply chain challenges. The team proposed three initiatives
        to address these issues: automation, vendor diversification,
        and process optimization.
        """
    )

    result = await summarizer(input_data)

    print(f"\nInput: {(input_data.text or '')[:100]}...")
    print(f"\nGenerated Summary:")
    print(result.model_dump_json(indent=2))

    return result


async def example_4_batch_processing():
    """Example 4: Batch processing with list of inputs"""
    print("\n\n📋 Example 4: Batch processing with list of inputs")
    print("-" * 70)

    class Topic(BaseModel):
        topic: Optional[str] = None

    class OneLiner(BaseModel):
        one_liner: Optional[str] = Field(
            None, description="A catchy one-line description"
        )

    one_liner_generator = make_transducible_function(
        InputModel=Topic,
        OutputModel=OneLiner,
        instructions="Create a catchy, memorable one-line description for the topic",
    )

    # Process multiple inputs at once
    topics = [
        Topic(topic="Artificial Intelligence"),
        Topic(topic="Climate Change"),
        Topic(topic="Remote Work"),
    ]

    results = await one_liner_generator(topics)

    print(f"\nProcessed {len(results)} topics:")
    for i, (topic, result) in enumerate(zip(topics, results), 1):
        print(f"\n{i}. Topic: {topic.topic}")
        print(f"   One-liner: {result.one_liner}")

    return results


async def main():
    """Run all examples"""
    print("=" * 70)
    print("Transduction with Python Code Examples")
    print("=" * 70)

    try:
        # Run each example
        await example_1_simple_transduction()
        await example_2_with_function_code()
        await example_3_summarization()
        await example_4_batch_processing()

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
