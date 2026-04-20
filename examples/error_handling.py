"""
Error Handling and Resilience Patterns in Agentics

This example demonstrates robust transduction patterns including:
- Retry logic with exponential backoff
- Graceful error handling and fallback strategies
- Partial result processing
- Input validation and sanitization
- Error categorization and reporting
"""

import asyncio
import logging
from typing import Optional, List, Union
from enum import Enum

from pydantic import BaseModel, Field, ValidationError
from agentics import AG


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ErrorType(str, Enum):
    VALIDATION_ERROR = "validation_error"
    LLM_ERROR = "llm_error"
    TIMEOUT_ERROR = "timeout_error"
    PARSE_ERROR = "parse_error"
    UNKNOWN_ERROR = "unknown_error"


class RobustAnswer(BaseModel):
    """A resilient answer type that captures both success and failure states."""
    
    answer: Optional[str] = Field(None, description="The extracted answer")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Confidence score 0-1")
    source_question: Optional[str] = Field(None, description="Original input question")
    error_type: Optional[ErrorType] = Field(None, description="Type of error if processing failed")
    error_message: Optional[str] = Field(None, description="Detailed error message")
    retry_count: int = Field(0, description="Number of retry attempts made")
    processing_time_ms: Optional[float] = Field(None, description="Processing time in milliseconds")
    is_success: bool = Field(True, description="Whether processing succeeded")


class ValidationResult(BaseModel):
    """Result of input validation with sanitized input."""
    
    is_valid: bool
    sanitized_input: Optional[str] = None
    validation_errors: List[str] = []


def validate_and_sanitize_input(input_text: str) -> ValidationResult:
    """Validate and sanitize input before processing."""
    
    errors = []
    sanitized = input_text.strip()
    
    # Basic validation rules
    if not sanitized:
        errors.append("Empty input")
        return ValidationResult(is_valid=False, validation_errors=errors)
    
    if len(sanitized) > 1000:
        errors.append("Input too long, truncating")
        sanitized = sanitized[:1000] + "..."
    
    # Remove potential harmful characters or patterns
    forbidden_patterns = ["<script>", "javascript:", "eval("]
    for pattern in forbidden_patterns:
        if pattern.lower() in sanitized.lower():
            errors.append(f"Removed forbidden pattern: {pattern}")
            sanitized = sanitized.replace(pattern, "[SANITIZED]")
    
    return ValidationResult(
        is_valid=True,
        sanitized_input=sanitized,
        validation_errors=errors
    )


async def safe_transduction_with_retry(
    agent: AG,
    input_data: Union[str, List[str]],
    max_retries: int = 3,
    timeout_seconds: float = 30.0
) -> AG:
    """
    Perform transduction with retry logic and comprehensive error handling.
    
    Args:
        agent: The AG instance to use for transduction
        input_data: Input data to process
        max_retries: Maximum number of retry attempts
        timeout_seconds: Timeout for each attempt
        
    Returns:
        AG instance with results (may include error states)
    """
    
    import time
    from asyncio import wait_for, TimeoutError
    
    # Ensure input_data is a list
    if isinstance(input_data, str):
        input_data = [input_data]
    
    results = []
    
    for i, input_item in enumerate(input_data):
        start_time = time.time()
        retry_count = 0
        
        # Validate input first
        validation = validate_and_sanitize_input(input_item)
        if not validation.is_valid:
            results.append(RobustAnswer(
                source_question=input_item,
                error_type=ErrorType.VALIDATION_ERROR,
                error_message="; ".join(validation.validation_errors),
                is_success=False,
                processing_time_ms=(time.time() - start_time) * 1000
            ))
            continue
        
        # Use sanitized input
        sanitized_input = validation.sanitized_input
        
        # Retry loop
        while retry_count <= max_retries:
            try:
                # Create a fresh agent instance for each retry
                temp_agent = AG(atype=RobustAnswer, llm=agent.llm)
                
                # Attempt transduction with timeout
                result = await wait_for(
                    temp_agent << [sanitized_input],
                    timeout=timeout_seconds
                )
                
                # Extract the result and add metadata
                if result.states and len(result.states) > 0:
                    answer = result.states[0]
                    answer.source_question = input_item
                    answer.retry_count = retry_count
                    answer.processing_time_ms = (time.time() - start_time) * 1000
                    answer.is_success = True
                    results.append(answer)
                    logger.info(f"Successfully processed item {i+1} after {retry_count} retries")
                    break
                else:
                    raise ValueError("No result returned from transduction")
                    
            except TimeoutError:
                logger.warning(f"Timeout on item {i+1}, retry {retry_count+1}")
                retry_count += 1
                if retry_count > max_retries:
                    results.append(RobustAnswer(
                        source_question=input_item,
                        error_type=ErrorType.TIMEOUT_ERROR,
                        error_message=f"Timeout after {timeout_seconds}s, {max_retries} retries",
                        retry_count=retry_count - 1,
                        is_success=False,
                        processing_time_ms=(time.time() - start_time) * 1000
                    ))
                else:
                    # Exponential backoff
                    await asyncio.sleep(2 ** retry_count)
                    
            except ValidationError as e:
                logger.error(f"Validation error on item {i+1}: {e}")
                results.append(RobustAnswer(
                    source_question=input_item,
                    error_type=ErrorType.VALIDATION_ERROR,
                    error_message=str(e),
                    retry_count=retry_count,
                    is_success=False,
                    processing_time_ms=(time.time() - start_time) * 1000
                ))
                break
                
            except Exception as e:
                logger.error(f"Unexpected error on item {i+1}, retry {retry_count+1}: {e}")
                retry_count += 1
                if retry_count > max_retries:
                    results.append(RobustAnswer(
                        source_question=input_item,
                        error_type=ErrorType.UNKNOWN_ERROR,
                        error_message=str(e),
                        retry_count=retry_count - 1,
                        is_success=False,
                        processing_time_ms=(time.time() - start_time) * 1000
                    ))
                else:
                    # Exponential backoff
                    await asyncio.sleep(2 ** retry_count)
    
    # Create result agent with all results
    result_agent = AG(atype=RobustAnswer)
    result_agent.states = results
    
    return result_agent


def analyze_results(agent: AG) -> dict:
    """Analyze the results and provide a summary report."""
    
    total = len(agent.states)
    successful = sum(1 for state in agent.states if state.is_success)
    failed = total - successful
    
    # Categorize errors
    error_counts = {}
    total_retries = 0
    total_processing_time = 0
    
    for state in agent.states:
        if state.error_type:
            error_counts[state.error_type] = error_counts.get(state.error_type, 0) + 1
        total_retries += state.retry_count
        if state.processing_time_ms:
            total_processing_time += state.processing_time_ms
    
    return {
        "total_items": total,
        "successful": successful,
        "failed": failed,
        "success_rate": successful / total if total > 0 else 0,
        "error_breakdown": error_counts,
        "total_retries": total_retries,
        "avg_processing_time_ms": total_processing_time / total if total > 0 else 0
    }


async def main():
    """Demonstrate error handling patterns with various input scenarios."""
    
    print("🔧 Agentics Error Handling Demo")
    print("=" * 50)
    
    # Test inputs with different error scenarios
    test_inputs = [
        "What is the capital of France?",  # Normal case
        "",  # Empty input - validation error
        "What is 2 + 2?",  # Simple math
        "x" * 1500,  # Too long - will be truncated
        "<script>alert('xss')</script>What is AI?",  # Malicious input
        "What happens when the LLM fails to respond properly and we need fallback handling?",  # Complex case
        "Simple question",  # Normal case
    ]
    
    # Create agent
    if not AG.get_llm_provider():
        print("⚠️  No LLM provider configured. Set up your API key in .env file.")
        return
        
    agent = AG(atype=RobustAnswer, llm=AG.get_llm_provider())
    
    print(f"📝 Processing {len(test_inputs)} test inputs with error handling...")
    print()
    
    # Process with error handling
    results = await safe_transduction_with_retry(
        agent=agent,
        input_data=test_inputs,
        max_retries=2,
        timeout_seconds=15.0
    )
    
    # Display results
    print("📊 RESULTS:")
    print("-" * 50)
    
    for i, result in enumerate(results.states):
        status = "✅" if result.is_success else "❌"
        print(f"{status} Item {i+1}:")
        print(f"   Question: {result.source_question[:50]}...")
        if result.is_success:
            print(f"   Answer: {result.answer}")
            print(f"   Confidence: {result.confidence}")
        else:
            print(f"   Error: {result.error_type} - {result.error_message}")
        print(f"   Retries: {result.retry_count}, Time: {result.processing_time_ms:.1f}ms")
        print()
    
    # Summary analysis
    analysis = analyze_results(results)
    print("📈 SUMMARY ANALYSIS:")
    print("-" * 50)
    print(f"Success Rate: {analysis['success_rate']:.1%} ({analysis['successful']}/{analysis['total_items']})")
    print(f"Total Retries: {analysis['total_retries']}")
    print(f"Avg Processing Time: {analysis['avg_processing_time_ms']:.1f}ms")
    
    if analysis['error_breakdown']:
        print("\nError Breakdown:")
        for error_type, count in analysis['error_breakdown'].items():
            print(f"  {error_type}: {count}")
    
    print("\n🎯 Key Takeaways:")
    print("- Input validation prevents processing of malformed data")
    print("- Retry logic with exponential backoff handles transient failures")
    print("- Structured error reporting aids debugging and monitoring")
    print("- Partial success handling allows processing to continue despite failures")


if __name__ == "__main__":
    asyncio.run(main())