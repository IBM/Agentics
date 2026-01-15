import os
import sys
import time

from openai import OpenAI

from tenacity import (
    retry,
    stop_after_attempt, # type: ignore
    wait_random_exponential, # type: ignore
)

from typing import Optional, List, Union, Any
if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


Model = Literal["gpt-4", "gpt-3.5-turbo", "text-davinci-003"]

OpenAI.api_key = os.getenv('OPENAI_API_KEY')
OPENAI_GEN_HYP = {
    "temperature": 0,
    "max_tokens": 250,
    "top_p": 1.,
    "frequency_penalty": 0,
    "presence_penalty": 0
}

@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
def run_chatgpt_query_multi_turn(messages,
                      model_name="gpt-4-turbo",  # pass "gpt4" for more recent model output
                      max_tokens=256,
                      temperature=0.0,
                      json_response=False):
    response = None
    num_retries = 10
    retry = 0
    while retry < num_retries:
        retry += 1
        try:
            client = OpenAI()

            if json_response:
                response = client.chat.completions.create(
                        model=model_name,
                        response_format= { "type": "json_object" },
                        messages=messages,
                        **OPENAI_GEN_HYP
                        ) 
            else:
                response = client.chat.completions.create(
                        model=model_name,
                        messages=messages,
                        **OPENAI_GEN_HYP
                        )
            break

        except Exception as e:
            print(e)
            print("GPT error. Retrying in 2 seconds...")
            time.sleep(2)

    return response


def run_llm_query_multi_turn(messages,
                            model_name: str = "gpt-4-turbo",
                            max_tokens: int = 256,
                            temperature: float = 0.0,
                            json_response: bool = False,
                            llm_object: Optional[Any] = None):
    """
    Unified LLM query function supporting both OpenAI API and CrewAI LLM objects.
    
    Args:
        messages: List of message dicts with 'role' and 'content'
        model_name: Model name (used for OpenAI API calls)
        max_tokens: Maximum tokens in response
        temperature: Temperature for sampling
        json_response: Whether to request JSON format response
        llm_object: Optional CrewAI LLM object. If provided, uses it instead of OpenAI API
        
    Returns:
        Response object compatible with OpenAI API format (has .choices[0].message.content)
    """
    
    # If CrewAI LLM object is provided, use it
    if llm_object is not None:
        try:
            from crewai import LLM
            # Check if it's a CrewAI LLM instance
            if isinstance(llm_object, LLM):
                return _run_crewai_llm_query(messages, llm_object, json_response)
        except ImportError:
            pass
    
    # Fall back to OpenAI API
    return run_chatgpt_query_multi_turn(
        messages=messages,
        model_name=model_name,
        max_tokens=max_tokens,
        temperature=temperature,
        json_response=json_response
    )


def _run_crewai_llm_query(messages: List[dict], llm: Any, json_response: bool = False) -> Any:
    """
    Execute a query using CrewAI LLM object and return response in OpenAI-compatible format.
    
    Args:
        messages: List of message dicts with 'role' and 'content'
        llm: CrewAI LLM object
        json_response: Whether to request JSON format response
        
    Returns:
        Response object with .choices[0].message.content attribute
    """
    num_retries = 10
    response = None
    
    for retry in range(num_retries):
        try:
            # Convert messages to string format for CrewAI LLM
            # CrewAI LLM.call() expects a single string prompt
            prompt = _format_messages_for_crewai(messages)
            
            # Call the LLM
            content = llm.call(prompt)
            
            # Wrap response in OpenAI-compatible format
            response = _wrap_crewai_response(content)
            break
            
        except Exception as e:
            print(f"CrewAI LLM error: {e}")
            if retry < num_retries - 1:
                print(f"Retrying ({retry + 1}/{num_retries})...")
                time.sleep(2)
            else:
                print("All retries exhausted")
                response = None
    
    return response


def _format_messages_for_crewai(messages: List[dict]) -> str:
    """Format OpenAI message format to a prompt string for CrewAI LLM."""
    prompt_parts = []
    
    for msg in messages:
        role = msg.get('role', 'user').upper()
        content = msg.get('content', '')
        prompt_parts.append(f"{role}: {content}")
    
    return "\n".join(prompt_parts)


class _OpenAICompatibleResponse:
    """Wrapper to make CrewAI LLM response compatible with OpenAI API format."""
    
    def __init__(self, content: str):
        self.choices = [_Choice(content)]


class _Choice:
    """Wrapper for choice object."""
    
    def __init__(self, content: str):
        self.message = _Message(content)


class _Message:
    """Wrapper for message object."""
    
    def __init__(self, content: str):
        self.content = content


def _wrap_crewai_response(content: str) -> _OpenAICompatibleResponse:
    """Wrap CrewAI LLM response in OpenAI-compatible format."""
    return _OpenAICompatibleResponse(content)