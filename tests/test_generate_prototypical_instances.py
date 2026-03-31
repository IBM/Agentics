import asyncio
from typing import Optional

import pytest
from pydantic import BaseModel

from agentics.core.transducible_functions import generate_prototypical_instances


class SimpleModel(BaseModel):
    """A simple model for testing"""
    name: str
    age: int
    email: Optional[str] = None


class ComplexModel(BaseModel):
    """A more complex model for testing"""
    id: int
    title: str
    description: str
    tags: list[str]
    is_active: bool


@pytest.mark.asyncio
async def test_generate_prototypical_instances_simple(llm_provider):
    """Test generating prototypical instances with a simple model"""
    if llm_provider is None:
        pytest.skip("No LLM provider available")
    
    instances = await generate_prototypical_instances(
        type=SimpleModel,
        n_instances=5,
        llm=llm_provider
    )
    
    # Check that we got the right number of instances
    assert len(instances) == 5, f"Expected 5 instances, got {len(instances)}"
    
    # Check that all instances are of the correct type
    for instance in instances:
        assert isinstance(instance, SimpleModel), f"Instance is not of type SimpleModel: {type(instance)}"
        assert instance.name is not None, "Name should not be None"
        assert instance.age is not None, "Age should not be None"
        assert isinstance(instance.age, int), "Age should be an integer"


@pytest.mark.asyncio
async def test_generate_prototypical_instances_complex(llm_provider):
    """Test generating prototypical instances with a complex model"""
    if llm_provider is None:
        pytest.skip("No LLM provider available")
    
    instances = await generate_prototypical_instances(
        type=ComplexModel,
        n_instances=3,
        llm=llm_provider
    )
    
    # Check that we got the right number of instances
    assert len(instances) == 3, f"Expected 3 instances, got {len(instances)}"
    
    # Check that all instances are of the correct type and have required fields
    for instance in instances:
        assert isinstance(instance, ComplexModel), f"Instance is not of type ComplexModel: {type(instance)}"
        assert instance.id is not None, "ID should not be None"
        assert instance.title is not None, "Title should not be None"
        assert instance.description is not None, "Description should not be None"
        assert isinstance(instance.tags, list), "Tags should be a list"
        assert isinstance(instance.is_active, bool), "is_active should be a boolean"


@pytest.mark.asyncio
async def test_generate_prototypical_instances_with_instructions(llm_provider):
    """Test generating prototypical instances with custom instructions"""
    if llm_provider is None:
        pytest.skip("No LLM provider available")
    
    custom_instructions = "Generate instances with names starting with 'Test' and ages between 20 and 30"
    
    instances = await generate_prototypical_instances(
        type=SimpleModel,
        n_instances=3,
        llm=llm_provider,
        instructions=custom_instructions
    )
    
    # Check that we got the right number of instances
    assert len(instances) == 3, f"Expected 3 instances, got {len(instances)}"
    
    # Check that all instances are of the correct type
    for instance in instances:
        assert isinstance(instance, SimpleModel), f"Instance is not of type SimpleModel: {type(instance)}"


@pytest.mark.asyncio
async def test_generate_prototypical_instances_default_count(llm_provider):
    """Test generating prototypical instances with default count (10)"""
    if llm_provider is None:
        pytest.skip("No LLM provider available")
    
    instances = await generate_prototypical_instances(
        type=SimpleModel,
        llm=llm_provider
    )
    
    # Check that we got the default number of instances (10)
    assert len(instances) == 10, f"Expected 10 instances (default), got {len(instances)}"
    
    # Check that all instances are valid
    for instance in instances:
        assert isinstance(instance, SimpleModel), f"Instance is not of type SimpleModel: {type(instance)}"


@pytest.mark.asyncio
async def test_generate_prototypical_instances_no_llm():
    """Test generating prototypical instances without LLM (should return empty list with one default instance)"""
    instances = await generate_prototypical_instances(
        type=SimpleModel,
        n_instances=5,
        llm=None
    )
    
    # When no LLM is provided, it should return a list with one default instance
    assert len(instances) == 1, f"Expected 1 default instance when no LLM, got {len(instances)}"


if __name__ == "__main__":
    # Run tests directly if executed as a script
    pytest.main([__file__, "-v"])

# Made with Bob
