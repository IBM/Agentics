from typing import (
    List,
    Optional,
    Type,
    Union,
)

from pydantic import BaseModel, Field


class AttributeMapping(BaseModel):
    """Generate a mapping from the source field in the source schema to the target attributes or the target schema"""

    target_field: str = Field(
        ..., description="The attribute of the source target that has to be mapped"
    )

    source_field: Optional[str] = Field(
        [],
        description="A list of attributes from the source type that can be used as an input for a function transforming them into the target taype. Empty list if none of them apply",
    )
    explanation: Optional[str] = Field(
        None, description="""reasons why you identified this mapping"""
    )
    confidence: Optional[float] = Field(
        0, description="""Confidence level for your suggested mapping"""
    )


class AttributeMappings(BaseModel):
    attribute_mappings: Optional[List[AttributeMapping]] = []


class ATypeMapping(BaseModel):
    source_atype: Optional[Union[Type[BaseModel], str]] = None
    target_atype: Optional[Union[Type[BaseModel], str]] = None
    attribute_mappings: Optional[List[AttributeMapping]] = Field(
        None, description="List of Attribute Mapping objects"
    )
    source_dict: Optional[dict] = Field(
        None, description="The Json schema of the source type"
    )
    target_dict: Optional[dict] = Field(
        None, description="The Json schema of the target type"
    )
    source_file: Optional[str] = None
    target_file: Optional[str] = None
    mapping: Optional[dict] = Field(None, description="Ground Truth mappings")
