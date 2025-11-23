from typing import Callable, Generic, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel, Field


class Provenance(BaseModel):
    relevant_source_attributes: Optional[list[str]] = Field(
        [],
        description="List of attribute names from the source type that influenced the inference of the target.",
    )
    mapping_explanation: Optional[str] = Field(
        None,
        description="a json object describing the mapping from source attributes to target attributes.",
    )


T = TypeVar("T", bound=BaseModel)


class Explanation(BaseModel, Generic[T]):
    provenance: Optional[Provenance] = []
    reasoning_process: Optional[str] = Field(
        None,
        description="A brief description of the reasoning process that led to the inference of the target attributes.",
    )
    detailed_mappings: Optional[dict[str, list[str]]] = Field(
        None,
        description="A for each attribute in the target type, list the source attributes that contributed to its inference.",
    )
    confidence: Optional[float] = Field(
        None,
        description="A confidence score (0.0 to 1.0) indicating the certainty of the inference.",
    )


StateReducer = Callable[[List[BaseModel]], BaseModel | List[BaseModel]]

StateOperator = Callable[[BaseModel], BaseModel]

StateFlag = Callable[[BaseModel], bool]


#### ERRORS


class AgenticsError(Exception):
    """Base class for all custom exceptions in Agentics."""

    pass


class AmapError(AgenticsError):
    pass


class InvalidStateError(AgenticsError):
    pass


class TransductionError(AgenticsError):
    pass


class AttributeMapping(BaseModel):
    """Generate a mapping from the source field in the source schema to the target attributes or the target schema"""

    target_field: Optional[str] = Field(
        None, description="The attribute of the source target that has to be mapped"
    )

    source_field: Optional[str] = Field(
        [],
        description="The attribute from the source type that can be used as an input for a function transforming it into the target taype. Empty list if none of them apply",
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
