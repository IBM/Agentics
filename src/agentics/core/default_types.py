from dataclasses import asdict, is_dataclass
from typing import Any, Callable, Generic, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel, Field, create_model


class Astr(BaseModel):
    value: str

    def __init__(self, value):
        if not isinstance(value, str):
            raise TypeError("Astr must be constructed with a string")
        super().__init__(value=value)


class Explanation(BaseModel):
    explanation: Optional[str] = Field(
        None,
        description="Provide a logical explanation for the reasoning process"
        "needed to generate the Left object from the Right object"
        "Explain in a human understandable way the steps taken",
    )
    relevant_source_attributes: Optional[dict[str, list[str]]] = Field(
        [],
        description="A mapping, for each slot of the Left (source) type to a lists of slots in the Right (target) type that were relevant for the inference",
    )
    confidence: Optional[float] = Field(
        None,
        description="A confidence score (0.0 to 1.0) indicating the certainty of the inference done",
    )


T = TypeVar("T", bound=BaseModel)


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


class GeneratedAtype(BaseModel):
    name: Optional[str] = Field(None, description="Name of the generated Pydantic type")
    description: Optional[str] = Field(
        None, description="Description of the generated Pydantic type"
    )
    python_code: Optional[str] = Field(
        None,
        description="Python Code for the described Pydantic type. Contains only the attribute definitions. Use Pydantic V2 syntax. Include class imports  as needed.",
    )
    methods: list[str] = Field(
        None,
        description="Methods (python code) for the class above that provide additional functionality for data import and export, conversion, and other as needed. refer to self.attribute_definitions for the attributes of the class. Make all methods asynchronous",
    )
    atype: Optional[Type[BaseModel]] = Field(
        None,
        description="The Pydantic type generated from the description",
        exclude=True,
    )
