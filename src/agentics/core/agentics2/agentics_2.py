import asyncio
from typing import Dict, Generic, List, Optional, Tuple, Type, TypeVar, get_type_hints

from pydantic import BaseModel, Field, field_validator

from agentics import AG
from agentics.core.agentics2.utils import merge_pydantic_models


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
    # source_type: Optional[str] = None
    provenance: Optional[Provenance] = []
    # Field([],
    # description="List of attribute names from the source type that influenced the inference of the target."
    # )
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

    # @field_validator("source_slot_provenance")
    # @classmethod
    # def check_keys_match_slots(cls, v, info):
    #     if v is None:
    #         return v
    #     source_type = info.data.get("source_type")
    #     if source_type is None:
    #         raise ValueError("source_type must be set before validating provenance.")
    #     valid_fields = set(get_type_hints(source_type).keys())
    #     invalid = [k for k in v.keys() if k not in valid_fields]
    #     if invalid:
    #         raise ValueError(
    #             f"Invalid provenance keys: {invalid}. "
    #             f"Valid slots are: {sorted(valid_fields)}"
    #         )
    #     return v


from typing import List

from pydantic import BaseModel


class AgenticsTransduction(AG):
    provide_explanations: bool = False
    explanations: List[Explanation] = None

    def merge_states(self, other: AG) -> "AgenticsTransduction":
        """
        Merge multiple AgenticTransduction or AG instances into one,
        assigning equal probability to all resulting states and
        removing the explicit 'probabilities' field.

        - Concatenates states, explanations, and noise.
        - Ensures all inputs share the same .type_.
        - Returns a new AgenticsTransduction with uniform weights.
        """
        merged = self.clone()
        merged.states = []
        merged.explanations = []
        merged.atype = merge_pydantic_models(
            self.atype,
            other.atype,
            name=f"Merged{self.atype.__name__}#{other.atype.__name__}",
        )
        for self_state in self:
            for other_state in other:
                merged.states.append(
                    merged.atype(**other_state.model_dump(), **self_state.model_dump())
                )
        return merged

    async def __lshift__(self, other):
        target = await super().__lshift__(other)
        if self.provide_explanations and isinstance(other, AG):
            target_explanation = AG(atype=Explanation)
            target_explanation.instructions = f"""You have been presented with the result of the transduction of this source type {other.atype.__pydantic_fields__}, 
            into this target type {self.atype.__pydantic_fields__}. Your task is to identify which attributes from the source type contributed to the inference of the target type.
            In your input, you will find a merged instance of the source and target types.
            Provide a mapping_explanation that describes how the source attributes were used to infer the target attributes.
            Also, provide a reasoning_process that outlines the steps taken to arrive at the target type from
            """

            target_explanation = await (
                target_explanation << target.merge_states(other)
            )

            self.explanations = target_explanation.states
            self.states = target.states
            return self
        else:
            return target

        # if isinstance(result, AgenticsTransduction):
        #     # Assuming the base AG class has probabilities and explanations attributes
        #     self.probabilities = result.probabilities
        #     self.explanations = result.explanations
        return self


class Answer(BaseModel):
    answer: str


class Question(BaseModel):
    question: str


async def main():
    agent = AgenticsTransduction(atype=Answer, provide_explanations=False)
    response = await (
        agent
        << AgenticsTransduction(
            atype=Question, states=[Question(question="What is the capital of France?")]
        )
    )
    print(response)


# asyncio.run(main())
