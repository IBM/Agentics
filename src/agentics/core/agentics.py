import asyncio
import csv
import json
import random
import time
from collections.abc import Iterable
from copy import copy, deepcopy
from functools import partial, reduce
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

import crewai
import pandas as pd
import yaml
from langchain_core.prompts import PromptTemplate
from loguru import logger
from pandas import DataFrame
from pydantic import BaseModel, Field, conlist, create_model

from agentics.abstractions.pydantic_transducer import (
    PydanticTransducerCrewAI,
    PydanticTransducerVLLM,
)
from agentics.abstractions.structured_output import generate_structured_output

# from agentics.core.globals import Memory
from agentics.core.llm_connections import watsonx_llm
from agentics.core.utils import (
    are_models_structurally_identical,
    chunk_list,
    clean_for_json,
    get_active_fields,
    make_all_fields_optional,
    pydantic_model_from_csv,
    pydantic_model_from_dataframe,
    pydantic_model_from_dict,
    pydantic_model_from_jsonl,
    remap_dict_keys,
    sanitize_dict_keys,
)

# memory = Memory()


T = TypeVar("T", bound=BaseModel)
ReduceStatesType = Callable[[List[T]], T]


class AgenticsError(Exception):
    """Base class for all custom exceptions in Agentics."""

    pass


class InvalidStateError(AgenticsError):
    pass


class TransductionError(AgenticsError):
    pass


from enum import Enum


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


AG = TypeVar("A", bound="Agentics")


class Agentics(BaseModel):
    """
    Agentics is a Python class that wraps a list of Pydantic objects and enables structured, type-driven logical transduction between them.

    Internally, Agentics is implemented as a Pydantic model. It holds:
        •	atype: a reference to the Pydantic class shared by all objects in the list.
        •	states: a list of Pydantic instances, each validated to be of type atype.
        •	tools: a list of tools (CrewAI or Langchain) to be used for transduction

    """

    model_config = {"arbitrary_types_allowed": True}
    atype: Type[BaseModel] = Field(
        BaseModel,
        description="""this is the type in common among all element of the list""",
    )
    states: List[BaseModel] = []
    transduce_fields: Optional[List[str]] = Field(
        None,
        description="""this is the list of field that will be used for the transduction, both incoming and outcoming""",
    )
    llm: Any = Field(watsonx_llm, exclude=True)
    tools: Optional[List[Any]] = Field(None, exclude=True, description="   ")
    instructions: Optional[str] = Field(
        """Generate an object of the specified type from the following input.""",
        description="Special instructions to be given to the agent for executing transduction",
    )
    prompt_template: Optional[str] = Field(
        None,
        description="Langchain style prompt pattern to be used when provided as an input for a transduction.  Refer to https://python.langchain.com/docs/concepts/prompt_templates/ ",
    )
    crew_prompt_params: Optional[Dict[str, str]] = Field(
        {
            "role": "Task Executor",
            "goal": "You execute tasks",
            "backstory": "You are always faithful and provide only fact based answers.",
            "expected_output": "Described by Pydantic Type",
        },
        description="prompt parameter for initializing Crew and Task",
    )
    skip_intensional_definiton: bool = Field(
        False,
        description="if True, don't compose intentional instruction for Crew Task",
    )
    memory_collection: Optional[str] = None
    transduction_logs_path: Optional[str] = Field(
        None,
        description="""If not null, the specified file will be created and used to save the intermediate results of transduction from each batch. The file will be updated in real time and can be used for monitoring""",
    )
    batch_size_transduction: Optional[int] = 20
    batch_size_amap: Optional[int] = 10
    verbose_transduction: bool = True
    verbose_agent: bool = False

    @staticmethod
    def create_crewai_llm(**kwargs):
        from crewai import LLM

        return LLM(**kwargs)

    ### Turn agentics into lists iterating on the states
    def __iter__(self):
        return iter(self.states)

    def __len__(self):
        return len(self.states)

    def __call__(self, *fields) -> AG:
        """Returns a new agentic with the subtype of fields"""
        atype = self.subset_atype(fields)
        new_ag = self.rebind_atype(atype, {i: i for i in fields})
        new_ag.transduce_fields = list(fields)
        return new_ag

    def __getitem__(self, index: int):
        """Returns the state for the provided index"""
        return self.states[index]

    # Synchronous map
    def filter(self, func: Callable[[BaseModel], bool]) -> AG:
        """func should be a function that takes as an input a state and return a boolean, false will be filtered out"""
        self.states = [state for state in self.states if func(state)]
        return self

    # Asynchronous map with exception-safe job gathering
    async def amap(self, func: Awaitable) -> AG:
        if self.verbose_transduction:
            logger.debug(f"Executing amap on function {func}")

        ## TODO override states
        chunks = chunk_list(self.states, self.batch_size_transduction)
        results = []
        i = 1
        for chunk in chunks:
            try:
                begin_time = time.time()
                tasks = []
                for state in chunk:
                    corutine = asyncio.wait_for(func(state), timeout=300)
                    tasks.append(corutine)
                result_run = await asyncio.gather(*tasks, return_exceptions=True)
                if self.transduction_logs_path:
                    with open(self.transduction_logs_path, "a") as f:
                        for state in result_run:
                            f.write(state.model_dump_json() + "\n")

                results += result_run

                # pt = PydanticTransducer(self.subset_atype(self.transduce_fields) if self.transduce_fields else self.atype, tools=self.tools, llm=self.llm, intensional_definiton=intensional_definition, verbose=self.verbose)
                # _states+= await pt.async_transduce(chunk)
                end_time = time.time()
                if self.verbose_transduction:
                    logger.debug(
                        f"{i * self.batch_size_amap if i > 1 else len(chunk)} states processed. {(end_time - begin_time) / self.batch_size_amap} seconds average per state in the last chunk ..."
                    )
                i += 1
            except asyncio.TimeoutError and Exception as e:
                size = (
                    self.batch_size_amap
                    if len(chunk) == self.batch_size_amap
                    else len(chunk)
                )
                if self.verbose_transduction:
                    logger.debug(
                        f"ERROR, states {(i - 1) * self.batch_size_amap + 1} to {((i - 1) * self.batch_size_amap) + size} have not been transduced"
                    )
                if self.verbose_transduction:
                    logger.debug(e)

                results += chunk

        # tasks = [func(state) for state in self.states]
        # results = await asyncio.gather(*tasks, return_exceptions=True)

        new_states = []
        for i, result in enumerate(results):
            if isinstance(result, Exception) or isinstance(
                result, asyncio.TimeoutError
            ):
                if self.verbose_transduction:
                    logger.debug(f"⚠️ Error processing state {i}: {result}")
                # You can choose to skip, retry, or store the error
                new_states.append(
                    self.states[i]
                )  # or keep the original: self.states[i]
            else:
                new_states.append(result)

        self.states = new_states
        return self

    def reduce(self, func: ReduceStatesType[T]) -> T:
        return func(self.states)
        # self.states = output
        # return self

    @classmethod
    def from_states(cls, states: List[BaseModel], atype: BaseModel = None) -> AG:
        if len(states) == 0:
            return cls()
        else:
            if not atype:
                if isinstance(states[0], BaseModel):
                    atype = type(states[0])
            wrong_state = None
            for state in states:
                if atype != type(state):
                    wrong_state = state
            if not wrong_state:
                return Agentics(atype=atype, states=states)
            else:
                raise InvalidStateError(
                    f"Expected {atype} for object {wrong_state.model_dump_json}"
                )

    @classmethod
    def from_csv(
        cls,
        csv_file,
        atype: Type[BaseModel] = None,
        max_rows: int = None,
        task_description: str = None,
        verbose=False,
    ) -> AG:
        """
        Import an object of type Agentics from a CSV file.
        If atype is not provided it will be automatically inferred from the column names and
        all attributes will be set as strings
        """

        states: List = []
        new_type = None
        if atype:
            logger.debug(
                f"Importing Agentics of type {atype.__name__} from CSV {csv_file}"
            )
            new_type = atype
        else:
            new_type = make_all_fields_optional(pydantic_model_from_csv(csv_file))
        with open(csv_file, encoding="utf-8-sig") as f:
            c_row = 0
            for row in csv.DictReader(f):
                if not max_rows or c_row < max_rows:
                    state = new_type(**row)
                    states.append(state)
                c_row += 1
        return cls(states=states, atype=new_type, task_description=task_description)

    @classmethod
    def from_dataframe(
        cls, dataframe: DataFrame, atype: Type[BaseModel] = None, max_rows: int = None
    ) -> AG:
        """
        Import an object of type Agentics from a Pandas DataFrame object.
        If atype is not provided it will be automatically inferred from the column names and
        all attributes will be set as strings
        """
        states: List[BaseModel] = []
        new_type = atype or pydantic_model_from_dataframe(dataframe)
        logger.debug(f"Importing Agentics of type {new_type.__name__} from DataFrame")

        for i, row in dataframe.iterrows():
            if max_rows and i >= max_rows:
                break
            state = new_type(**row.to_dict())
            states.append(state)
        return cls(states=states, atype=new_type)

    @classmethod
    def from_json(
        cls,
        path_to_json_file: str,
        atype: Optional[Type[BaseModel]] = None,
        max_rows: Optional[int] = None,
        jsonl: bool = False,
    ) -> AG:
        """
        Import an object of type Agentics from jsonl file.
        If atype is not provided it will be automatically inferred from the json schema.
        """
        if jsonl:
            states: List = []
            c_row = 0
            new_type = None
            if atype:
                new_type = atype
            else:
                new_type = pydantic_model_from_jsonl(path_to_json_file)
            for line in open(path_to_json_file, encoding="utf-8"):
                if not max_rows or c_row < max_rows:
                    state_dict = sanitize_dict_keys(json.loads(line))
                    states.append(new_type(**state_dict))
                c_row += 1
            return cls(states=states, atype=new_type)
        else:
            c_row = 0
            input_states = json.load(open(path_to_json_file, encoding="utf-8"))
            states = []
            if atype:
                new_type = atype
            else:
                new_type = (
                    pydantic_model_from_dict(input_states[0])
                    if len(input_states) > 0
                    else BaseModel
                )

            for state in input_states:
                if not max_rows or c_row < max_rows:
                    state_dict = sanitize_dict_keys(state)
                    states.append(new_type(**state_dict))
                c_row += 1
            return cls(states=states, atype=new_type)

    def subset_atype(self, include_fields: set[str]) -> Type[BaseModel]:
        """Generate a type which is a subset of a_type containing only fields in include list"""
        fields = {
            field: (
                self.atype.model_fields[field].annotation,
                self.atype.model_fields[field].default,
            )
            for field in include_fields
        }
        return create_model("_".join(include_fields), **fields)

    def rebind_atype(
        self, new_atype: BaseModel, mapping: Dict[str, str] = None
    ) -> BaseModel:
        """Return an agentic of type atype where all the states have been converted to atype, keeping only the matching attributes, discariding the remaining."""
        new_ag = deepcopy(self)
        new_ag.atype = new_atype
        new_ag.states = []

        for state in self.states:
            if mapping:
                new_state = remap_dict_keys(state.model_dump(), mapping)
                new_ag.states.append(new_atype(**new_state))

            else:
                new_ag.states.append(new_atype(**state.model_dump()))
        return new_ag

    def add_attribute(
        self,
        slot_name: str,
        slot_type: type = str,
        default_value=None,
        description: Optional[str] = None,
    ):
        """
        Add a new slot to the `atype` and rebase the Agentics model.

        Args:
            slot_name (str): Name of the new slot to add.
            slot_type (type): Data type of the slot (default: str).
            default_value: Default value for the slot (default: None).
            description (str, optional): Description for the slot.

        Returns:
            Type[BaseModel]: A new Pydantic model with the added slot.
        """
        # Clone existing fields
        fields = {
            field: (
                self.atype.model_fields[field].annotation,
                Field(
                    default=self.atype.model_fields[field].default,
                    description=self.atype.model_fields[field].description,
                ),
            )
            for field in self.atype.model_fields.keys()
        }

        # Add the new field
        fields[slot_name] = (
            slot_type,
            Field(default=default_value, description=description),
        )

        # Create a new model with the added field
        new_model = create_model(f"{self.atype.__name__}_extended", **fields)

        # Optionally re-assign it to self.atype
        return self.rebind_atype(new_model)

    def clone(agentics_instance):
        copy_instance = copy(agentics_instance)
        copy_instance.states = deepcopy(agentics_instance.states)
        copy_instance.tools = agentics_instance.tools  # shallow copy, ok if immutable
        return copy_instance

    def truncate_states(self, start: int, end: int) -> AG:
        self.states = self.states[start:end]
        return self

    @staticmethod
    def copy_attribute_values(
        state: BaseModel, source_attribute: str, target_attribute: str
    ) -> BaseModel:
        """for each state, copy the value from source_attribute to the target_attribute
        Usage: for generating fewshots,
        copy values for the target_attribute from source_attribute that holds the ground_truth.
        """
        source_value = getattr(state, source_attribute)
        setattr(state, target_attribute, source_value)
        return state

    async def copy_fewshots_from_ground_truth(
        self, source_target_pairs: list[tuple[str, str]], first_n: Optional[int] = None
    ) -> AG:
        """for each state, copy fields values from ground truth to target attributes
        to be used as fewshot during transduction
        """
        for src, target in source_target_pairs:
            func = partial(
                Agentics.copy_attribute_values,
                source_attribute=src,
                target_attribute=target,
            )
            await self.apply_to_states(func, first_n=first_n)
        return self

    async def __lshift__(self, other):
        """This is a transduction operation projecting a list of pydantic objects of into a target types
        Results are accumulated in the self instance and returned back as a result.
        Return None if the right operand is not of type AgenticList
        """
        output = self.clone()
        output.states = []
        input_prompts = (
            []
        )  ## gather input prompts for transduction by dumping input states
        target_type = (
            self.subset_atype(self.transduce_fields)
            if self.transduce_fields
            else self.atype
        )
        if isinstance(other, Agentics):
            if self.verbose_transduction:
                logger.debug(
                    f"Executing task: {self.instructions}\n{len(other.states)} states will be transduced"
                )

            if other.prompt_template:
                prompt_template = PromptTemplate.from_template(other.prompt_template)
            else:
                prompt_template = None
            i = 0
            for i in range(len(other.states)):
                if prompt_template:
                    input_prompts.append(
                        "SOURCE:\n"
                        + prompt_template.invoke(
                            other.states[i].model_dump(include=other.transduce_fields)
                        ).text
                    )
                else:
                    input_prompts.append(
                        "SOURCE:\n"
                        + json.dumps(
                            other.states[i].model_dump(include=other.transduce_fields)
                        )
                    )

        elif isinstance(other, Iterable) and all(isinstance(i, str) for i in other):
            if self.verbose_transduction:
                logger.debug(
                    f"Transduction from input texts {other} to {type(target_type)} in progress. This might take a while"
                )
            input_prompts = ["\nSOURCE:\n" + x for x in other]
        else:
            return NotImplemented

        ## expand prompts with relevant knowledge from memory
        if self.memory_collection:
            collections = await memory.get_collections()
            if self.memory_collection in collections:
                final_prompts = []
                for prompt in input_prompts:
                    passages = memory.retrieve_content(self.memory_collection, prompt)
                    newline_split_passages = "\n".join(passages)
                    final_prompts.append(
                        f"""Read the following passages provided as context: 
                                            {newline_split_passages}
                                            Now transduce output for the following prompt:
                                            {prompt}"""
                    )
                input_prompts = final_prompts

        ## collect few shots, only when all target slots are non null TODO need to improve with some non null
        instructions = ""

        # Add instructions
        if self.skip_intensional_definiton:
            instructions = f"{self.instructions}" if self.instructions else "\n"
        else:
            instructions += "\nYour task is to transduce a source Pydantic Object into the specified Output type. Generate only slots that are logically deduced from the input information, otherwise live then null.\n"
            if self.instructions:
                instructions += (
                    "\nRead carefully the following instructions for executing your task:\n"
                    + self.instructions
                )

        # Gather few shots
        few_shots = ""
        for i in range(len(self.states)):
            if self.states[i] and get_active_fields(
                self.states[i], allowed_fields=set(self.transduce_fields)
            ) == set(self.transduce_fields):
                few_shots += (
                    "Example\nSOURCE:\n"
                    + other.states[i].model_dump_json(include=other.transduce_fields)
                    + "\nTARGET:\n"
                    + self.states[i].model_dump_json(include=self.transduce_fields)
                    + "\n"
                )
        if len(few_shots) > 0:
            instructions += (
                "Here is a list of few shots examples for your task:\n" + few_shots
            )

        ## Perform Transduction
        ## TODO override states
        chunks = chunk_list(input_prompts, self.batch_size_transduction)
        output_states = []

        i = 1
        transducer_class = (
            PydanticTransducerCrewAI
            if type(self.llm) == crewai.LLM
            else PydanticTransducerVLLM
        )
        if self.verbose_transduction:
            logger.debug(f"transducer class: {transducer_class}")
        for chunk in chunks:
            try:
                begin_time = time.time()
                transduced_type = (
                    self.subset_atype(self.transduce_fields)
                    if self.transduce_fields
                    else self.atype
                )
                pt = transducer_class(
                    transduced_type,
                    tools=self.tools,
                    llm=self.llm,
                    intensional_definiton=instructions,
                    verbose=self.verbose_agent,
                    **self.crew_prompt_params,
                )
                output_states_tmp = await asyncio.wait_for(
                    pt.async_transduce(chunk), timeout=200
                )
                output_states += [
                    (
                        output_state
                        if not isinstance(output_states_tmp, asyncio.TimeoutError)
                        else self.states[i]
                    )
                    for i, output_state in enumerate(output_states_tmp)
                ]
                end_time = time.time()
                size = (
                    self.batch_size_transduction
                    if len(chunk) == self.batch_size_transduction
                    else len(chunk)
                )
                if self.verbose_transduction:
                    logger.debug(
                        f"Processed {size} states in {end_time - begin_time} seconds"
                    )
            except Exception as e:
                if self.verbose_transduction:
                    logger.debug(
                        "Warning: Failed to transduce batch. Executing individual steps"
                    )
                size = (
                    self.batch_size_transduction
                    if len(chunk) == self.batch_size_transduction
                    else len(chunk)
                )

                begin_time = time.time()
                pt = transducer_class(
                    target_type,
                    tools=self.tools,
                    llm=self.llm,
                    intensional_definiton=instructions,
                    verbose=self.verbose_agent,
                    **self.crew_prompt_params,
                )
                for state in chunk:
                    try:
                        output_transduction = await pt.async_transduce([state])
                        if isinstance(output_transduction, tuple):
                            output_states += dict([output_transduction])
                        else:
                            output_states += output_transduction
                        logger.debug(".", end="")
                    except Exception as e:
                        if self.verbose_transduction:
                            logger.debug(
                                f"Warning: Failed to transduce state {state} for the following reason: ",
                                e,
                            )
                        output_states += [target_type()]
                end_time = time.time()
                if self.verbose_transduction:
                    logger.debug(
                        f"Processed {size} states in {end_time - begin_time} seconds"
                    )

            if self.transduction_logs_path:
                with open(self.transduction_logs_path, "a") as f:
                    for state in output_states:
                        if state :
                            f.write(state.model_dump_json() + "\n")
                        else: f.write(self.atype().model_dump_json() + "\n")
            if self.verbose_transduction:
                logger.debug(
                    f"{i * self.batch_size_transduction if i > 1 else len(chunk)} states processed in {(end_time - begin_time) / self.batch_size_transduction} seconds average per state ..."
                )
            i += 1

        if isinstance(other, Agentics):
            for i in range(len(other.states)):
                output_state = output_states[i]
                if isinstance(output_state, tuple):
                    output_state_dict = dict([output_state])
                elif are_models_structurally_identical(type(output_state), target_type):
                    output_state_dict = output_state.model_dump()
                else:
                    output_state_dict = output_state[0].model_dump()

                merged = self.atype(
                    **(other.states[i].model_dump() | output_state_dict)
                )
                output.states.append(merged)
        elif isinstance(other, Iterable) and all(isinstance(i, str) for i in other):
            for i in range(len(other)):
                if isinstance(output_states[i] , self.atype):
                    output.states.append(self.atype(**output_states[i].model_dump()))
                elif output_states[i]:
                    output.states.append(self.atype(**output_states[i][0].model_dump()))
                else: output.states.append(self.atype())
        return output

    async def apply_to_states(
        self, func: Callable[[BaseModel], BaseModel], first_n: Optional[int] = None
    ) -> AG:
        """
        Applies a function to each state in the Agentics object.

        Parameters:
        - func: A function that takes a Pydantic model (a state) and returns a modified Pydantic model.

        Returns:
        - A new Agentics object with the transformed states.
        """
        if first_n is None:
            self.states = [func(state) for state in self.states]
        else:
            self.states = [
                func(state) for state in self.states[:first_n]
            ] + self.states[first_n:]
        return self

    def product(self, other: AG) -> AG:
        """
        AG1.product(AG2, include_fields) returns the product of two types AG'

        e.g.    AG1([x1,x2]) * AG2([y1, y2]) returns AG([x1-y1, x2-y1, x2-y1, x2-y2])
                here, xi-yj means the filed values are filled in from xi and yj so making a product of two states

        Usage: AG1 is an optimizer and AG2 is evaluation set.
        duplicate dataset AG2 per each AG1 optimization parameter set.
        """
        new_fields = {}
        for field in other.atype.model_fields.keys():
            new_fields[field] = (
                other.atype.model_fields[field].annotation,
                Field(
                    default=other.atype.model_fields[field].default,
                    description=other.atype.model_fields[field].description,
                ),
            )

        for field in self.atype.model_fields.keys():
            new_fields[field] = (
                self.atype.model_fields[field].annotation,
                Field(
                    default=self.atype.model_fields[field].default,
                    description=self.atype.model_fields[field].description,
                ),
            )
        prod_atype = create_model(
            f"{self.atype.__name__}__{other.atype.__name__}", **new_fields
        )

        extended_ags = []
        for state in self.states:
            extended_ag = deepcopy(other)
            extended_ag.atype = prod_atype
            extended_ag.states = [
                prod_atype(**(other_state.model_dump() | state.model_dump()))
                for other_state in other.states
            ]
            extended_ags.append(extended_ag)

        return reduce((lambda x, y: Agentics.add_states(x, y)), extended_ags)

    def quotient(self, other: AG) -> list[AG]:
        """
        AG1.quotient(AG') returns the list of quotients [AG1]

        Revsers of the product, segment the states of AG'

        Usage: After evaluating the prompts we want separate the evaluated sets and reduce score from each
        """
        quotient_list = []
        quotient_size, quotient_counts = len(self.states), len(other.states) // len(
            self.states
        )
        for ind in range(quotient_counts):
            quotient_ag = self.clone()
            quotient_ag.states = [
                self.atype(**(other_state.model_dump()))
                for other_state in other.states[
                    ind * quotient_size : (ind + 1) * quotient_size
                ]
            ]
            quotient_list.append(quotient_ag)
        return quotient_list

    @staticmethod
    def add_states(first: AG, other: AG) -> AG:
        return Agentics(
            atype=first.atype, tools=first.tools, states=first.states + other.states
        )

    async def __add__(self, other):
        transducer_class = (
            PydanticTransducerCrewAI
            if type(self.llm) == crewai.LLM
            else PydanticTransducerVLLM
        )
        if isinstance(other, Agentics):
            return Agentics(
                atype=self.atype, tools=self.tools, states=self.states + other.states
            )
        elif isinstance(other, int):
            # TODO implement logics for expansion
            if self.verbose_transduction:
                logger.debug(
                    f"Generating {other} synthetic data samples for type {self.atype}"
                )
            instructions = (
                f"""Generate a random entity of the given type:\n{self.atype.model_json_schema()}"""
                + (
                    f"""those are samples you can take inspiration from:\n{str(self.states)}"""
                    if len(self.states) > 0
                    else ""
                )
            )
            pt = PydanticTransducerCrewAI(
                self.atype,
                tools=self.tools,
                llm=self.llm,
                verbose=self.verbose_agent,
                intensional_definiton=instructions,
            )
            states = await pt.async_transduce([""], n_samples=other)
            if type(states[0] == self.atype):
                self.states += states
            else:
                self.states += states[0]
            return self

        return NotImplemented

    async def map_atypes(self, other: AG) -> ATypeMapping:
        if self.verbose_agent:
            logger.debug(f"Mapping type {other.atype} into type {self.atype}")

        # target_schema_dict= self.atype.model_json_schema()
        # source_schema_dict= other.atype.model_json_schema()
        target_attributes = []
        for target_attribute in self.atype.model_fields.items():
            target_attributes.append(
                "TARGET_SCHEMA:\n"
                + str(self.atype.model_json_schema())
                + "\nTARGET_ATTRIBUTE: "
                + str(target_attribute[0])
                + "\nSOURCE_SCHEMA:\n"
                + str(other.atype.model_json_schema())
            )

        mappings = Agentics(atype=AttributeMapping)
        mappings.instructions = f"""Map the TARGET_ATTRIBUTE to the right attribute of in the SOURCE_SCHEMA"""
        output = await (mappings << target_attributes)
        return ATypeMapping(
            source_atype=other.atype,
            target_atype=self.atype,
            attribute_mappings=output.states,
        )

    async def map_atypes_fast(self, other: AG) -> ATypeMapping:
        if self.verbose_agent:
            logger.debug(f"Mapping type {other.atype} into type {self.atype}")

        target_schema_dict = self.atype.model_json_schema()
        source_schema_dict = other.atype.model_json_schema()["properties"]
        mappings = Agentics(atype=ATypeMapping, transduce_fields=["attribute_mappings"])
        mappings.instructions = f"""provide each attribute mapping from the SOURCE schema to zero or more attributes of the TARGET schema, providing a pydantic output as instructed"""
        output = await (
            mappings
            << [f"SOURCE:\n{str(source_schema_dict)}\nTARGET:{str(target_schema_dict)}"]
        )
        return output.attribute_mappings

    async def self_transduction(
        self,
        source_fields: List[str],
        target_fields: List[str],
        instructions: str = None,
    ):
        target = self.clone()
        self.transduce_fields = source_fields
        if instructions:
            target.instructions = instructions
        target.transduce_fields = target_fields

        output_process = target << self
        output = await output_process
        return output

    def get_random_sample(self, percent: float) -> AG:
        if not (0 <= percent <= 1):
            raise ValueError("Percent must be between 0 and 1")

        sample_size = int(len(self.states) * percent)
        output = self.clone()
        output.states = random.sample(self.states, sample_size)
        return output

    def to_csv(self, csv_file: str) -> Any:
        if self.verbose_transduction:
            logger.debug(f"Exporting {len(self.states)} Agentics to CSV {csv_file}")
        field_names = self.atype.model_fields.keys()
        with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            for state in self.states:
                writer.writerow(state.model_dump())

    def to_jsonl(self, jsonl_file: str) -> Any:
        if self.verbose_transduction:
            logger.debug(f"Exporting {len(self.states)} Agentics to CSV {jsonl_file}")
        with open(jsonl_file, mode="w", newline="", encoding="utf-8") as f:
            for state in self.states:
                try:
                    f.write(json.dumps(clean_for_json(state)) + "\n")
                except Exception as e:
                    logger.debug(f"⚠️ Failed to serialize state: {e}")
                    f.write(json.dumps(self.atype().model_dump()))

    def to_dataframe(self) -> DataFrame:
        """
        Converts the current Agentics states into a pandas DataFrame.

        Returns:
            DataFrame: A pandas DataFrame representing the current states.
        """
        data = [state.model_dump() for state in self.states]
        return pd.DataFrame(data)

    def pretty_print(self):
        output = ""
        for state in self.states:
            output += yaml.dump(state.model_dump(), sort_keys=False) + "\n"
        print(output)
        return output
