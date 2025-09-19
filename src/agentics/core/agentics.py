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
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import pandas as pd
import yaml
from crewai import LLM
from langchain_core.prompts import PromptTemplate
from loguru import logger
from pandas import DataFrame
from pydantic import BaseModel, Field, create_model

from agentics.abstractions.async_executor import (
    PydanticTransducerCrewAI,
    PydanticTransducerVLLM,
    aMap,
)
from agentics.core.errors import InvalidStateError
from agentics.core.llm_connections import available_llms, get_llm_provider
from agentics.core.mapping import AttributeMapping, ATypeMapping
from agentics.core.utils import (
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

# from agentics.core.globals import Memory

# Type variables
AG = TypeVar("AG", bound="AG")
T = TypeVar("T", bound="BaseModel")
StateReducer = Callable[[List[BaseModel]], BaseModel | List[BaseModel]]
StateOperator = Callable[[BaseModel], BaseModel]
StateFlag = Callable[[BaseModel], bool]


class AG(BaseModel, Generic[T]):
    """
    Agentics is a Python class that wraps a list of Pydantic objects and enables structured, type-driven logical transduction between them.

    Internally, Agentics is implemented as a Pydantic model. It holds:
        •	atype: a reference to the Pydantic class shared by all objects in the list.
        •	states: a list of Pydantic instances, each validated to be of type atype.
        •	tools: a list of tools (CrewAI or Langchain) to be used for transduction

    """

    atype: Type[BaseModel] = Field(
        BaseModel,
        description="""this is the type in common among all element of the list""",
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
    instructions: Optional[str] = Field(
        """Generate an object of the specified type from the following input.""",
        description="Special instructions to be given to the agent for executing transduction",
    )
    llm: Any = Field(default_factory=get_llm_provider, exclude=True)
    max_iter: int = Field(
        3,
        description="Max number of iterations for the agent to provide a final transduction when using tools.",
    )
    memory_collection: Optional[str] = None
    prompt_template: Optional[str] = Field(
        None,
        description="Langchain style prompt pattern to be used when provided as an input for a transduction.  Refer to https://python.langchain.com/docs/concepts/prompt_templates/ ",
    )
    reasoning: Optional[bool] = None
    skip_intentional_definition: bool = Field(
        False,
        description="if True, don't compose intentional instruction for Crew Task",
    )
    states: List[BaseModel] = []
    tools: Optional[List[Any]] = Field(None, exclude=True)
    transduce_fields: Optional[List[str]] = Field(
        None,
        description="""this is the list of field that will be used for the transduction, both incoming and outcoming""",
    )
    transduction_logs_path: Optional[str] = Field(
        None,
        description="""If not null, the specified file will be created and used to save the intermediate results of transduction from each batch. The file will be updated in real time and can be used for monitoring""",
    )
    transduction_timeout: float | None = None
    verbose_transduction: bool = True
    verbose_agent: bool = False

    class Config:
        model_config = {"arbitrary_types_allowed": True}

    @staticmethod
    def create_crewai_llm(**kwargs):
        return LLM(**kwargs)

    @classmethod
    def get_llm_provider(
        cls, provider_name: str = "first"
    ) -> Union[LLM, dict[str, LLM]]:
        if provider_name == "first":
            return (
                next(iter(available_llms.values()), None)
                if len(available_llms) > 0
                else None
            )
        if provider_name == "list":
            return available_llms
        if provider_name in available_llms:
            return available_llms[provider_name]
        raise ValueError(f"Unknown provider: {provider_name}")

    def __iter__(self):
        """Iterates over the list of states"""
        return iter(self.states)

    def __len__(self):
        """Returns the number of states"""
        return len(self.states)

    def __call__(self, *fields, persist: Optional[Union[bool, List[str]]] = None) -> AG:
        """
        Returns a new agentic with the subtype of fields from self.

        Args:
            *fields (str): The fields used to create a new AG,
                these fields are used for transductions.
            persist (bool or list[str], optional): The created AG persists additional fields
                from self, but those additional fields are not updated by the transduction.
                - If persist is None, the AG atype only contains the fields
                - If persist is True, the AG type remains the same and all the fields from
                    self are persisted. Only the *fields are updated (similar to self-transduction)
                - If persist is a list of strings, a new AG is created that includes the *fields as
                    well as the persistent fields given. Only the *fields are updated
        """
        if persist and isinstance(persist, bool):
            new_ag = self.clone()
            new_ag.transduce_fields = list(fields)
            return new_ag
        elif isinstance(persist, Iterable) and all(isinstance(i, str) for i in persist):
            all_fields = fields + tuple(persist)
        else:
            all_fields = fields
        atype = self.subset_atype(all_fields)
        new_ag = self.rebind_atype(atype, {f: f for f in all_fields})
        new_ag.transduce_fields = list(fields)
        return new_ag

    def __getitem__(self, index: int):
        """Returns the state for the provided index"""
        return self.states[index]

    def filter(self, func: StateFlag) -> AG:
        """func should be a function that takes as an input a state and return a boolean, false will be filtered out"""
        self.states = [state for state in self.states if func(state)]
        return self

    async def amap(self, func: StateOperator, timeout=None) -> AG:
        """Asynchronous map with exception-safe job gathering"""
        if self.verbose_transduction:
            logger.debug(f"Executing amap on function {func}")

        mapper = aMap(func=func, timeout=timeout)
        begin_time = time.time()
        try:
            tasks = [mapper.execute(state) for state in self.states]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            if self.transduction_logs_path:
                with open(self.transduction_logs_path, "a") as f:
                    for state in results:
                        f.write(state.model_dump_json() + "\n")

        except Exception as e:
            if self.verbose_transduction:
                logger.debug(str(e))
            results = self.states

        end_time = time.time()
        if self.verbose_transduction:
            logger.debug(
                f"{len(self.states)} states processed. {(end_time - begin_time) / len(self.states): 0.4f} seconds average per state"
            )

        _states = []
        n_errors = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                if self.verbose_transduction:
                    logger.debug(f"⚠️ Error processing state {i}: {result}")
                _states.append(self[i])
                n_errors += 1
            else:
                _states.append(result)
        if self.verbose_transduction:
            if n_errors:
                logger.debug(f"ERROR, {n_errors} states have not been transduced")

        self.states = _states
        return self

    async def areduce(self, func: StateReducer) -> AG:
        output = await func(self.states)
        self.states = [output] if isinstance(output, BaseModel) else output
        return self

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
                return AG(atype=atype, states=states)
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
    def from_jsonl(
        cls,
        path_to_json_file: str,
        atype: Optional[Type[BaseModel]] = None,
        max_rows: Optional[int] = None,
        jsonl: bool = True,
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
        self, source_target_pairs: List[Tuple[str, str]], first_n: Optional[int] = None
    ) -> AG:
        """for each state, copy fields values from ground truth to target attributes
        to be used as fewshot during transduction
        """
        for src, target in source_target_pairs:
            func = partial(
                AG.copy_attribute_values,
                source_attribute=src,
                target_attribute=target,
            )
            await self.apply(func, first_n=first_n)
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
        )  # gather input prompts for transduction by dumping input states
        target_type = (
            self.subset_atype(self.transduce_fields)
            if self.transduce_fields
            else self.atype
        )
        if isinstance(other, AG):
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

        elif isinstance(other, str) or (
            isinstance(other, Iterable) and all(isinstance(i, str) for i in other)
        ):
            if isinstance(other, str):
                other = [other]
            if self.verbose_transduction:
                logger.debug(
                    f"Transduction from input texts {other} to {type(target_type)} in progress. This might take a while"
                )
            input_prompts = ["\nSOURCE:\n" + x for x in other]
        else:
            return NotImplemented

        # expand prompts with relevant knowledge from memory
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
        if self.skip_intentional_definition:
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

        # Perform Transduction
        transducer_class = (
            PydanticTransducerCrewAI
            if type(self.llm) == LLM
            else PydanticTransducerVLLM
        )
        if self.verbose_transduction:
            logger.debug(f"transducer class: {transducer_class}")
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
                intentional_definiton=instructions,
                verbose=self.verbose_agent,
                max_iter=self.max_iter,
                timeout=self.timeout,
                reasoning=self.reasoning,
                **self.crew_prompt_params,
            )
            transduced_results = await pt.execute(*input_prompts)
            end_time = time.time()
            if self.verbose_transduction:
                logger.debug(
                    f"Processed {len(input_prompts)} states in {end_time - begin_time:0.4f} seconds"
                )
        except Exception as e:
            if self.verbose_transduction:
                logger.debug(str(e))
            transduced_results = self.states

        n_errors = 0
        output_states = []
        for i, result in enumerate(transduced_results):
            if isinstance(result, Exception):
                if self.verbose_transduction:
                    logger.debug(f"⚠️ Error processing state {i}: {result}")
                output_states.append(
                    self.states[i] if i < len(self.states) else target_type()
                )
                n_errors += 1
            else:
                output_states.append(result)
        if self.verbose_transduction:
            if n_errors:
                logger.debug(f"ERROR, {n_errors} states have not been transduced")

        if self.transduction_logs_path:
            with open(self.transduction_logs_path, "a") as f:
                for state in output_states:
                    if state:
                        f.write(state.model_dump_json() + "\n")
                    else:
                        f.write(self.atype().model_dump_json() + "\n")

        if isinstance(other, AG):
            for i in range(len(other.states)):
                output_state = output_states[i]
                if isinstance(output_state, tuple):
                    output_state_dict = dict([output_state])
                else:
                    output_state_dict = output_state.model_dump()

                merged = self.atype(
                    **(self[i].model_dump() | other[i].model_dump() | output_state_dict)
                )
                output.states.append(merged)
        elif isinstance(other, Iterable) and all(isinstance(i, str) for i in other):
            for i in range(len(other)):
                if isinstance(output_states[i], self.atype):
                    output.states.append(self.atype(**output_states[i].model_dump()))
                elif output_states[i]:
                    output.states.append(self.atype(**output_states[i][0].model_dump()))
                else:
                    output.states.append(self.atype())
        else:
            ValueError(f"<< expected as Agentic or List of str, received {type(other)}")
        return output

    async def apply(self, func: StateOperator, first_n: Optional[int] = None) -> AG:
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

        return reduce((lambda x, y: AG.add_states(x, y)), extended_ags)

    def quotient(self, other: AG) -> List[AG]:
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
        return AG(
            atype=first.atype, tools=first.tools, states=first.states + other.states
        )

    def __add__(self, other):
        if isinstance(other, AG):
            return AG(
                atype=self.atype, tools=self.tools, states=self.states + other.states
            )
        return NotImplemented

    async def map_atypes(self, other: AG) -> ATypeMapping:
        if self.verbose_agent:
            logger.debug(f"Mapping type {other.atype} into type {self.atype}")

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

        mappings = AG(atype=AttributeMapping)
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
        mappings = AG(atype=ATypeMapping, transduce_fields=["attribute_mappings"])
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
        target.instructions = instructions or target.instructions
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

    @property
    def fields(self) -> List[str]:
        """Returns the list of atype model fields"""
        return list(self.atype.model_fields)

    def pretty_print(self):
        output = f"Atype : {self.atype}\n"
        for state in self.states:
            output += yaml.dump(state.model_dump(), sort_keys=False) + "\n"
        return output

    def append(self, state: BaseModel):
        """Append the state into the list of states"""
        self.states.append(state)

    @property
    def timeout(self):
        return self.transduction_timeout

    @timeout.setter
    def timeout(self, value: float):
        self.transduction_timeout = value
