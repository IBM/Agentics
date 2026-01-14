import csv
import io
import json
import logging
import os
import random
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
    get_args,
    get_origin,
    get_type_hints,
)

import pandas as pd
import yaml
from crewai import LLM
from crewai.llms.base_llm import BaseLLM
from langchain_core.prompts import PromptTemplate
from loguru import logger
from pandas import DataFrame
from pydantic import BaseModel, Field, ValidationError, create_model

from agentics.core.async_executor import (
    PydanticTransducerCrewAI,
    PydanticTransducerMellea,
    PydanticTransducerVLLM,
    aMap,
)
from agentics.core.atype import (
    copy_attribute_values,
    get_active_fields,
    get_pydantic_fields,
    make_all_fields_optional,
    pydantic_model_from_csv,
    pydantic_model_from_dataframe,
    pydantic_model_from_dict,
    pydantic_model_from_jsonl,
)
from agentics.core.default_types import (
    AmapError,
    AttributeMapping,
    ATypeMapping,
    Explanation,
    InvalidStateError,
    StateOperator,
    StateReducer,
)
from agentics.core.llm_connections import available_llms, get_llm_provider
from agentics.core.utils import (
    chunk_list,
    get_function_io_types,
    import_pydantic_from_code,
    is_str_or_list_of_str,
    merge_pydantic_models,
    sanitize_dict_keys,
)
from agentics.core.vector_store import VectorStore

logging.basicConfig(level=logging.ERROR)

AG = TypeVar("AG", bound="AG")
T = TypeVar("T", bound="BaseModel")


class AG(BaseModel, Generic[T]):
    """
    Agentics is a Python class that wraps a list of Pydantic objects and enables structured, type-driven logical transduction between them.

    Internally, Agentics is implemented as a Pydantic model. It holds:
        •	atype: a reference to the Pydantic class shared by all objects in the list.
        •	states: a list of Pydantic instances, each validated to be of type atype.
        •	tools: a list of tools (CrewAI or Langchain) to be used for transduction

    """

    atype: Type[BaseModel] = Field(
        None,
        description="""this is the type in common among all element of the list""",
    )
    atype_code: Type[BaseModel] = Field(
        None,
        description="""Python code for the used type""",
    )
    states: List[BaseModel] = []
    tools: Optional[List[Any]] = Field(None, exclude=True)
    transduce_fields: Optional[List[str]] = Field(
        None,
        description="""this is the list of field that will be used for the transduction, both incoming and outcoming""",
    )
    instructions: Optional[str] = Field(
        """Generate an object of the specified type from the following input.""",
        description="Special instructions to be given to the agent for executing transduction",
    )
    transduction_type: Optional[str] = Field(
        "amap",
        description="Type of transduction to be used, amap, areduce",
    )
    llm: Any = Field(default_factory=get_llm_provider, exclude=True)

    provide_explanations: bool = False
    explanations: Optional[list[Explanation]] = None
    reasoning: Optional[bool] = None
    max_iter: int = Field(
        3,
        description="Max number of iterations for the agent to provide a final transduction when using tools.",
    )

    transient_pbar: bool = False
    transduction_logs_path: Optional[str] = Field(
        None,
        description="""If not null, the specified file will be created and used to save the intermediate results of transduction from each batch. The file will be updated in real time and can be used for monitoring""",
    )
    prompt_template: Optional[str] = Field(
        None,
        description="Langchain style prompt pattern to be used when provided as an input for a transduction.  Refer to https://python.langchain.com/docs/concepts/prompt_templates/ ",
    )
    transduction_timeout: float | None = 300
    verbose_transduction: bool = True
    verbose_agent: bool = False
    areduce_batch_size: Optional[int] = Field(
        None,
        description="The size of the bathes to be used when transduction type is areduce",
    )
    amap_batch_size: Optional[int] = Field(
        20,
        description="The size of the bathes to be used when transduction type is amap",
    )
    areduce_batches: List[BaseModel] = []
    save_amap_batches_to_path: Optional[str] = None

    crew_prompt_params: Optional[Dict[str, str]] = Field(
        {
            "role": "Task Executor",
            "goal": "You execute tasks",
            "backstory": "You are always faithful and provide only fact based answers.",
            "expected_output": "Described by Pydantic Type",
        },
        description="prompt parameter for initializing Crew and Task",
    )
    vector_store: Optional[VectorStore] = None

    class Config:
        model_config = {"arbitrary_types_allowed": True}

    @property
    def __name__(self) -> str:
        """Returns the name of the atype"""
        return self.atype.__name__

    @property
    def fields(self) -> List[str]:
        """Returns the list of atype model fields"""
        return list(self.atype.model_fields)

    @property
    def timeout(self):
        return self.transduction_timeout

    @timeout.setter
    def timeout(self, value: float):
        self.transduction_timeout = value

    #################
    ##### LLMs  #####
    #################

    @staticmethod
    def create_crewai_llm(**kwargs):
        return LLM(**kwargs)

    async def generate_atype(
        self, description: str, retry: int = 3
    ) -> Tuple[str, Type[BaseModel]] | None:
        class GeneratedAtype(BaseModel):
            python_code: Optional[str] = Field(
                None, description="Python Code for the described Pydantic type"
            )
            methods: list[str] = Field(None, description="Methods for the class above")

        i = 0
        while i < retry:
            generated_atype_ag = await (
                AG(
                    atype=GeneratedAtype,
                    instructions="""Generate python code for the input nl type specs.
                Make all fields Optional. Use only primitive types for the fields, avoiding nested.
                Provide descriptions for the class and all its fields, using Field(None,description= "...")
                If the input nl type spec is a question, generate a pydantic type that can be used to
                represent the answer to that question.
                """,
                )
                << description
            )
            if len(generated_atype_ag.states) > 0 and generated_atype_ag[0].python_code:
                gen_type = import_pydantic_from_code(generated_atype_ag[0].python_code)
                if gen_type:
                    self.atype = gen_type
                    self.atype_code = generated_atype_ag[0].python_code
                    return self
                else:
                    i += 1
            else:
                i += 1
        return self

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

    ################################
    ##### List Functionalities #####
    ################################

    def __iter__(self):
        """Iterates over the list of states"""
        return iter(self.states)

    def __len__(self):
        """Returns the number of states"""
        return len(self.states)

    def __getitem__(self, index: int):
        """Returns the state for the provided index"""
        return self.states[index]

    def append(self, state: BaseModel):
        """Append the state into the list of states"""
        self.states.append(state)

    ################################
    ##### Agentics Utilities   #####
    ################################
    def clone(agentics_instance):
        copy_instance = copy(agentics_instance)
        copy_instance.states = deepcopy(agentics_instance.states)
        copy_instance.tools = agentics_instance.tools  # shallow copy, ok if immutable
        return copy_instance

    def filter_states(self, start: int = None, end: int = None) -> AG:
        new_self = self.clone()
        new_self.states = self.states[start:end]
        return new_self

    def set_default_value(self, field: str, default_value: Any = None) -> AG:
        new_self = self.clone()
        for state in self:
            if getattr(state, field):
                setattr(state, field, default_value)
            new_self.append(state)
        return new_self

    def get_random_sample(self, percent: float) -> AG:
        """An AG is returned with randomly selected states, given the percentage of samples to return."""
        if not (0 <= percent <= 1):
            raise ValueError("Percent must be between 0 and 1")

        sample_size = int(len(self.states) * percent)
        output = self.clone()
        output.states = random.sample(self.states, sample_size)
        return output

    ######################################
    ##### Validation Functionalities #####
    ######################################

    def validate(
        self, coerce: bool = False, return_error=False
    ) -> Union[bool, tuple[bool, list[str]]]:
        """
        Validate that all states in an Agentics object match its declared type.

        Args:
            ag: An Agentics AG instance with attributes:
                - atype: a Pydantic model class
                - states: a list of instances or dicts representing states
            coerce: If True, converts dicts or mismatched BaseModels into ag.atype instances.

        Returns:
            (ok, problems)
            ok: True if all states are valid (after optional coercion)
            problems: list of string messages describing validation errors
        """
        problems: list[str] = []

        # --- Structural sanity check ---
        if not hasattr(self, "atype") or not hasattr(self, "states"):
            raise TypeError(
                "Expected an Agentics object with `.atype` and `.states` attributes"
            )

        atype = self.atype
        if not isinstance(atype, type) or not issubclass(atype, BaseModel):
            raise TypeError("ag.atype must be a subclass of pydantic.BaseModel")

        # --- Validation loop ---
        for i, state in enumerate(self.states):
            try:
                # Case 1: already correct Pydantic model
                if isinstance(state, atype):
                    continue

                # Case 2: Coercion requested
                if coerce:
                    if isinstance(state, dict):
                        self.states[i] = atype.model_validate(state)
                    elif isinstance(state, BaseModel):
                        self.states[i] = atype.model_validate(state.model_dump())
                    else:
                        raise TypeError(
                            f"Unsupported state type: {type(state).__name__}"
                        )
                else:
                    # Validate only (without changing list)
                    if isinstance(state, dict):
                        atype.model_validate(state)
                    elif isinstance(state, BaseModel):
                        atype.model_validate(state.model_dump())
                    else:
                        raise TypeError(
                            f"Unsupported state type: {type(state).__name__}"
                        )

            except (ValidationError, TypeError) as e:
                problems.append(f"State {i}: invalid type or data — {e}")
        if return_error:
            if len(problems) == 0:
                return True, []
            else:
                return False, problems
        else:
            if len(problems) == 0:
                return True
            else:
                return False

    ######################################
    ##### aMapReduce Functionalities #####
    ######################################

    async def amap(self, func: StateOperator, timeout=None) -> AG:
        """Asynchronous map with exception-safe job gathering"""
        if not timeout:
            timeout = self.timeout
        mapper = aMap(func=func, timeout=timeout)
        hints = get_type_hints(func)
        SourceType, TargetType = get_function_io_types(func)

        if "state" in hints:
            state_t = hints["state"]
            origin = get_origin(state_t)

            # --- CASE 1: list[...] (reduce mode) ---
            if origin is list:
                (inner_type,) = get_args(state_t)
                if not issubclass(inner_type, self.atype):
                    raise AmapError(
                        f"The input type list[{inner_type}] of the provided function "
                        f"is not compatible with required atype {self.atype}"
                    )

            # --- CASE 2: single model ---
            else:
                if not issubclass(state_t, self.atype):
                    raise AmapError(
                        f"The input type {state_t} of the provided function "
                        f"is not a subclass of required atype {self.atype}"
                    )
        # --- RETURN TYPE CHECK ---
        if "return" in hints and issubclass(hints["return"], BaseModel):
            self.atype = TargetType
        batches = chunk_list(self.states, chunk_size=self.amap_batch_size)
        results = []
        if self.save_amap_batches_to_path:
            os.makedirs(os.path.dirname(self.save_amap_batches_to_path), exist_ok=True)
        for batch in batches:
            try:
                batch_results = await mapper.execute(
                    *batch, description=f"Executing amap on {func.__name__}"
                )

            except Exception:
                batch_results = self.states

            if isinstance(batch_results, list):
                results += batch_results
            else:
                results.append(batch_results)
            if self.save_amap_batches_to_path:
                with open(self.save_amap_batches_to_path, "a", encoding="utf-8") as f:
                    if isinstance(batch_results, list):
                        for batch_result in batch_results:
                            if isinstance(batch_result, BaseModel):
                                f.write(batch_result.model_dump_json())
                                f.write("\n")
                            elif isinstance(batch_result, dict):
                                f.write(json.dumps(batch_result))
                                f.write("\n")
                            else:
                                logger.debug(
                                    f"Error, instance {batch_result} is not a pydantic object"
                                )

                    elif isinstance(batch_results, BaseModel):
                        f.write(batch_results.model_dump_json())
                        f.write("\n")
                    elif isinstance(batch_results, dict):
                        f.write(json.dumps(batch_results))
                        f.write("\n")
                    else:
                        logger.debug(
                            f"Error, instance {batch_results} is not a pydantic object"
                        )

        _states = []
        n_errors = 0
        if not isinstance(results, list):
            results = [results]
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                if self.verbose_transduction:
                    logger.debug(
                        f"⚠️ AMAP generated an error processing state # {i}: {result}"
                    )
                if i < len(self.states):
                    _states.append(self.atype(**self.states[i].model_dump()))
                else:
                    _states.append(self.atype())
                    logger.debug(
                        f"⚠️ AMAP Big Error, this is a framework ISSUE , the generate instance number {i} doesn't have a corresponding source state: {result}"
                    )
                n_errors += 1
            else:
                _states.append(result)
        if self.verbose_transduction:
            if n_errors:
                logger.debug(f"Error, {n_errors} states have not been transduced")

        self.states = _states
        return self

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

    async def areduce(self, func: StateReducer) -> AG:
        output = await func(self.states)
        self.states = [output] if isinstance(output, BaseModel) else output
        return self

    ################################
    ##### Logical Transduction #####
    ################################

    async def __lshift__(self, other):
        """This is a transduction operation projecting a list of pydantic objects of into a target types
        Results are accumulated in the self instance and returned back as a result.
        Return None if the right operand is not of type AgenticList
        """
        from agentics.core.atype import AGString

        async def llm_call(input: AGString) -> AGString:
            input.string = self.llm.call(input.string)
            return input

        if not self.atype and isinstance(other, str):
            return self.llm.call(other)

        if not self.atype and is_str_or_list_of_str(other):
            if self.transduction_type == "amap":
                input_messages = AG(states=[AGString(string=x) for x in other])
                input_messages = await input_messages.amap(llm_call)
                return [x.string for x in input_messages.states]

        if self.transduction_type == "areduce":
            if other.transduce_fields is not None:
                new_other = other.subset_atype(other.transduce_fields)
            else:
                new_other = other
            if is_str_or_list_of_str(new_other):
                chunks = chunk_list(new_other, chunk_size=self.areduce_batch_size)
            else:
                chunks = chunk_list(
                    new_other.states, chunk_size=self.areduce_batch_size
                )
            self.transduction_type = "amap"
            ReducedOtherAtype = create_model(
                "ReducedOtherAtype",
                reduced_other_states=(list[new_other.atype] | None, Field([])),
            )

            reduced_other_ag = AG(
                atype=ReducedOtherAtype,
                states=[
                    ReducedOtherAtype(reduced_other_states=chunk) for chunk in chunks
                ],
            )

            self = await (self << reduced_other_ag)
            return self

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

        elif is_str_or_list_of_str(other):
            if isinstance(other, str):
                other = [other]
            input_prompts = ["\nSOURCE:\n" + x for x in other]
        elif isinstance(other, list):
            try:
                input_prompts = ["\nSOURCE:\n" + str(x) for x in other]
            except:
                return ValueError
        else:
            try:
                input_prompts = ["\nSOURCE:\n" + str(other)]
            except:
                return ValueError

        ## collect few shots, only when all target slots are non null TODO need to improve with some non null
        instructions = ""

        # Add instructions

        instructions += "\nYour task is to transduce a source Pydantic Object into the specified Output type. Generate only slots that are logically deduced from the input information, otherwise live then null.\n"
        if self.instructions:
            instructions += (
                "\nRead carefully the following instructions for executing your task:\n"
                + self.instructions
            )

        # Perform Transduction
        transducer_class = (
            PydanticTransducerCrewAI
            if isinstance(self.llm, BaseLLM)
            else PydanticTransducerMellea if type(self.llm) == str else None
        )
        if not transducer_class:
            raise TypeError(
                "Provided llm object is neither a crew ai llm nor a string (for mellea's llm)"
            )
        try:
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
            transduced_results = await pt.execute(
                *input_prompts,
                description=f"Transducing {self.__name__} << {'AG[str]' if not isinstance(other, AG) else other.__name__}",
                transient_pbar=self.transient_pbar,
            )
        except Exception as e:
            transduced_results = self.states

        n_errors = 0
        output_states = []
        for i, result in enumerate(transduced_results):
            if isinstance(result, Exception):
                output_states.append(
                    self.states[i] if i < len(self.states) else target_type()
                )
                n_errors += 1
            else:
                output_states.append(result)
        if self.verbose_transduction:
            if n_errors:
                logger.debug(f"Error: {n_errors} states have not been transduced")

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
                    **(
                        (self[i].model_dump() if len(self) > i else {})
                        | other[i].model_dump()
                        | output_state_dict
                    )
                )
                output.states.append(merged)
        # elif is_str_or_list_of_str(other):
        elif isinstance(other, list):
            for i in range(len(other)):
                if isinstance(output_states[i], self.atype):
                    output.states.append(self.atype(**output_states[i].model_dump()))
                else:
                    output.states.append(self.atype())
        else:
            if isinstance(output_states[0], self.atype):
                output.states.append(self.atype(**output_states[i].model_dump()))

        if self.provide_explanations and isinstance(other, AG):
            target_explanation = AG(atype=Explanation)
            target_explanation.instructions = f"""
            You have been presented with two Pydantic Objects:
            a left object that was logically derived from a right object.
            Your task is to provide a detailed explanation of how the left object was derived from the right object."""
            target_explanation = await (
                target_explanation << output.compose_states(other)
            )

            self.explanations = target_explanation.states
            self.states = output.states
            return self
        else:
            return output

    async def copy_fewshots_from_ground_truth(
        self, source_target_pairs: List[Tuple[str, str]], first_n: Optional[int] = None
    ) -> AG:
        """for each state, copy fields values from ground truth to target attributes
        to be used as fewshot during transduction
        """
        for src, target in source_target_pairs:
            func = partial(
                copy_attribute_values,
                source_attribute=src,
                target_attribute=target,
            )
            await self.apply(func, first_n=first_n)
        return self

    async def self_transduction(
        self,
        source_fields: List[str] | None = None,
        target_fields: List[str] | None = None,
        instructions: str = None,
    ):
        target = self.clone()
        # if not source_fields and not target_fields:
        #     return await self.amap(self._single_self_transduction)

        if not source_fields:
            self.transduce_fields = get_active_fields(self[0])
        else:
            self.transduce_fields = source_fields

        target.instructions = instructions or target.instructions
        if not target_fields:
            target.transduce_fields = list(
                {x["name"] for x in get_pydantic_fields(self.atype)}
                - get_active_fields(self[0])
            )
        else:
            target.transduce_fields = target_fields

        output_process = target << self
        output = await output_process
        return output

    ##################################
    ##### Import Functionalities #####
    ##################################

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
    ):
        """
        Import an Agentics (AG) from CSV.

        `csv_file` may be:
        - str/Path to a file
        - a text or binary stream (StringIO, BytesIO, file handle)
        - a Streamlit UploadedFile
        - a raw CSV string
        If `atype` is not provided, it is inferred from the header with
        `pydantic_model_from_csv` and all fields are optional strings.
        """

        def _to_text_stream(src) -> io.TextIOBase:
            # 1) Path on disk
            if isinstance(src, (str, os.PathLike)) and os.path.exists(src):
                # use utf-8-sig to gracefully handle a BOM if present
                return open(src, "r", encoding="utf-8-sig", newline="")

            # 2) Raw CSV string (heuristic: contains a newline or a comma)
            if isinstance(src, str) and ("\n" in src or "," in src):
                return io.StringIO(src)

            # 3) Streamlit UploadedFile (has getbuffer/getvalue)
            if hasattr(src, "getbuffer"):
                return io.StringIO(src.getbuffer().tobytes().decode("utf-8-sig"))
            if hasattr(src, "getvalue"):
                return io.StringIO(src.getvalue().decode("utf-8-sig"))

            # 4) Already a text stream
            if isinstance(src, io.TextIOBase):
                try:
                    src.seek(0)
                except Exception:
                    pass
                return src

            # 5) Binary stream -> wrap as text
            if isinstance(src, (io.BytesIO, io.BufferedIOBase, io.RawIOBase)):
                try:
                    src.seek(0)
                except Exception:
                    pass
                return io.TextIOWrapper(src, encoding="utf-8-sig", newline="")

            raise TypeError(f"Unsupported CSV input type: {type(src).__name__}")

        # Decide/Infer atype first (works with path/stream/string)
        if atype is not None:
            new_type = atype
            if "logger" in globals():
                logger.debug(
                    f"Importing Agentics of type {atype.__name__} from CSV {type(csv_file).__name__}"
                )
        else:
            # pydantic_model_from_csv already handles path/stream/string
            new_type = make_all_fields_optional(pydantic_model_from_csv(csv_file))

        # Normalize to a text stream for DictReader
        f = _to_text_stream(csv_file)
        close_after = isinstance(csv_file, (str, os.PathLike)) and os.path.exists(
            csv_file
        )

        try:
            states: List[BaseModel] = []
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("CSV appears to have no header row.")

            for c_row, row in enumerate(reader):
                if max_rows is not None and c_row >= max_rows:
                    break
                # Build the state using the inferred/provided Pydantic model
                state = new_type(**row)
                states.append(state)

        finally:
            # Only close if we opened a real file path
            if close_after:
                f.close()

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
        # FIX HERE: Normalize incoming DataFrame
        if hasattr(dataframe, "to_pandas"):
            dataframe = dataframe.to_pandas()

        for i, row in dataframe.iterrows():
            if max_rows and i >= max_rows:
                break
            state = new_type(**sanitize_dict_keys(row.to_dict()))
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

    ##################################
    ##### Export Functionalities #####
    ##################################

    def pretty_print(self):
        output = f"AG[{self.atype}]\n\n"
        for i, state in enumerate(self.states):
            output += f"state {i}:\n"
            output += yaml.dump(
                state.model_dump() if isinstance(state, BaseModel) else str(state),
                sort_keys=False,
            )
            if self.explanations and len(self.explanations) > i:
                output += (
                    "\nexplanations:\n"
                    + yaml.dump(self.explanations[i].model_dump())
                    + "\n"
                )
            output += "\n\n"
        print(output)
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

    def to_jsonl(self, jsonl_file: str, append: bool = False) -> Any:
        if self.verbose_transduction:
            logger.debug(
                f"Exporting {len(self.states)} states or atype {self.atype} to {jsonl_file}"
            )
        with open(
            jsonl_file, mode="w" if not append else "a", newline="", encoding="utf-8"
        ) as f:
            for state in self.states:
                try:
                    f.write(state.model_dump_json() + "\n")
                except Exception as e:
                    logger.debug(f"⚠️ Failed to serialize state: {e}")
                    f.write(json.dumps(self.atype().model_dump()) + "\n")

    def to_dataframe(self) -> DataFrame:
        """
        Converts the current Agentics states into a pandas DataFrame.

        Returns:
            DataFrame: A pandas DataFrame representing the current states.
        """
        data = [state.model_dump() for state in self.states]
        return pd.DataFrame(data)

    ########################################
    ##### aType Manipulation Functions #####
    ########################################

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
        prod_atype = create_model(f"{self.__name__}__{other.__name__}", **new_fields)

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

    def merge_states(self, other: AG) -> AG:
        """
        Merge states of two AGs pairwise

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

    def compose_states(self, other: AG) -> AG:
        """
        compose states of two AGs pairwise,

        """
        merged = self.clone()
        merged.states = []
        merged.explanations = []
        merged.atype = self.atype @ other.atype

        for self_state in self:
            for other_state in other:
                merged.states.append(merged.atype(right=other_state, left=self_state))
        return merged

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
        self, new_atype: Type[BaseModel], mapping: Dict[str, str] | None = None
    ):
        """
        Return a new AG whose `atype` is rebound to `new_atype`.

        Each state is converted into an instance of `new_atype`.
        - If `mapping` is provided, it remaps source field names to target names.
        - Only matching attributes are kept; extra fields are discarded.
        - If a state cannot be converted, it is skipped (with a warning).

        Args:
            new_atype: Target Pydantic model class.
            mapping: Optional dict mapping {old_key: new_key}.

        Returns:
            AG: a new Agentics object with states of type `new_atype`.
        """
        new_ag = deepcopy(self)
        new_ag.atype = new_atype
        new_ag.states = []

        for state in self.states:
            data = state.model_dump()

            if mapping:
                # keep only remapped keys
                data = {mapping.get(k, k): v for k, v in data.items() if k in mapping}

            try:
                new_state = new_atype(**data)
                new_ag.states.append(new_state)
            except ValidationError as e:
                # Skip or log; up to you how strict you want to be
                logger.warning("Failed to rebind state %s: %s", state, e)

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
        new_model = create_model(f"{self.__name__}_extended", **fields)

        # Optionally re-assign it to self.atype
        return self.rebind_atype(new_model)

    #####################################
    ##### Vector Store Capabilities #####
    #####################################
    def build_index(self):
        self.vector_store = VectorStore()
        logger.debug(
            f"Indexing AG. {len(self)} states will be indexed, this might take a while"
        )
        texts = [x.model_dump_json() for x in self]
        self.vector_store.import_data(texts)

    def search(self, query: str, k: int = 5) -> AG:
        filtered_ag = self.clone()
        if not self.vector_store or (
            self.vector_store and self.vector_store.store.next_id != len(self)
        ):
            self.build_index()
        filtered_ag.states = []
        results = self.vector_store.search(query, k=k)

        for result in results:
            filtered_ag.append(self.states[result[1]["id"]])
        return filtered_ag

    def cluster(self, n_partitions: int = None) -> list[AG]:
        if not self.vector_store:
            self.vector_store = VectorStore()

        if self.vector_store.store.next_id != len(self):
            self.build_index()
        if not n_partitions:
            n_partitions = int(len(self) / 10)
        logger.debug(
            f"Clustering AG containing {len(self)} states into {n_partitions} AGs. This might take a while ..."
        )
        clusters = self.vector_store.cluster(k=n_partitions)

        results = []
        cluster_ag = self.clone()
        cluster_ag.states = []
        for cluster in clusters:
            current_cluster_ag = cluster_ag.clone()

            for state in cluster:
                current_cluster_ag.append(self.states[state["id"]])
            results.append(current_cluster_ag)
        return results

    def filter_by_attribute_value(self, attribute, value):
        """
        Return a cloned AG containing only states where state.<attribute> == value.
        Works for both dict states and Pydantic states.
        """
        out = self.clone()
        out.states = []
        for state in self.states:
            if hasattr(state, attribute):
                if getattr(state, attribute) == value:
                    out.states.append(state)
                continue
        return out
