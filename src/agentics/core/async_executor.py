import asyncio
import os
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, Callable, List, Type, Union

from crewai import Agent, Crew, Process, Task
from dotenv import load_dotenv
from loguru import logger
from openai import AsyncOpenAI
from pydantic import BaseModel

from agentics.core.llm_connections import watsonx_llm
from agentics.core.utils import async_odered_progress, openai_response

load_dotenv()


class AsyncExecutor(ABC):

    wait: int = 0.01
    max_retries: int = 2
    timeout: int | None = None
    _retry: int = 0

    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, **kwargs):
        [setattr(self, name, value) for name, value in kwargs.items()]

    async def execute(
        self,
        *inputs: Union[Any],
        description: str = "Executing",
        transient_pbar: bool = False,
    ) -> Union[Any, Iterable[Any]]:
        """
        Execute over one or many inputs.
        Now also supports: execute([input1, input2, ...])
        """
        # -------------------------------------------------
        # 0. normalize inputs
        # -------------------------------------------------
        if len(inputs) == 1 and isinstance(inputs[0], (list, tuple)):
            flat_inputs: List[Any] = list(inputs[0])
        else:
            flat_inputs = list(inputs)

        _inputs: List[Any] = []
        _indices: List[int] = []

        # -------------------------------------------------
        # 1. single input → single await
        # -------------------------------------------------
        if len(flat_inputs) == 1:
            try:
                return await asyncio.wait_for(
                    self._execute(flat_inputs[0]), timeout=self.timeout
                )
            except Exception as e:
                # prepare for retry
                if self._retry < self.max_retries:
                    _indices = [0]
                    _inputs = [flat_inputs[0]]
                answers = [e]
        else:
            # -------------------------------------------------
            # 2. multiple inputs → gather all
            # -------------------------------------------------
            answers = await async_odered_progress(
                flat_inputs,
                self._execute,
                description=description,
                timeout=self.timeout,
                transient_pbar=transient_pbar,
            )

            # collect the ones that failed
            for i, answer in enumerate(answers):
                if isinstance(answer, Exception) and self._retry < self.max_retries:
                    _inputs.append(flat_inputs[i])
                    _indices.append(i)

        # -------------------------------------------------
        # 3. retry logic
        # -------------------------------------------------
        self._retry += 1
        if _inputs:
            # retry only failed ones
            _answers = await self.execute(
                _inputs,  # note: pass as single list so it normalizes again
                description=f"Retrying {len(_inputs)} state(s), attempt {self._retry}",
                transient_pbar=True,
            )
            # if a single retry result came back, make it a list
            if not isinstance(_answers, list):
                _answers = [_answers]
            for i, answer in zip(_indices, _answers):
                answers[i] = answer

        # reset for next call
        self._retry = 0
        return answers

    @abstractmethod
    async def _execute(self, input: Union[BaseModel, str], **kwargs) -> BaseModel:
        pass


import inspect


class aMap(AsyncExecutor):
    func: Callable

    def __init__(self, func: Callable, **kwargs):
        self.func = func
        super().__init__(**kwargs)

    async def _execute(self, state: BaseModel | dict, **kwargs):
        """
        Function transduction (amap): call the underlying func with the fields
        of `state` as keyword arguments.
        """
        # 1. turn state into a plain dict
        if isinstance(state, BaseModel):
            payload = state.model_dump()
        elif isinstance(state, dict):
            payload = state
        else:
            raise TypeError(f"state must be a BaseModel or dict, got {type(state)}")

        # 2. call the mapped function with **payload
        if inspect.iscoroutinefunction(self.func):
            output = await self.func(**payload, **kwargs)
        else:
            output = self.func(**payload, **kwargs)

        return output


class PydanticTransducer(AsyncExecutor):

    async def execute(self, *inputs: str, **kwargs) -> List[BaseModel]:
        """Pydantic transduction always returns a list of pydantic models"""
        output = await super().execute(*inputs, **kwargs)
        if not isinstance(output, list):
            output = [output]
        return output

    @abstractmethod
    async def _execute(self, input: str) -> BaseModel:
        pass


from typing import Optional

import mellea
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field

from agentics.core.utils import extract_json_objects


class PydanticTransducerMellea(PydanticTransducer):
    llm: AsyncOpenAI
    intentional_definiton: str
    verbose: bool = False
    MAX_CHAR_PROMPT: int = 15000

    def __init__(
        self,
        atype: Type[BaseModel],
        verbose: bool = False,
        llm=None,
        tools=None,
        intentional_definiton=None,
        timeout=10000,
        **kwargs,
    ):
        self.atype = atype
        self.verbose = verbose
        self.llm = llm
        self.tools = tools
        self.timeout = timeout
        self.intentional_definiton = (
            intentional_definiton
            or "Generate an object of the specified Pydantic Type from the following input."
        )
        self.llm_params = {
            "extra_body": {"guided_json": self.atype.model_json_schema()},
            "logprobs": False,
            "n": 1,
        }
        self.llm_params.update(kwargs)

    async def execute(
        self,
        input: Union[str, Iterable[str]],
        logprobs: bool = False,
        n_samples: int = 1,
        **kwargs,
    ) -> Union[BaseModel, Iterable[BaseModel]]:

        parser = PydanticOutputParser(pydantic_object=self.atype)

        prompt = PromptTemplate(
            template="Transduce the SOURCE object into a target JSON object matching this schema: {format_instructions}\n\nSOURCE: {target}\n\nTARGET:",
            input_variables=["target"],
            partial_variables={"format_instructions": parser.get_format_instructions()},
        )

        if isinstance(input, str):
            formatted_prompt = prompt.format(target=input)
            m = mellea.start_session()
            result = m.chat(formatted_prompt)
            res = m.chat(formatted_prompt)
            result = extract_json_objects(res.content, expected_type=self.atype)

            return result

        elif isinstance(input, Iterable) and all(isinstance(i, str) for i in input):
            processes = []
            for state in input:
                corutine = openai_response(
                    model=os.getenv("VLLM_MODEL_ID"),
                    base_url=os.getenv("VLLM_URL"),
                    user_prompt=default_user_prompt + str(state),
                    **self.llm_params,
                )
                processes.append(corutine)
            results = await asyncio.wait_for(
                asyncio.gather(*processes, return_exceptions=True), timeout=self.timeout
            )

            decoded_results = []
            for result in results:
                if issubclass(type(result), Exception):
                    if self.verbose:
                        logger.debug("Something went wrongs, generating empty states")
                    decoded_results.append(self.atype())
                else:
                    decoded_results.append(self.atype.model_validate_json(result))
            return decoded_results
        else:
            return NotImplemented


class PydanticTransducerVLLM(PydanticTransducer):
    llm: AsyncOpenAI
    intentional_definiton: str
    verbose: bool = False
    MAX_CHAR_PROMPT: int = 15000

    def __init__(
        self,
        atype: Type[BaseModel],
        verbose: bool = False,
        llm=None,
        tools=None,
        intentional_definiton=None,
        timeout=10000,
        **kwargs,
    ):
        self.atype = atype
        self.verbose = verbose
        self.llm = llm
        self.tools = tools
        self.timeout = timeout
        self.intentional_definiton = (
            intentional_definiton
            or "Generate an object of the specified Pydantic Type from the following input."
        )
        self.llm_params = {
            "extra_body": {"guided_json": self.atype.model_json_schema()},
            "logprobs": False,
            "n": 1,
        }
        self.llm_params.update(kwargs)

    async def execute(
        self,
        input: Union[str, Iterable[str]],
        logprobs: bool = False,
        n_samples: int = 1,
        **kwargs,
    ) -> Union[BaseModel, Iterable[BaseModel]]:

        default_user_prompt = "\n".join(
            [
                self.intentional_definiton,
                "Generate an object of the specified Pydantic Type from the following input.\n",
            ]
        )
        self.llm_params.update(kwargs)
        if isinstance(input, str):
            result = await openai_response(
                model=os.getenv("VLLM_MODEL_ID"),
                base_url=os.getenv("VLLM_URL"),
                user_prompt=default_user_prompt + str(state),
                **self.llm_params,
            )
            decoded_result = self.atype.model_validate_json(result)
            return decoded_result

        elif isinstance(input, Iterable) and all(isinstance(i, str) for i in input):
            processes = []
            for state in input:
                corutine = openai_response(
                    model=os.getenv("VLLM_MODEL_ID"),
                    base_url=os.getenv("VLLM_URL"),
                    user_prompt=default_user_prompt + str(state),
                    **self.llm_params,
                )
                processes.append(corutine)
            results = await asyncio.wait_for(
                asyncio.gather(*processes, return_exceptions=True), timeout=self.timeout
            )

            decoded_results = []
            for result in results:
                if issubclass(type(result), Exception):
                    if self.verbose:
                        logger.debug("Something went wrongs, generating empty states")
                    decoded_results.append(self.atype())
                else:
                    decoded_results.append(self.atype.model_validate_json(result))
            return decoded_results
        else:
            return NotImplemented


class PydanticTransducerCrewAI(PydanticTransducer):
    crew: Crew
    llm: Any
    intentional_definiton: str
    verbose: bool = False
    max_iter: int = 3
    MAX_CHAR_PROMPT: int = 15000

    def __init__(
        self,
        atype: Type[BaseModel],
        verbose: bool = False,
        llm=None,
        tools=None,
        intentional_definiton=None,
        max_iter=max_iter,
        timeout: float | None = 200,
        **kwargs,
    ):
        self.atype = atype
        self.llm = llm or watsonx_llm
        self.timeout = timeout
        self.intentional_definiton = (
            intentional_definiton
            or "Generate an object of the specified Pydantic Type from the following input."
        )
        self.prompt_params = {
            "role": "Task Executor",
            "goal": "You execute tasks",
            "backstory": "You are always faithful and provide only fact based answers.",
            "expected_output": "Described by Pydantic Type",
        }
        self.prompt_params.update(kwargs)
        agent = Agent(
            role=self.prompt_params["role"],
            goal=self.prompt_params["goal"],
            backstory=self.prompt_params["backstory"],
            verbose=verbose,
            max_iter=max_iter,
            llm=self.llm,
            tools=tools if tools else [],
        )
        task = Task(
            description=self.intentional_definiton + " {task_description}",
            expected_output=self.prompt_params["expected_output"],
            output_file="",
            agent=agent,
            output_pydantic=self.atype,
            tools=tools,
        )
        self.crew = Crew(
            agents=[agent],
            tasks=[task],
            process=Process.sequential,
            verbose=verbose,
            manager_llm=self.llm,
            function_calling_llm=self.llm,
            chat_llm=self.llm,
        )

    async def _execute(self, input: str) -> BaseModel:
        answer = await self.crew.kickoff_async(
            {"task_description": input[: self.MAX_CHAR_PROMPT]}
        )
        return answer.pydantic
