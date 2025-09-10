import asyncio
import json
import os
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, Optional, Type, Union

from crewai import Agent, Crew, Process, Task
from dotenv import load_dotenv
from loguru import logger
from openai import AsyncOpenAI
from pydantic import BaseModel

from agentics.core.llm_connections import watsonx_llm
from agentics.core.utils import openai_response

load_dotenv()


class Transducer(ABC):

    wait: int = 0.01
    max_retries = 5
    _retry: int = 0

    model_config = {"arbitrary_types_allowed": True}

    @abstractmethod
    async def transduce(
        self, input: Union[str, Iterable[str]], *args, **kwargs
    ) -> Union[BaseModel, Iterable[BaseModel]]:
        pass


class TransducerVLLM(Transducer):
    llm: AsyncOpenAI
    intensional_definiton: str
    verbose: bool = False
    MAX_CHAR_PROMPT: int = 15000

    def __init__(
        self,
        atype: Type[BaseModel],
        verbose: bool = False,
        llm=None,
        tools=None,
        intensional_definiton=None,
        **kwargs,
    ):
        self.atype = atype
        self.verbose = verbose
        self.llm = llm
        self.tools = tools
        self.intensional_definiton = (
            intensional_definiton
            if intensional_definiton
            else """Generate an object of the specified Pydantic Type from the following input."""
        )
        self.llm_params = {
            "extra_body": {"guided_json": self.atype.model_json_schema()},
            "logprobs": False,
            "n": 1,
        }
        self.llm_params.update(kwargs)

    async def transduce(
        self,
        input: Union[str, Iterable[str]],
        logprobs: bool = False,
        n_samples: int = 1,
        **kwargs,
    ) -> Union[BaseModel, Iterable[BaseModel]]:

        default_user_prompt = "\n".join(
            [
                self.intensional_definiton,
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
                asyncio.gather(*processes, return_exceptions=True), timeout=10000
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


class TransducerCrewAI(Transducer):
    crew: Crew
    llm: Any
    intensional_definiton: str
    verbose: bool = False
    max_iter: int = 3
    MAX_CHAR_PROMPT: int = 15000

    def __init__(
        self,
        atype: Type[BaseModel],
        verbose: bool = False,
        llm=None,
        tools=None,
        intensional_definiton=None,
        max_iter=max_iter,
        **kwargs,
    ):
        self.atype = atype
        self.llm = llm if llm else watsonx_llm
        self.intensional_definiton = (
            intensional_definiton
            if intensional_definiton
            else """Generate an object of the specified Pydantic Type from the following input."""
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
            description=self.intensional_definiton + " {task_description}",
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

    async def _transduce_with_retry(self, input: str):
        answer = await self.crew.kickoff_async(
            {"task_description": input[: self.MAX_CHAR_PROMPT]}
        )
        return answer.pydantic

    async def transduce(self, *inputs: str) -> Union[BaseModel, Iterable[BaseModel]]:
        self._retry += 1
        _inputs = []
        if len(inputs) == 1:
            try:
                answers = [await self._transduce_with_retry(inputs[0])]
            except Exception as e:
                _indices = 0
                _inputs = [inputs[0]] if isinstance(e, Exception) else []
        else:
            tasks = [asyncio.create_task(self._transduce_with_retry(i)) for i in inputs]
            answers = await asyncio.gather(*tasks, return_exceptions=True)
            _inputs = []
            _indices = []
            for i, task in enumerate(tasks):
                if task.exception() and self._retry <= self.max_retries:
                    _inputs.append(inputs[i])
                    _indices.append(i)
        if _inputs:
            logger.info(f"retrying {len(_inputs)} states")
            if self.verbose:
                logger.debug(f"retrying {len(_inputs)} states")
            asyncio.sleep(self.wait)
            _answers = await self.transduce(*_inputs)
            for i, answer in zip(_indices, _answers):
                answers[i] = answer

        self._retry = 0
        return answers
