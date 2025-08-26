import asyncio
import json
import os
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


class PydanticTransducerVLLM:
    model_config = {"arbitrary_types_allowed": True}
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

    async def async_transduce(
        self,
        input: Union[str, list[Any]],
        logprobs: bool = False,
        n_samples: int = 1,
        **kwargs,
    ) -> list[list[BaseModel]]:

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


class PydanticTransducerCrewAI:
    crew: Crew
    llm: Any
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
            max_iter=3,
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

    async def __kickoff_with_index(self, state, i):
        return (await self.crew.kickoff_async(state), i, state)

    async def async_transduce(self, input):
        if isinstance(input, str):
            answer = self.crew.kickoff_async({"task_description": input})
            ans = await answer
            return ans.pydantic
        elif isinstance(input, Iterable) and all(isinstance(i, str) for i in input):
            input_states = [
                {"task_description": x[:self.MAX_CHAR_PROMPT]} for x in input
            ]
            answer_list = await self.crew.kickoff_for_each_async(input_states)

            return [x.pydantic for x in answer_list]
        else:
            return NotImplemented

    def transduce(self, input):
        answer = self.crew.kickoff({"task_description": input})
        return answer.pydantic


import asyncio

if __name__ == "__main__":

    class PersonalInformation(BaseModel):
        first_name: Optional[str] = None
        last_name: Optional[str] = None
        year_of_birth: Optional[int] = None
        nationality: Optional[str] = None

    pt = PydanticTransducerCrewAI(PersonalInformation, llm=None)
    print(
        asyncio.run(
            pt.async_transduce(
                [
                    """Hi , I am John. My father is Dave Smith. 
                                I was born in April 1977, in British Columbia """,
                    """Hi , I am John. My father is Dave Smith. 
                                I was born in April 1977, in British Columbia """,
                ]
                * 5
            )
        )
    )
    # from agentics.core.llm_connections import vllm_llm
    # pt = PydanticTransducerVLLM(atype=PersonalInformation, llm=vllm_llm)
    # print(
    #     asyncio.run(
    #         pt.async_transduce(
    #             [
    #                 """Hi , I am John. My father is Dave Smith.
    #                                I was born in April 1977, in British Columbia """,
    #                 """Hi , I am John. My father is Dave Smith.
    #                                I was born in April 1977, in British Columbia """,
    #             ]
    #             * 10
    #         )
    #     )
    # )
