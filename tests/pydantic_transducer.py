
import asyncio
from typing import Optional

from pydantic import BaseModel

from agentics.abstractions.pydantic_transducer import PydanticTransducerCrewAI

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
