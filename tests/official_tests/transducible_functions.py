import asyncio
from typing import Optional

from pydantic import BaseModel

from agentics.core.transducible_functions import Transduce, transducible


class GenericInput(BaseModel):
    content: Optional[str] = None


class Email(BaseModel):
    to: Optional[str] = None
    subject: Optional[str] = None
    body: Optional[str] = None


@transducible()
async def write_an_email(state: GenericInput) -> Email:
    """Write an email about the provided content. Elaborate on that and make up content as needed"""
    # example code to modify states before transduction
    return Transduce(state)


## Transducible functions can be introspected to easily get their input , output , description and original function
print(write_an_email.input_model)
print(write_an_email.target_model)
print(write_an_email.description)
print(write_an_email.__original_fn__)


single_mail = asyncio.run(
    write_an_email(
        GenericInput(
            content=f"Hi Lisa, I made great progress with the new release of Agentics 2.0"
        )
    )
)
print(single_mail.model_dump_json(indent=2))
