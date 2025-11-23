from typing import Optional

from pydantic import BaseModel

from agentics.core.transducible_functions import Transduce


class GenericInput(BaseModel):
    content: Optional[str] = None


class Email(BaseModel):
    to: Optional[str] = None
    subject: Optional[str] = None
    body: Optional[str] = None


async def write_an_email(state: GenericInput) -> Email:
    """Write an email about the provided content. Elaborate on that and make up content as needed"""
    return Transduce(state)
