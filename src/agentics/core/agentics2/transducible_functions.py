import asyncio
import functools
import inspect
from typing import Any, Optional, get_type_hints

from dotenv import load_dotenv
from pydantic import BaseModel, create_model

from agentics import AG  # adjust if your import is agentics.core.agentics

load_dotenv()
import functools
import inspect
import logging
from typing import Any, Optional, get_args, get_origin, get_type_hints

from pydantic import BaseModel, Field, create_model

from agentics import AG  # adjust import if needed
from agentics.core.agentics2.utils import (
    get_function_io_types,
    has_explicit_return_none,
    pydantic_models_from_function,
    unwrap_optional,
)

logging.getLogger("huggingface_hub.utils._http").setLevel(logging.ERROR)


class ParameterWrapper:
    def __init__(self, parameters: dict[str, Any] | BaseModel):
        if isinstance(parameters, BaseModel) or isinstance(parameters, dict):
            self.parameters = parameters
        else:
            raise TypeError(
                f"Expected dict or BaseModel, got {type(parameters).__name__}"
            )

    def __repr__(self):
        return f"ParameterWrapper({self.parameters})"


class MapWrapper:
    def __init__(self, arguments: list[dict[str, Any] | ParameterWrapper | BaseModel]):
        if not isinstance(arguments, list):
            raise TypeError("arguments must be a list")

        # normalize: convert dicts → ParameterWrapper
        self.arguments: list[ParameterWrapper] = []
        for arg in arguments:
            if isinstance(arg, ParameterWrapper):
                self.arguments.append(arg)
            elif isinstance(arg, dict):
                self.arguments.append(ParameterWrapper(arg))
            elif isinstance(arg, BaseModel):
                self.arguments.append(ParameterWrapper(arg.model_dump()))
            else:
                raise TypeError(
                    f"Each item must be a dict or ParameterWrapper, got {type(arg).__name__}"
                )

    def __iter__(self):
        return iter(self.arguments)

    def __getitem__(self, idx: int) -> ParameterWrapper:
        return self.arguments[idx]

    def __len__(self):
        return len(self.arguments)

    def __repr__(self):
        return f"MapWrapper({self.arguments})"


class Transduce:
    object: BaseModel

    def __init__(self, object: BaseModel):
        self.object = object


def transducible(tools=[]):
    def _transducible(fn):
        """
        Decorator that transforms a python function into a transducible function.
        """
        input_types, TargetModel = get_function_io_types(fn)
        if len(input_types.keys()) != 1:
            raise TypeError("Transducible functions must contain only one argument")
        if not inspect.iscoroutinefunction(fn):
            raise SystemError("Transducible functions must be asynchronous")
        SourceModel = list(input_types.values())[0]
        if not (issubclass(SourceModel, BaseModel)) and not (
            issubclass(SourceModel, ParameterWrapper)
        ):
            raise TypeError("Source type should be a pydantic object")
        if not (issubclass(TargetModel, BaseModel)) and not (
            issubclass(TargetModel, ParameterWrapper)
        ):
            raise TypeError("Value type should be a pydantic object")
        # template target AG
        target_ag_template = AG(atype=TargetModel, tools=tools)
        source_ag_template = AG(atype=SourceModel)
        target_ag_template.instructions = f"""You are transducing the function {fn.__name__}.
    It takes objects of type {SourceModel.__name__} and transduces them into objects of type {TargetModel.__name__}.
    Follow the instruction below to perform the transduction.
    {fn.__doc__ or ""}"""

        async def wrap_single(*args, **kwargs):
            input = args[0]
            output = await fn(input)
            if isinstance(output, Transduce) or isinstance(output.object, SourceModel):
                source_ag = source_ag_template.clone()
                source_ag.states = [output.object]
                target_ag = await (target_ag_template << source_ag)

                if len(target_ag) == 1:
                    return target_ag[0]
                else:
                    raise Exception(
                        "Transduction returned no state output. This is a framework issue"
                    )
            elif isinstance(output, target_ag_template.atype):
                return output
            else:
                raise TypeError(
                    f"Returned output{output} is neither an instance of {target_ag_template.atype.__name__} nor a Transduce object"
                )

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            if len(args) != 1:
                raise ValueError(
                    f"Functions accepts only a single instance of {SourceModel.__name__}  or  list[{SourceModel.__name__}]  "
                )

            if isinstance(args[0], SourceModel) or isinstance(args[0], Transduce):
                return await wrap_single(*args, **kwargs)

            elif isinstance(args[0], list):
                ### MAP MULTIPLE INSTANCES
                source_ag = source_ag_template.clone()

                source_ag.states = args[0]

                intermediate_results = await source_ag.amap(wrap_single)

                return intermediate_results.states
            else:
                raise ValueError("Function accepts only ")

        wrapper.input_model = SourceModel
        wrapper.target_model = TargetModel
        wrapper.target_ag_template = target_ag_template
        return wrapper

    return _transducible
