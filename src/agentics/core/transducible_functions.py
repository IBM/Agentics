from typing import Any, Callable

from dotenv import load_dotenv
from pydantic import BaseModel

from agentics.core.agentics_2 import (
    AgenticsTransduction as AG,  # adjust if your import is agentics.core.agentics
)

load_dotenv()
import functools
import inspect
import logging

from agentics.core.utils_2 import get_function_io_types, percent_non_empty_fields

logging.getLogger("huggingface_hub.utils._http").setLevel(logging.ERROR)


class Transduce:
    object: BaseModel

    def __init__(self, object: BaseModel | list[BaseModel]):
        self.object = object


from typing import get_args


def transducible(
    *,
    areduce: bool = False,
    tools: list[Any] | None = [],
    enforce_output_type: bool = False,
    llm: Any = AG.get_llm_provider(),
    reasoning: bool = False,
    max_iter: int = 3,
    verbose_transduction: bool = True,
    verbose_agent: bool = False,
    batch_size: int = 10,
):
    """
    Usage:
        @transducible()
        async def f(state: MyInput) -> MyOutput: ...

        @transducible(tools=[...], enforce_output_type=True)
        async def g(state: MyInput) -> MyOutput: ...
    """
    if tools is None:
        tools = []

    def _transducible(fn: Callable):
        """
        Decorator that transforms a python function into a transducible function.
        """
        # 1) infer IO types from original function
        input_types, TargetModel = get_function_io_types(fn)

        if len(input_types) != 1:
            raise TypeError("Transducible functions must contain exactly one argument")

        if not inspect.iscoroutinefunction(fn):
            raise SystemError("Transducible functions must be asynchronous (async def)")

        if areduce:
            input_type = list(input_types.values())[0]

            SourceModel = get_args(input_type)[0]
        else:
            SourceModel = list(input_types.values())[0]

        source_objects_samples = []
        # 3) template AGs
        target_ag_template = AG(
            atype=TargetModel,
            transduction_type="areduce" if areduce else "amap",
            tools=tools,
            llm=llm,
            reasoning=reasoning,
            max_iter=max_iter,
            verbose_agent=verbose_agent,
            verbose_transduction=verbose_transduction,
            amap_batch_size=batch_size,
        )
        source_ag_template = AG(atype=SourceModel, amap_batch_size=batch_size)

        target_ag_template.instructions = f"""
===============================================
TASK : 
You are transducing the function {fn.__name__}.
Input Type: {SourceModel.__name__} 
Output Type: {TargetModel.__name__}.

INSTRUCTIONS:
{fn.__doc__ or ""}

===============================================
"""

        async def generate_prototypical_sources(
            n_instances=10, llm=AG.get_llm_provider()
        ):
            return await generate_prototypical_instances(
                SourceModel, n_instances=n_instances, llm=llm
            )

        @functools.wraps(fn)
        async def wrap_single(input_obj):
            """
            Handle a single SourceModel or Transduce(SourceModel → ???).
            """

            if areduce:
                output = await fn(input_obj)
                if isinstance(output, TargetModel):
                    return output
                elif isinstance(output, Transduce) and isinstance(output.object, list):
                    source_ag = source_ag_template.clone()
                    source_ag.states = output.object

                    target_ag = await (target_ag_template << source_ag)
                    return target_ag.states

            else:
                output = await fn(input_obj)
                # Case 1: user returns Transduce(SourceModel → needs LLM transduction)
                if isinstance(output, Transduce) and isinstance(
                    output.object, SourceModel
                ):
                    source_ag = source_ag_template.clone()
                    source_ag.states = [output.object]

                    target_ag = await (target_ag_template << source_ag)

                    if len(target_ag) == 1:
                        return target_ag[0]
                    raise RuntimeError(
                        "Transduction returned no state output. This is a framework issue."
                    )

                # Case 2: enforce type of direct output
                if enforce_output_type and not isinstance(
                    output, target_ag_template.atype
                ):
                    raise TypeError(
                        f"Returned object {output!r} is not an instance of "
                        f"{target_ag_template.atype.__name__}"
                    )

                # Case 3: just return what the function gave us
                return output

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            """
            Public entrypoint, supports:
              - f(SourceModel)
              - f(Transduce(...))
              - f([SourceModel, SourceModel, ...])
            """

            if len(args) != 1 or kwargs:
                raise ValueError(
                    f"Function accepts only a single positional argument: "
                    f"{SourceModel.__name__} or list[{SourceModel.__name__}]"
                )

            arg0 = args[0]

            if areduce:
                if isinstance(arg0, list):
                    source_ag = source_ag_template.clone()
                    source_ag.states = [arg0]
                    intermediate_results = await source_ag.amap(wrap_single)
                    if len(intermediate_results) == 1:
                        return intermediate_results[0]
                    else:
                        return intermediate_results.states

            else:

                # Single instance
                if isinstance(arg0, (SourceModel, Transduce)):
                    return await wrap_single(arg0)

                # List of instances → amap
                if isinstance(arg0, list):
                    source_ag = source_ag_template.clone()
                    source_ag.states = (
                        arg0  # assume list[SourceModel] or list[Transduce]
                    )

                    # wrap_single(input_obj) is our amap function
                    intermediate_results = await source_ag.amap(wrap_single)
                    return intermediate_results.states

            raise ValueError(
                f"Function accepts only {SourceModel.__name__}, "
                f"Transduce, or list of those."
            )

        # Expose some metadata for debugging/introspection
        wrapper.input_model = SourceModel
        wrapper.target_model = TargetModel
        wrapper.description = fn.__doc__
        wrapper.tools = tools
        wrapper.__original_fn__ = fn  # optional: makes introspection easier
        wrapper.generate_prototypical_sources = generate_prototypical_sources
        return wrapper

    return _transducible


def make_transducible_from_function(fn: Callable, **kwargs):
    """
    Programmatically turn any async function into a transducible one
    by reusing the existing decorator machinery.
    """
    decorator_factory = transducible(**kwargs)
    wrapped_fn = decorator_factory(fn)
    return wrapped_fn


import functools
import inspect
from typing import Any, Callable

from pydantic import BaseModel


def make_transducible_from_types(
    InputModel: type[BaseModel],
    OutputModel: type[BaseModel],
    *,
    instructions: str = "",
    **kwargs,
):
    """
    Create a transducible function from InputModel → OutputModel
    using your existing transducible decorator.
    """

    async def _auto_fn(state: InputModel) -> OutputModel:
        """{instructions}"""
        return Transduce(state)

    _auto_fn.__name__ = f"{InputModel.__name__}_to_{OutputModel.__name__}"
    _auto_fn.__annotations__ = {"state": InputModel, "return": OutputModel}
    _auto_fn.__doc__ = instructions

    # Delegate everything to your existing decorator
    return transducible(**kwargs)(_auto_fn)


class TransductionConfig:
    def __init__(self, model, **config):
        self.model = model  # a Pydantic model (Input)
        self.config = config  # extra arguments (instructions, tools, ...)


def With(model, **kwargs):
    return TransductionConfig(model, **kwargs)


from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass  # Pydantic v2


def _model_lshift(OutputModel, InputType):
    """
    Defines A << B semantics ("reverse arrow"):

        1. A << With(B, config...)         → B → A  (custom)
        2. A << B                          → B → A  (default)
        3. A << g (g: C → B)               → C → A  (composition)
        4. A << (B & C)                    → (B & C) → A  (merged types)
    """

    from agentics.core.transducible_functions import make_transducible_from_types

    # ---------------------------------------------------------
    # CASE 0 — A << With(B, ...)
    # ---------------------------------------------------------
    if isinstance(InputType, TransductionConfig):
        InputModel = InputType.model
        extra = InputType.config

        if not isinstance(InputModel, ModelMetaclass):
            raise TypeError("With(...) must wrap a Pydantic model class")

        return make_transducible_from_types(
            InputModel=InputModel,
            OutputModel=OutputModel,
            **extra,
        )

    # ---------------------------------------------------------
    # CASE 1 — A << B   (default model-to-model)
    # including merged models from A & B
    # ---------------------------------------------------------
    if isinstance(InputType, ModelMetaclass):
        # Simple transduction: B → A
        return make_transducible_from_types(
            InputModel=InputType,
            OutputModel=OutputModel,
            instructions=f"Transduce {InputType.__name__} → {OutputModel.__name__}",
        )

    # ---------------------------------------------------------
    # CASE 2 — A << g   (function composition)
    # where g is a transduction f: C → B
    # ---------------------------------------------------------
    if callable(InputType) and hasattr(InputType, "input_model"):
        g = InputType

        # g : InputModel -> MidModel
        InputModel = g.input_model
        MidModel = g.target_model
        OutModel = OutputModel

        # Build transducer: MidModel → OutModel
        f = make_transducible_from_types(
            InputModel=MidModel,
            OutputModel=OutModel,
            instructions=f"Transduce {MidModel.__name__} → {OutModel.__name__}",
        )

        # Compose into: InputModel → OutModel
        async def composed(x: InputModel):
            mid = await g(x)
            return await f(mid)

        composed.__name__ = f"{OutModel.__name__}_after_{MidModel.__name__}"
        composed.input_model = InputModel
        composed.target_model = OutModel
        return composed

    # ---------------------------------------------------------
    # FALLBACK
    # ---------------------------------------------------------
    raise TypeError(f"Unsupported operand for << : {InputType!r}")


# Patch the operator into Pydantic v2 models
ModelMetaclass.__lshift__ = _model_lshift


async def semantic_merge(instance1: BaseModel, instance2: BaseModel) -> BaseModel:
    Type1 = type(instance1)
    Type2 = type(instance2)
    MergedType = Type1 & Type2
    target = AG(
        atype=MergedType,
        instructions="Merge the two provided instances into an instance of the target type."
        "copy non null attributes verbatim if only one option is provided"
        "if different values for the same attribute are provided, try to derive one that represent the semantic average of the two options."
        "If missing value of the target merged type can be inferred, fill them otherwise leave blank ",
    )
    merged_instance = await (
        target << f"{instance1.model_dump_json()}\n{instance2.model_dump_json()} "
    )
    return merged_instance[0]


from typing import Type

from pydantic import BaseModel, create_model

from agentics import AG


async def generate_prototypical_instances(
    type: Type[BaseModel], n_instances: int = 10, llm: Any = AG.get_llm_provider()
) -> list[BaseModel]:
    DynamicModel = create_model(
        "ListOfObjectsOfGivenType", instances=(list[type], ...)  # REQUIRED field
    )

    target = AG(
        atype=DynamicModel,
        instructions=f"""
              Generate list of {n_instances} random instances of the following type 
              {type.model_json_schema()}. 
              Try to fill most of the attributed for each generated instance as possible
              """,
        llm=llm,
    )
    generated = await (target << "")
    return generated.states[0].instances


from typing import Any, Awaitable, Protocol


class TransducibleFn(Protocol):
    input_model: Any
    target_model: Any
    target_ag_template: Any
    __original_fn__: Any

    async def __call__(self, state: Any) -> Any: ...


async def estimateLogicalProximity(func, llm=AG.get_llm_provider()):
    sources = await generate_prototypical_instances(func.input_model, llm=llm)
    targets = await func(sources)
    total_lp = 0
    if len(targets) > 0:
        for target, source in zip(targets, sources):

            lp = percent_non_empty_fields(target)
            print(f" {target} <- {source} . LP: {lp}")
            total_lp += lp
        return total_lp / len(targets)
    else:
        return 0
