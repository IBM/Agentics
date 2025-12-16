import types
from typing import Any, Callable, Optional

from dotenv import load_dotenv
from pydantic import BaseModel

from agentics.core.agentics import AG
from agentics.core.utils import (
    import_last_function_from_code,
    import_pydantic_from_code,
)

load_dotenv()
import functools
import inspect
import logging
from typing import Any, Callable, Tuple, get_args

from agentics.core.default_types import GeneratedAtype
from agentics.core.utils import get_function_io_types, percent_non_empty_fields

logging.getLogger("huggingface_hub.utils._http").setLevel(logging.ERROR)


class Transduce:
    object: BaseModel

    def __init__(self, object: BaseModel | list[BaseModel]):
        self.object = object


class TransductionResult:
    def __init__(self, value, explanation):
        self.value = value
        self.explanation = explanation

    def __iter__(self):
        yield self.value
        yield self.explanation

    def __len__(self):
        return 2

    def __repr__(self):
        return f"TransductionResult(value={self.value}, explanation={self.explanation})"


# ============================================================
# ★ PATCH — UNIVERSAL UNPACK LOGIC
# ============================================================


def _unpack_if_needed(result):
    """
    Handles unpacking for:
      - TransductionResult
      - list[TransductionResult]
      - passthrough for values

    Caller decides by using 1 or 2 variables.
    """
    import inspect

    frame = inspect.currentframe().f_back
    lhs = frame.f_code.co_names
    want_two = len(lhs) >= 2

    # Single result
    if isinstance(result, TransductionResult):
        return (result.value, result.explanation) if want_two else result.value

    # Batch results
    if isinstance(result, list) and all(
        isinstance(x, TransductionResult) for x in result
    ):
        if want_two:
            return ([x.value for x in result], [x.explanation for x in result])
        else:
            return [x.value for x in result]

    return result


# ============================================================


def _wrap_composed(fn):
    """
    Ensures that ANY composed function behaves like a transducible function.
    """
    fn.__lshift__ = types.MethodType(_function_lshift, fn)
    return fn


def transducible(
    *,
    areduce: bool = False,
    tools: list[Any] | None = [],
    enforce_output_type: bool = False,
    llm: Any = AG.get_llm_provider(),
    reasoning: bool = False,
    max_iter: int = 10,
    verbose_transduction: bool = True,
    verbose_agent: bool = False,
    batch_size: int = 10,
    provide_explanation: bool = False,
    timeout: int = 300,
    post_processing_function: Optional[Callable[[BaseModel], BaseModel]] = None,
    persist_output: str = None,
):
    if tools is None:
        tools = []

    def _transducible(fn: Callable):

        # 1) infer IO types
        input_types, TargetModel = get_function_io_types(fn)
        if len(input_types) != 1:
            raise TypeError("Transducible functions must contain exactly one argument")

        if not inspect.iscoroutinefunction(fn):
            raise SystemError("Transducible functions must be async")

        if areduce:
            input_type = list(input_types.values())[0]
            SourceModel = get_args(input_type)[0]
        else:
            SourceModel = list(input_types.values())[0]

        # Template AGs
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
            transduction_timeout=timeout,
            save_amap_batches_to_path=persist_output,
            provide_explanations=provide_explanation,
        )
        source_ag_template = AG(
            atype=SourceModel,
            amap_batch_size=batch_size,
            transduction_timeout=timeout,
            save_amap_batches_to_path=persist_output,
        )

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

        # ----------------------------------------------------
        # wrap_single
        # ----------------------------------------------------
        @functools.wraps(fn)
        async def wrap_single(input_obj):

            if areduce:
                pre = await fn(input_obj)
                if isinstance(pre, TargetModel):
                    return pre

                elif isinstance(pre, Transduce) and isinstance(pre.object, list):
                    source_ag = source_ag_template.clone()
                    source_ag.states = pre.object
                    target_ag_template.transduction_type = "areduce"

                    target_ag = await (target_ag_template << source_ag)
                    return target_ag.states[0]

            else:
                pre = await fn(input_obj)

                # Trigger LLM transduction
                if isinstance(pre, Transduce) and isinstance(pre.object, SourceModel):
                    source_ag = source_ag_template.clone()
                    source_ag.states = [pre.object]

                    target_ag = await (target_ag_template << source_ag)

                    if len(target_ag) == 1:
                        out = target_ag[0]

                        if post_processing_function:
                            out = post_processing_function(out)

                        if provide_explanation and len(target_ag.explanations) == 1:
                            return TransductionResult(out, target_ag.explanations[0])

                        return out

                    raise RuntimeError("Transduction returned no output.")

                if enforce_output_type and not isinstance(
                    pre, target_ag_template.atype
                ):
                    raise TypeError(
                        f"Returned object {pre} not instance of {TargetModel.__name__}"
                    )

                if post_processing_function:
                    return post_processing_function(pre)
                else:
                    return pre

        # ----------------------------------------------------
        # wrapper
        # ----------------------------------------------------
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):

            if len(args) != 1 or kwargs:
                raise ValueError(
                    f"Function accepts only {SourceModel.__name__} or list[...]"
                )

            input = args[0]

            # REDUCE
            if areduce:
                if isinstance(input, AG):
                    input = input.states
                if not isinstance(input, list):
                    raise ValueError(
                        f"Function with areduce=True accepts only list[...]"
                    )
                source_ag = source_ag_template.clone()
                source_ag.states = [input]

                intermediate = await source_ag.amap(wrap_single)

                if len(intermediate) == 1:
                    return intermediate[0]

                if provide_explanation:
                    paired = [
                        TransductionResult(state, explanation)
                        for state, explanation in zip(
                            intermediate.states, intermediate.explanations
                        )
                    ]
                    return _unpack_if_needed(paired)

                return intermediate.states

            # AMAP
            if isinstance(input, (SourceModel, Transduce)):
                return await wrap_single(input)
            if isinstance(input, AG):
                input = input.states
            if isinstance(input, list):
                source_ag = source_ag_template.clone()
                source_ag.states = input
                intermediate = await source_ag.amap(wrap_single)

                # ★ PATCH: list can contain TransductionResult OR raw values
                return intermediate.states

            raise ValueError(
                f"Function accepts only {SourceModel.__name__}, Transduce, or list."
            )

        wrapper.input_model = SourceModel
        wrapper.target_model = TargetModel
        wrapper.description = fn.__doc__
        wrapper.tools = tools
        wrapper.__original_fn__ = fn

        wrapper.__lshift__ = types.MethodType(_function_lshift, wrapper)

        return _wrap_composed(wrapper)

    return _transducible


def wrap_as_transducible_function(fn: Callable, **kwargs):
    """
    Programmatically turn any async function into a transducible one
    by reusing the existing decorator machinery.
    """
    decorator_factory = transducible(**kwargs)
    wrapped_fn = decorator_factory(fn)
    return wrapped_fn


def make_transducible_function(
    *,
    InputModel: type[BaseModel] = None,
    OutputModel: type[BaseModel] = None,
    function_code: str = "",
    instructions: str = "",
    name: str = "",
    **kwargs,
):
    """
    Create a transducible function from InputModel → OutputModel
    using your existing transducible decorator.
    """
    if function_code:
        _auto_fn = import_last_function_from_code(function_code)

        _auto_fn.__doc__ = _auto_fn.__doc__ + instructions
        _auto_fn.__source__ = function_code

    elif InputModel and OutputModel:

        # If reduce, the input type must be list[InputModel]
        if kwargs.get("areduce", False):
            AnnotatedInput = list[InputModel]
        else:
            AnnotatedInput = InputModel

        async def _auto_fn(state: AnnotatedInput) -> OutputModel:
            """{instructions}"""
            return Transduce(state)

        _auto_fn.__name__ = (
            f"{InputModel.__name__}_to_{OutputModel.__name__}" if not name else name
        )
        _auto_fn.__annotations__ = {"state": AnnotatedInput, "return": OutputModel}
        _auto_fn.__doc__ = instructions

        # Delegate everything to your existing decorator
    return _wrap_composed(transducible(**kwargs)(_auto_fn))


class TransductionConfig:
    def __init__(self, model, **config):
        self.model = model  # a Pydantic model (Input)
        self.config = config  # extra arguments (instructions, tools, ...)


def With(model, **kwargs):
    return TransductionConfig(model, **kwargs)


from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass  # Pydantic v2


def _function_lshift(f, InputType):
    """
    f << X   =   composition
        CASES:
            X : instance         → run now
            X : Model            → build g:X→input(f) then return f∘g
            X : transducible fn  → compose f∘X
    """
    from agentics.core.transducible_functions import make_transducible_function

    # f must be a transducible function
    B = f.input_model
    C = f.target_model

    # --------------------------------------------
    # CASE 0: instance → run immediately
    # --------------------------------------------
    if isinstance(InputType, BaseModel):
        typed = f << type(InputType)
        return typed(InputType)

    # --------------------------------------------
    # CASE 1: f << Model
    # f: B→C,   want A→C
    # --------------------------------------------
    if isinstance(InputType, ModelMetaclass):
        A = InputType  # A→B transducer

        g = make_transducible_function(
            InputModel=A,
            OutputModel=B,
            instructions=f"Transduce {A.__name__} → {B.__name__}",
        )

        # build f∘g
        async def composed(a: A):
            b = await g(a)
            return await f(b)

        composed.__name__ = f"{C.__name__}_after_{B.__name__}_after_{A.__name__}"
        composed.input_model = A
        composed.target_model = C
        composed.__lshift__ = types.MethodType(_function_lshift, composed)
        return _wrap_composed(composed)

    # --------------------------------------------
    # CASE 2: f << g   (function << function)
    # f: B→C, g: A→B
    # --------------------------------------------
    if callable(InputType) and hasattr(InputType, "target_model"):
        g = InputType
        A = g.input_model
        B = g.target_model
        C = f.target_model

        async def composed(a: A):
            return await f(await g(a))

        composed.__name__ = f"{C.__name__}_after_{B.__name__}_after_{A.__name__}"
        composed.input_model = A
        composed.target_model = C
        composed.__lshift__ = types.MethodType(_function_lshift, composed)
        return _wrap_composed(composed)

    raise TypeError(f"Unsupported operand for function << : {InputType!r}")


def _model_lshift(OutputModel, InputType):
    """
    A << B:
        B is Model        → build B→A transducer
        B is instance     → call (A<<type(B))(B)
        B is function     → compose: B.input → A
        B is With(...)    → parameterized transduction
    """
    from agentics.core.transducible_functions import make_transducible_function

    # CASE: A << With(B, ...)
    if isinstance(InputType, TransductionConfig):
        M = InputType.model
        f = make_transducible_function(
            InputModel=M,
            OutputModel=OutputModel,
            **InputType.config,
        )
        f.__lshift__ = types.MethodType(_function_lshift, f)
        return _wrap_composed(f)

    # CASE: A << instance
    if isinstance(InputType, BaseModel):
        f = OutputModel << type(InputType)
        f.__lshift__ = types.MethodType(_function_lshift, f)
        return f(InputType)

    # CASE: A << Model (normal transductor)
    if isinstance(InputType, ModelMetaclass):
        # ★ ADD THIS

        f = make_transducible_function(
            InputModel=InputType,
            OutputModel=OutputModel,
            instructions=f"Transduce {InputType.__name__} → {OutputModel.__name__}",
        )
        # Ensure proper identity and composition
        f.input_model = InputType
        f.target_model = OutputModel
        f.__lshift__ = types.MethodType(_function_lshift, f)
        return _wrap_composed(f)

    # CASE: A << g   (compose OutputModel∘g)
    if callable(InputType) and hasattr(InputType, "input_model"):
        g = InputType
        A = g.input_model
        B = g.target_model
        Y = OutputModel  # B→Y transducer

        f = make_transducible_function(
            InputModel=B,
            OutputModel=Y,
            instructions=f"Transduce {B.__name__} → {Y.__name__}",
        )

        async def composed(x: A):
            mid = await g(x)
            return await f(mid)

        composed.__name__ = f"{Y.__name__}_after_{B.__name__}"
        composed.input_model = A
        composed.target_model = Y
        composed.__lshift__ = types.MethodType(_function_lshift, composed)
        return _wrap_composed(composed)

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
    generated = await (target << "   ")
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


async def generate_atype_from_description(
    description: str,
    retry: int = 3,
) -> GeneratedAtype | None:
    """
    Use Agentics to generate a Pydantic type from a natural language description.

    Returns:
        (generated_type, python_code) on success, or None if all retries fail.
    """

    i = 0
    while i < retry:
        generated_atype_ag = await (
            AG(
                atype=GeneratedAtype,
                instructions="""
Generate python code for the input natural-language type specification.

Requirements:
- Define exactly ONE Pydantic BaseModel.
- Make all fields Optional.
- Use only primitive types for the fields (str, int, float, bool, list[str], etc.).
- Avoid nested Pydantic models.
- Provide descriptions for the class and all its fields using:
    Field(None, description="...")
- If the input is a question, generate a Pydantic type that can represent
  the answer to that question.
Return ONLY valid Python V2 code in `python_code`.
""",
            )
            << description
        )

        if generated_atype_ag.states and generated_atype_ag[0]:
            generated_atype_ag[0].atype = import_pydantic_from_code(
                generated_atype_ag[0].python_code
            )
            return generated_atype_ag[0]
        i += 1

    return None
