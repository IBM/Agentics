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

    def __str__(self) -> str:
        obj = self.object

        # List case
        if isinstance(obj, list):
            return "\n".join(self._one_to_str(x) for x in obj)

        # Single object
        return self._one_to_str(obj)

    def __repr__(self) -> str:
        return f"Transduce(object={self._one_to_str(self.object)})"

    @staticmethod
    def _one_to_str(x: Any) -> str:
        if isinstance(x, BaseModel):
            return x.model_dump_json(indent=2)
        if isinstance(x, (dict, list, tuple)):
            # readable generic fallback
            import json

            try:
                return json.dumps(x, indent=2, ensure_ascii=False, default=str)
            except TypeError:
                return str(x)
        return str(x)


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


class TransducibleFunction:
    """
    A callable wrapper around an async transducible function that supports
    the ``<<`` composition operator natively (class-level ``__lshift__``).

    Python's operator dispatch looks up dunder methods on the *class*, not on
    the instance, so attaching ``__lshift__`` as an instance attribute on a
    plain ``function`` object is silently ignored.  Wrapping in this class
    fixes that: ``f << g`` now works exactly like ``f.__lshift__(g)``.

    Attributes
    ----------
    schema_registry_url : str or None
        When set, the function is bound to a specific schema registry.
        ``transducible_function_listener`` will use this URL to validate
        incoming messages and produce results.
    """

    def __init__(
        self,
        fn,
        input_model,
        target_model,
        description=None,
        tools=None,
        original_fn=None,
        schema_registry_url: Optional[str] = None,
    ):
        self._fn = fn
        self.input_model = input_model
        self.target_model = target_model
        self.description = description
        self.tools = tools or []
        self.__original_fn__ = original_fn
        self.schema_registry_url = schema_registry_url
        # Preserve the wrapped function's identity
        functools.update_wrapper(self, fn)

    async def __call__(self, *args, **kwargs):
        return await self._fn(*args, **kwargs)

    def __lshift__(self, other):
        return _function_lshift(self, other)

    def __repr__(self):
        return (
            f"TransducibleFunction({self.__name__}: "
            f"{self.input_model.__name__} → {self.target_model.__name__})"
        )


def _wrap_composed(fn, input_model=None, target_model=None):
    """
    Ensures that ANY composed async function behaves like a transducible
    function by wrapping it in a ``TransducibleFunction`` instance.

    If ``fn`` is already a ``TransducibleFunction``, return it unchanged.
    """
    if isinstance(fn, TransducibleFunction):
        return fn
    im = input_model or getattr(fn, "input_model", None)
    tm = target_model or getattr(fn, "target_model", None)
    return TransducibleFunction(
        fn=fn,
        input_model=im,
        target_model=tm,
        description=getattr(fn, "description", None),
        tools=getattr(fn, "tools", []),
        original_fn=getattr(fn, "__original_fn__", None),
    )


def _registry_check_or_register(
    registry_url: str,
    source_model,
    target_model,
    auto_register: bool,
    fn_name: str,
) -> None:
    """
    Validate that *source_model* and *target_model* are registered in the
    schema registry at *registry_url*.

    If ``auto_register=True`` and a model is missing, it is registered
    automatically (using the model class name as the subject base, with the
    standard ``-value`` suffix).

    If ``auto_register=False`` and a model is missing, a ``RuntimeError``
    is raised immediately at decoration time so the developer is notified
    before any Kafka traffic occurs.

    Parameters
    ----------
    registry_url : str
        Base URL of the schema registry (e.g. ``http://localhost:8081``).
    source_model : type[BaseModel]
        The input Pydantic model class.
    target_model : type[BaseModel]
        The output Pydantic model class.
    auto_register : bool
        When ``True``, missing schemas are registered automatically.
    fn_name : str
        Name of the decorated function (used in error messages).
    """
    import json
    import sys

    import requests

    def _subject(model) -> str:
        return f"{model.__name__}-value"

    def _is_registered(subject: str) -> bool:
        try:
            url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions/latest"
            r = requests.get(url, timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def _register(model) -> None:
        subject = _subject(model)
        schema_json = model.model_json_schema()
        payload = {"schemaType": "JSON", "schema": json.dumps(schema_json)}
        url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions"
        try:
            r = requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                timeout=10,
            )
            if r.status_code in (200, 201):
                schema_id = r.json().get("id")
                sys.stderr.write(
                    f"✓ @transducible({fn_name}): auto-registered "
                    f"'{subject}' (schema ID {schema_id})\n"
                )
                sys.stderr.flush()
            else:
                raise RuntimeError(
                    f"@transducible({fn_name}): failed to auto-register "
                    f"'{subject}': HTTP {r.status_code} — {r.text}"
                )
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(
                f"@transducible({fn_name}): error auto-registering '{subject}': {exc}"
            ) from exc

    for model in (source_model, target_model):
        subject = _subject(model)
        if not _is_registered(subject):
            if auto_register:
                _register(model)
            else:
                raise RuntimeError(
                    f"@transducible({fn_name}): model '{model.__name__}' is not "
                    f"registered in the schema registry at {registry_url!r} "
                    f"(subject '{subject}' not found).  "
                    f"Either register it first or use auto_register=True."
                )
        else:
            sys.stderr.write(
                f"✓ @transducible({fn_name}): '{subject}' found in registry.\n"
            )
            sys.stderr.flush()


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
    transduce_fields: list[str] = None,
    prompt_template: str = None,
    schema_registry_url: Optional[str] = None,
    auto_register: bool = False,
):
    """
    Decorator that turns an async function into a transducible function.

    Parameters
    ----------
    schema_registry_url : str, optional
        URL of the Karapace / Confluent Schema Registry.  When provided the
        decorator will verify that both ``input_model`` and ``target_model``
        are already registered in the registry.  If they are not registered
        and ``auto_register=True`` the schemas are registered automatically.
        If they are not registered and ``auto_register=False`` a
        ``RuntimeError`` is raised at decoration time.
    auto_register : bool, default False
        When ``True`` (and ``schema_registry_url`` is set), automatically
        register the input and target Pydantic models in the schema registry
        if they are not already present.  Has no effect when
        ``schema_registry_url`` is ``None``.
    """
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
            prompt_template=prompt_template,
        )
        source_ag_template = AG(
            atype=SourceModel,
            amap_batch_size=batch_size,
            transduction_timeout=timeout,
            save_amap_batches_to_path=persist_output,
            transduce_fields=transduce_fields,
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

                        return TransductionResult(out, None)

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

        # ── Registry validation / auto-registration ──────────────────────
        if schema_registry_url is not None:
            _registry_check_or_register(
                registry_url=schema_registry_url,
                source_model=SourceModel,
                target_model=TargetModel,
                auto_register=auto_register,
                fn_name=fn.__name__,
            )

        return TransducibleFunction(
            fn=wrapper,
            input_model=SourceModel,
            target_model=TargetModel,
            description=fn.__doc__,
            tools=tools,
            original_fn=fn,
            schema_registry_url=schema_registry_url,
        )

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

        async def _auto_fn(state):
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
        async def _composed_model(a):
            b = await g(a)
            # Unwrap TransductionResult so f receives the plain model instance
            if isinstance(b, TransductionResult):
                b = b.value
            return await f(b)

        _composed_model.__name__ = f"{C.__name__}_after_{B.__name__}_after_{A.__name__}"
        return TransducibleFunction(
            fn=_composed_model,
            input_model=A,
            target_model=C,
        )

    # --------------------------------------------
    # CASE 2: f << g   (function << function)
    # f: B→C, g: A→B
    # --------------------------------------------
    if callable(InputType) and hasattr(InputType, "target_model"):
        g = InputType
        A = g.input_model
        B_inner = g.target_model
        C_inner = f.target_model

        async def _composed_fn(a):
            mid = await g(a)
            # Unwrap TransductionResult so f receives the plain model instance
            if isinstance(mid, TransductionResult):
                mid = mid.value
            return await f(mid)

        _composed_fn.__name__ = (
            f"{C_inner.__name__}_after_{B_inner.__name__}_after_{A.__name__}"
        )
        return TransducibleFunction(
            fn=_composed_fn,
            input_model=A,
            target_model=C_inner,
        )

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
        return make_transducible_function(
            InputModel=M,
            OutputModel=OutputModel,
            **InputType.config,
        )

    # CASE: A << instance
    if isinstance(InputType, BaseModel):
        f = OutputModel << type(InputType)
        return f(InputType)

    # CASE: A << Model (normal transductor)
    if isinstance(InputType, ModelMetaclass):
        return make_transducible_function(
            InputModel=InputType,
            OutputModel=OutputModel,
            instructions=f"Transduce {InputType.__name__} → {OutputModel.__name__}",
        )

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

        async def _composed_model_g(x):
            mid = await g(x)
            # Unwrap TransductionResult so f receives the plain model instance
            if isinstance(mid, TransductionResult):
                mid = mid.value
            return await f(mid)

        _composed_model_g.__name__ = f"{Y.__name__}_after_{B.__name__}"
        return TransducibleFunction(
            fn=_composed_model_g,
            input_model=A,
            target_model=Y,
        )

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
    type: Type[BaseModel],
    n_instances: int = 10,
    llm: Any = AG.get_llm_provider(),
    instructions: str = None,
) -> list[BaseModel]:

    DynamicModel = create_model(
        "ListOfObjectsOfGivenType",
        instances=(list[type] | None, None),  # REQUIRED field
    )
    full_instructions = f"""
              Generate list of {n_instances} random instances of the following type
              {type.model_json_schema()}.
              fill all attributed for each generated instance
              """
    if instructions:
        full_instructions += "Adhere to the following instructions \n" + instructions

    target = AG(
        atype=DynamicModel,
        instructions=full_instructions,
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
