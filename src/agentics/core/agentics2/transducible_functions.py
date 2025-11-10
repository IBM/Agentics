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
from typing import Any, Optional, get_args, get_origin, get_type_hints

from pydantic import BaseModel, Field, create_model

from agentics import AG  # adjust import if needed
from agentics.core.agentics2.utils import unwrap_optional


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


def pydantic_models_from_function(fn):
    """
    Build:
    - InputModel:
        * if the function has exactly 1 param and it's already a Pydantic model,
          use that model directly
        * otherwise, build a synthetic Input model with one field per parameter
    - Target:
        * from return annotation, but ALL fields optional
    """
    sig = inspect.signature(fn)
    hints = get_type_hints(fn)

    # ----- figure out the input model -----
    params = list(sig.parameters.items())
    single_param_name = params[0][0] if len(params) == 1 else None
    single_param_ann = hints.get(single_param_name) if single_param_name else None

    if (
        len(params) == 1
        and isinstance(single_param_ann, type)
        and issubclass(single_param_ann, BaseModel)
    ):
        # user already said: def f(x: MyModel)
        InputModel = single_param_ann
    else:
        # build a composite pydantic model from all params
        input_fields = {}
        for name, param in sig.parameters.items():
            ann = hints.get(name, Any)
            if param.default is inspect._empty:
                input_fields[name] = (ann, ...)
            else:
                input_fields[name] = (ann, param.default)
        InputModel = create_model(f"{fn.__name__}Input", **input_fields)

    # ----- figure out the target model -----
    ret_ann = hints.get("return", Any)

    if isinstance(ret_ann, type) and issubclass(ret_ann, BaseModel):
        # clone as all-optional
        optional_fields = {
            f_name: (Optional[f_field.annotation], None)
            for f_name, f_field in ret_ann.model_fields.items()
        }
        Target = create_model(f"{fn.__name__}Target", **optional_fields)
    else:
        # single optional field called "result"
        Target = create_model(
            f"{fn.__name__}Target",
            result=(Optional[ret_ann], None),
        )

    return InputModel, Target


import ast
import inspect


def has_explicit_return_none(fn) -> bool:
    """
    Return True if the function has an explicit 'return None' or bare 'return'.
    """
    tree = ast.parse(inspect.getsource(fn))
    for node in ast.walk(tree):
        if isinstance(node, ast.Return):
            # bare `return`
            if node.value is None:
                return True
            # explicit `return None`
            if isinstance(node.value, ast.Constant) and node.value.value is None:
                return True
    return False


def transducible(fn):
    """
    Decorator that transforms a python function into a transducible function.
    """
    # build the models once, not at every call
    SourceModel, TargetModel = pydantic_models_from_function(fn)
    sig = inspect.signature(fn)
    is_fn_async = inspect.iscoroutinefunction(fn)

    # template target AG
    target_ag_template = AG(atype=TargetModel)
    source_ag_template = AG(atype=SourceModel)

    async def normalize_arguments(*args, **kwargs):
        """
        Normalize whatever we got (a SourceModel, a plain dict, plain kwargs, or
        positional args matching the function) into:
            input_obj: SourceModel
            call_kwargs: dict to actually call the user function with
        """
        # 1) source model passed directly
        if len(args) == 1 and isinstance(args[0], SourceModel) and not kwargs:
            input_obj = args[0]
            call_kwargs = {}
            return input_obj, call_kwargs

        # 2) single dict passed positionally: fn({"a": 2, "b": 3})
        #    (this happens a lot in your amap path)
        if len(args) == 1 and isinstance(args[0], dict) and not kwargs:
            data = args[0]
            # build SourceModel from keys it knows
            model_kwargs = {
                k: v for k, v in data.items() if k in SourceModel.model_fields
            }
            input_obj = SourceModel(**model_kwargs)
            # build call_kwargs only for actual fn params
            fn_params = sig.parameters
            call_kwargs = {k: v for k, v in data.items() if k in fn_params}
            return input_obj, call_kwargs

        # 3) pure kwargs: fn(a=2, b=3)
        if not args and kwargs:
            model_kwargs = {
                k: v for k, v in kwargs.items() if k in SourceModel.model_fields
            }
            input_obj = SourceModel(**model_kwargs)
            fn_params = sig.parameters
            call_kwargs = {k: v for k, v in kwargs.items() if k in fn_params}
            return input_obj, call_kwargs

        # 4) mixed / positional — try to bind, but first drop unknown kwargs
        if kwargs:
            fn_params = sig.parameters
            kwargs = {k: v for k, v in kwargs.items() if k in fn_params}

        try:
            bound = sig.bind(*args, **kwargs)
        except TypeError:
            # last-resort fallback: turn everything we can into the SourceModel
            all_kwargs = {}
            # positional args: we can’t safely map these to names here,
            # so we just give up and raise
            raise
        else:
            bound.apply_defaults()
            input_obj = SourceModel(**bound.arguments)
            call_kwargs = dict(bound.arguments)
            return input_obj, call_kwargs

    async def wrap_single(*args, **kwargs):

        input_obj, call_kwargs = await normalize_arguments(*args, **kwargs)

        # -------------------------------------------------------------
        # 2. call the original function (sync OR async)
        # -------------------------------------------------------------
        try:
            if is_fn_async:
                if input_obj:
                    user_result = await fn(input_obj, **call_kwargs)
                else:
                    user_result = await fn(**call_kwargs)
            else:
                if input_obj:
                    user_result = await fn(input_obj, **call_kwargs)
                else:
                    user_result = await fn(**call_kwargs)
        except:
            user_result = ParameterWrapper(input_obj)

        # -------------------------------------------------------------
        # 3. normalize what the user returned
        #    allowed:
        #    - TargetModel instance
        #    - bare value matching TargetModel.result type
        #    - dict to be merged into InputModel
        #    - None → just use the original input_obj
        # -------------------------------------------------------------
        result_field_type = None
        if "result" in TargetModel.model_fields:
            result_field_type = unwrap_optional(
                TargetModel.model_fields["result"].annotation
            )

        if (
            user_result is not None
            and isinstance(user_result, TargetModel)
            and not TargetModel.__name__.endswith("Target")
        ):
            # user already did the job — we can just return it
            return user_result
        elif (
            user_result is not None
            and isinstance(user_result, TargetModel)
            and TargetModel.__name__.endswith("Target")
        ):
            return user_result

        # bare value that should go into result
        if (
            user_result is not None
            and result_field_type is not None
            and isinstance(user_result, result_field_type)
        ):
            # wrap it into the TargetModel directly
            return user_result

        # dict → merge into input
        if isinstance(user_result, ParameterWrapper):
            base = input_obj.model_dump()
            user_results_dict = (
                user_result.parameters
                if isinstance(user_result.parameters, dict)
                else (
                    user_result.parameters.model_dump()
                    if isinstance(user_result.parameters, BaseModel)
                    else None
                )
            )
            if user_results_dict:
                merged = {**base, **user_results_dict}
            else:
                raise ValueError(
                    f"Arguments {user_result.parameters} is not a valude object"
                )
            input_obj = SourceModel(**merged)
        elif user_result is None:
            if has_explicit_return_none(fn):
                return None
            else:
                # do nothing: input_obj stays as it is
                pass
        else:
            raise ValueError(
                """The function should return either an object of type TargetModel 
                or a ParameterWrapper object, which will trigger transduction from those arguments"""
            )

        # -------------------------------------------------------------
        # 4. build the source Agentics graph
        # -------------------------------------------------------------
        instructions = f"""You are transducing the function {fn.__name__}.
It takes objects of type {SourceModel.__name__} and transduces them into objects of type {TargetModel.__name__}.
Follow the instruction below to perform the transduction.
{fn.__doc__ or ""}"""
        source_ag = source_ag_template.clone()
        source_ag.states = input_obj if isinstance(input_obj, list) else [input_obj]
        # description=instructions,

        # attach the intermediate trace so the transduction can see it
        # source_ag.intermediates = intermediates

        # -------------------------------------------------------------
        # 5. run the transduction (ASYNC!)
        #    note: don't mutate the template globally — clone its instructions per call
        # -------------------------------------------------------------
        # if your AG supports setting instructions on the template:
        target_ag_template.instructions = (
            instructions  # <- avoid mutating shared template
        )
        target_ag = await (target_ag_template << source_ag)

        # -------------------------------------------------------------
        # 6. return the first resulting state
        # -------------------------------------------------------------
        if getattr(target_ag, "states", None):
            first = target_ag.states[0]
            data = first.model_dump()
            # if this was the single-field version, return just that
            if "result" in data and len(data) == 1:
                return data["result"]
            return first

        return None

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):

        if not (len(args) == 1 and isinstance(args[0], MapWrapper)):
            return await wrap_single(*args, **kwargs)

        else:
            ### MAP MULTIPLE INSTANCES
            source_ag = source_ag_template.clone()
            for argument in args[0].arguments:
                if isinstance(argument, ParameterWrapper):
                    if SourceModel.__name__.endswith("$AGSource"):
                        source_ag.states.append(argument.parameters)
                    else:
                        source_ag.states.append(SourceModel(**argument.parameters))
                else:
                    if SourceModel.__name__.endswith("$AGSource"):
                        source_ag.states.append(argument)
                    else:
                        source_ag.states.append(SourceModel(**argument))

            # async def _mapper(x):
            #     return await wrap_single(**x.model_dump())

            intermediate_results = await source_ag.amap(wrap_single)

            return intermediate_results.states
            ## Define 2 AGs, one with final answers and the other with those that have to be sent to LLM
            # send_to_llm=AG(atype=SourceModel)
            # output_ag=AG(atype=TargetModel)
            # for result in intermediate_results:
            #     if isinstance(result, ParameterWrapper):
            #         send_to_llm.states.append(result.parameters)
            #         output_ag.states.append(None)
            #     elif isinstance(result,SourceModel):
            #         send_to_llm.states.append(result)
            #         output_ag.states.append(None)
            #     elif isinstance(result,TargetModel):
            #         output_ag.states.append(result)

            # is_amap=True
            # input_obj = send_to_llm.states
            # if len(input_obj)>0:
            #     instructions = f"""You are transducing the function {fn.__name__}.
            #     It takes objects of type {SourceModel.__name__} and transduces them into objects of type {TargetModel.__name__}.
            #     Follow the instruction below to perform the transduction.
            #     {fn.__doc__ or ""}"""
            #     source_ag=source_ag_template.clone()
            #     source_ag.states=input_obj if isinstance(input_obj,list) else [input_obj]
            #     target_ag_template.instructions = instructions  # <- avoid mutating shared template
            #     llm_output_ag = await (target_ag_template << source_ag)
            #     final_states=[]
            #     for func_out in output_ag.states:
            #         if func_out : final_states.append(final_states)
            #         else: final_states.append(llm_output_ag.states[len(final_states)])
            #     removed_types=[]
            #     for state in final_states:
            #         data = state.model_dump()
            #         # if this was the single-field version, return just that
            #         if "result" in data and len(data) == 1:
            #             removed_types.append(data["result"])
            #         else: removed_types.append(state)
            #     return removed_types

            # else: return output_ag.states

        # --------------------------------------------  -----------------
        # 1. get the input object (either the user passed the pydantic
        #    model directly, or we need to bind and build it)
        # -------------------------------------------------------------

    # optional: expose for debugging
    wrapper.input_model = SourceModel
    wrapper.target_model = TargetModel
    wrapper.target_ag_template = target_ag_template
    return wrapper
