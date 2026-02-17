# Operator 	Description
# sem_map 	Map each record using a natural language projection
# sem_filter 	Keep records that match the natural language predicate
# sem_extract 	Extract one or more attributes from each row
# sem_agg 	Aggregate across all records (e.g. for summarization)
# sem_topk 	Order the records by some natural language sorting criteria
# sem_join 	Join two datasets based on a natural language predicate
import pathlib
from typing import Type

import pandas as pd
from pydantic import BaseModel, Field

from agentics import AG
from agentics.core.atype import create_pydantic_model


def normalize_input_data(source: AG | pd.DataFrame) -> AG:
    if type(source) is pd.DataFrame:
        return AG.from_dataframe(source)
    elif type(source) is AG:
        return source.clone()
    else:
        raise ValueError("source must be of type AG or pd.DataFrame")


async def sem_map(
    source: AG | pd.DataFrame,
    target_type: Type[BaseModel] | str,
    instructions: str,
    merge_output: bool = True,  ## Target, Merged
    **kwargs,
) -> AG | pd.DataFrame:
    """
    Agentics-native semantic map (LOTUS-style) from a source `AG` into a target schema.
    also works for LOTUS-style sem_extract

    This function implements an agentic analogue of LOTUS `sem_map`: it semantically
    transforms each source state into a target representation defined by `target_type`,
    using natural-language `instructions` to guide the mapping.  The resulting mapped states can optionally be merged back into the
    original `source` states.

    Parameters
    ----------
    source : AG
        The input Agentics graph/state collection containing the states to be mapped.
    target_type : Type[pydantic.BaseModel] | str
        Target schema for the mapped output.
        - If a `BaseModel` subclass, it is used directly as the target `atype`.
        - If a `str`, a Pydantic model is created dynamically named after target_type,
          using `instructions` as semantic guidance. A type named after the BaseModel
          with a single argument of type string.
    instructions : str , optional
        Natural-language rubric describing how to produce the target fields from the
        source content (e.g., extraction rules, normalization requirements, labeling
        criteria).
    source_fields : list[str], optional
        If provided, restricts the mapping input to these fields from each source
        state by setting `source.transduce_fields`.
    merge_output : bool, default=True
        If True, merge the mapped output into the original `source` states and return
        the merged `AG`. If False, return only the mapped output `AG`.
    **kwargs
        Additional keyword arguments forwarded to `AG(...)` when constructing the
        target agent graph (e.g., model/connection configuration, batching, caching,
        execution settings).

    Returns
    -------
    AG
        - If `merge_output=True`: an `AG` containing the original source states with
          mapped fields merged in.
        - If `merge_output=False`: the `AG` produced by the transduction, containing
          only the mapped output states.

    Notes
    -----
    - The semantic mapping is executed asynchronously via `await (target_ag << source)`.
    """
    ag_source = normalize_input_data(source)
    target_ag = AG(
        atype=(
            create_pydantic_model(
                [(target_type, "str", instructions, False)], target_type
            )
            if isinstance(target_type, str)
            else target_type
        ),
        instructions=instructions,
        **kwargs,
    )

    map_out = await (target_ag << ag_source)
    output_ag = None
    if merge_output:
        output_ag = ag_source.merge_states(map_out)
    else:
        output_ag = ag_source
    if type(source) is pd.DataFrame:
        return output_ag.to_dataframe()
    return output_ag


async def sem_filter(
    source: AG | pd.DataFrame,
    predicate_template: str,
    sensitivity: float = 0.8,
    **kwargs,
) -> AG | pd.DataFrame:
    """
    Agentics-native semantic filter over an `AG` using a LangChain-style condition template.

    This function evaluates a natural-language predicate for each state in `source`
    and returns a new `AG` containing only the states classified as satisfying the
    predicate. It is an agentic analogue of LOTUS-style semantic filtering.

    The `predicate_template` is a **LangChain-style template** (e.g., using `{field}`
    placeholders) that is rendered against each source state's fields. The rendered
    text is then passed to an LLM-based logical classifier which produces a boolean
    decision (`condition_true`) for that state.

    Parameters
    ----------
    source : AG
        The input Agentics collection to be filtered.
    predicate_template : str
        A LangChain-style prompt template over the fields of `source` states, using
        placeholders like `{reviewText}` or `{title}`. For each state, the template is
        rendered with that state's field values and the resulting text is classified
        as True/False by the logical classifier.
    **kwargs
        Additional keyword arguments forwarded to `AG(...)` when constructing the
        classifier target graph (e.g., model/connection configuration, retries,
        caching, or batching settings). These override the defaults set here.

    Returns
    -------
    AG
        A cloned `AG` containing only the subset of original `source` states that
        satisfy the predicate (i.e., where `condition_true` is True).

    Notes
    -----
    - This function clones `source` to avoid mutating the original object.
    - Filtering assumes a 1:1 positional alignment between `source` and `map_out`.
      If your transduction can reorder or drop items, switch to an ID-based alignment.
    - Default classifier settings include `amap_batch_size=20` for batched evaluation.
    """

    ag_source = normalize_input_data(source)

    target_ag = AG(
        atype=create_pydantic_model(
            [
                (
                    "sentence_is_true",
                    "bool",
                    """Provide True if you think the input sentence is True, False otherwise""",
                    False,
                ),
                (
                    "truth_score",
                    "float",
                    """Provide a number from 0 to 1 to assess the degree to which the input Sentence is True. If you are uncertain, provide a number in the range (0,1)""",
                    False,
                ),
            ],
            name="filter",
        ),
        instructions="""You have been given an input sentence.
            Read the input text and return True if the sentence is true, False otherwise""",
        amap_batch_size=20,
        **kwargs,
    )
    # Keep the more sophisticated version that handles both cases
    if "{" in predicate_template:
        ag_source.prompt_template = predicate_template
    else:
        target_ag.instructions += f"\n\nPredicate: {predicate_template}"

    map_out = await (target_ag << ag_source)
    target = ag_source.clone()
    target.states = []

    for map_out_c, source_c in zip(map_out.states, ag_source.states):
        if map_out_c.truth_score and map_out_c.truth_score >= sensitivity:
            target.append(source_c)

    if type(source) is pd.DataFrame:
        return target.to_dataframe()
    else:
        return target


async def sem_agg(
    source: AG | pd.DataFrame,
    target_type: Type[BaseModel] | str,
    instructions: str | None = None,
    # merge_output: bool = True,  ## Target, Merged
    **kwargs,
) -> AG | pd.DataFrame:
    """ """
    ag_source = normalize_input_data(source)
    target_ag = AG(
        atype=(
            create_pydantic_model(
                [(target_type, "str", instructions, False)], target_type
            )
            if isinstance(target_type, str)
            else target_type
        ),
        instructions=instructions,
        transduction_type="areduce",
        **kwargs,
    )

    output_ag = await (target_ag << ag_source)
    if type(source) is pd.DataFrame:
        return output_ag.to_dataframe()
    return output_ag


# Made with Bob
