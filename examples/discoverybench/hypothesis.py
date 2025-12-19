from pydantic import BaseModel, Field
from typing import Optional
class Relation(BaseModel):
    """
Relations (interactons) between two variables under a given context that contribute to an hypothesis
E.g., (inversely proportional ,age, income), (“contribute_to”, "salary" , "total income").    
    """
    relation_name:Optional[str] = None
    subject: Optional[str] = Field(None,description="The Variable which is the subject of this relation.")
    object: Optional[str] = Field(None,description="The Variable which is the object of this relation.")
    hypothesis: Optional[str] = Field(None,description="An english sentence stating the hypothesis expressed by this specific relation")

class Hypothesis(BaseModel):
    """
A data-driven hypothesis h in H (the space of such hypotheses) is a declarative sentence about
the state of the world whose truth value may be inferred from a given dataset D using a verification
procedure VD : H → {supported, unsupported}, for instance, via statistical modeling.
    """
    declarative_sentence:Optional[str] = None
    contexts: Optional[list[str]]=Field(None,description=
            """Boundary conditions that limit the scope of a hypothesis.
            E.g., “for men over the age of 30” or “in Asia and Europe” 
            or unbounded/full dataset when not specified""")
    variables: Optional[list[str]]=Field(None,description="""
Known set of concepts that interact in a meaningful way under a given context to
produce the hypothesis. E.g., gender, age, or income. Note that each hypothesis is associated
with a target variable and a set of independent variables.
""")
    relationships: Optional[list[Relation]] = Field(None,description="""
Interactions between a given set of variables under a given context that produces
the hypothesis. E.g., “(inversely proportional ,age, income), (“contribute_to”, "salary" , "total income").
""")

# Given a set of dataset columns, a ground-truth hypothesis, and the analysis workflow used, your task is to extract three dimensions that define the hypothesis: Context, Variables, and Relations. \
#         Here are the definitions for these dimensions:
#         - Contexts: Boundary conditions that limit the scope of a hypothesis. E.g., “for men over \
#         the age of 30”, “in Asia and Europe”. If the context applies to the full dataset, then extract the context from the dataset_descrption.
#         - Variables: Known concepts that interact in a meaningful way under a given context to \
#         produce the hypothesis. E.g., gender, age, income, or "None" if there is no interacting variable.
#         - Relations: Interactions between a given set of variables under a given context to produce \
#         the hypothesis. E.g., “quadratic relationship”, “inversely proportional”, piecewise conditionals, \
#         or "None" if there is no interacting relationship.
#         Make sure to only use the information present in the hypothesis and the workflow. Do not add any new information. \
#         For each dimension, be specific, and do not omit any important details.

#         Here is the metadata for the task:
#         ```json
#         {{
#         "datasets": %s,
#         "hypothesis": "%s",
#         "workflow": "%s"
#         }}
#         ```

#         Return your answer as a JSON object in the following format:
#         ```json
#         {{
#         "sub_hypo": [
#             {{
#                 "text": the hypothesis in natural language,
#                 "context": a short text description of the context of the hypothesis,
#                 "variables": a list of columns involved in the hypothesis,
#                 "relations": a short text description of the relationship between the variables of the hypothesis
#             }},
#             ...
#         ]
#         }}```      


    # question_obj.datasets_descriptions = [x.get_description() for x in question_obj.dbs]