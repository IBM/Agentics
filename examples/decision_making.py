import asyncio
import json
from typing import Optional

from pydantic import BaseModel

from agentics.core.transducible_functions import With


class DecisionMakingTask(BaseModel):
    task_description: str
    options: list[str]


class DecisionMakingCase(BaseModel):
    case_description: Optional[str] = None


class DecisionOutcome(BaseModel):
    chosen_option: str


decision_making = DecisionOutcome << With(
    DecisionMakingTask & DecisionMakingCase, provide_explanation=True
)

task = DecisionMakingTask(
    task_description="Choose the best mode of transportation",
    options=["Car", "Bicycle", "Public Transit", "Walking"],
)
case = DecisionMakingCase(
    case_description="The destination is 5 miles away in a"
    "busy urban area with moderate traffic."
)

decision, explanation = asyncio.run(decision_making(task & case))


print(decision.model_dump())
data = json.loads(explanation.model_dump_json())
print(json.dumps(data, indent=2))
