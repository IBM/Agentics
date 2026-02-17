# AG (Agentics)

**AG** (short for "Agentics") objects are wrappers around lists of objects having the same Pydantic type.
They are designed to enable async logical transduction among their instances.
AG containers enable us to think about AI workflows in terms of structured data transformations rather than agent behaviors, knowledge, and tasks.

## The AG Class

AG (Agentics) is a Python class that wraps a list of Pydantic objects and enables structured, type-driven logical transduction between them.

**Import and Usage:**
```python
from agentics import AG  # Recommended: use AG alias

# Create a typed container
movies = AG(atype=Movie)
```

Internally, Agentics is implemented as a Pydantic model. It holds:
	•	atype: a reference to the Pydantic class shared by all objects in the list.
	•	states: a list of Pydantic instances, each validated to be of type atype.
    •	tools: a list of tools (CrewAI or Langchain) to be used for transduction

```python
from typing import Type, List
from pydantic import BaseModel, Field

class Agentics(BaseModel):
    atype: Type[BaseModel] = Field(
        ..., 
        description="The shared Pydantic type for all elements in the list."
    )
    states: List[BaseModel] = []
    tools: Optional[List[Any]] = Field(
        None,
        description="List of tools to be used by the agent"
    )
    ...
```


## Atypes

Agentics types are dynamic as they can be modified at run time while ensuring coherent semantics of the data they represent. To this aim, their Pydantic type is represented by the aslot, that can be assigned and modified at runtime. 

Any subclass of BaseModel (i.e. any possible Pydantic Type) can be used as an atype as long as it is serializable.

```python
from agentics.core.agentics import Agentics as AG
from pydantic import BaseModel

class Movie(BaseModel):
    title: str
    genre: str
    description:str

movies = AG(atype=Movie)
movies.states.append(Movie(title="Superman"))
print(movies)
```

## Importing CSV and JSONL

Agentics states can be initialized loaded and saved to .csv or .jsonl files. AType will be automatically generated from the column names (after they will be normalized as needed for valid pydantic fields), with all attributes set to strings.

```python
from pydantic import BaseModel
from agentics.core.agentics import Agentics as AG


# Load CSV automatically acquiring atype
orders = AG.from_csv("data/orders.csv")

# Note that atype contains only strings.
print(orders.atype)
orders.to_csv("data/orders_copy.csv")

# Load Jsonl automatically acquiring atype. 
orders = AG.from_jsonl("data/orders.jsonl")

# Note that atype contains integers fields not only strings.
print(orders.atype)
orders.to_jsonl("data/orders_copy.jsonl")
```

If atype is provided, the file must contain fields that match the attributes defined in atype for them to be acquired, otherwise they'll be set to null. Providing explicit atype is recommedend to have more control on the types of the attributes, which will be otherwise set to string, and consistency on the column names and attribute matching. In addition, it is a convenient way to narrow down the number of attributes required for the task.



```python
# Load from CSV providing custom type (Only matching column names will be inferred)
orders = AG.from_csv("data/orders.csv", atype = Order)
print(orders.atype)

#note that states contains only the attribites in atype, others have been filtered out
orders.pretty_print()
AG.to_csv("data/orders_filtered.jsonl")
```

## Rebind

Agentic types are mutable, and can be modified dynamically, by assigning a new atype

```python
movies = AG.from_csv("data/orders.csv")
print(movies.atype)

class MovieReview(Movie):
    review:str
    quality_score:Optional[int] = Field(None,description="The quality of the movies in a scale 0 to 10")

movies.rebind_atype(MovieReview)
print(movies.states[0])

```

You can also modify and rebind an exiting Agentic. Similarly can also remove attributes. The following code is equivalent to the code before.

```python
movies = AG.from_csv("data/orders.csv")
print(movies[0])
movies.add_attribute("review",str)
movies.add_attribute("quality_score",int,description="The quality of the movies in a scale 0 to 10")
print(movies[0])
movies.subset_atype("title","genre","description")
print(movies[0]) ## note that movies[0] is a shorthand for movies.states[0]
```


## 🎯 Transduction between Agentics

In addition to Transducible Functions syntax, AGs enables built in transduction. This was the preferred syntax for AG 1.0 and it is still supported by Agentics 2.0.

```python
import asyncio
from pydantic import BaseModel
from agentics import AG
from typing import Optional

class Answer(BaseModel):
    answer: Optional[str] = None
    justification: Optional[str] = None
    confidence: Optional[float] = None

async def main():
    input_questions = [
        "What is the capital of Italy?",
        "What is the best F1 team in history?",
    ]

    answers = await (AG(atype=Answer) << input_questions)

    answers.pretty_print()

asyncio.run(main())
```


## Reference code

See the [examples directory](../examples/) for practical demonstrations of AG usage, including:
- `hello_world.py` - Basic transduction example
- `generate_tweets.py` - Content generation
- `emotion_extractor.py` - Text analysis

## Go to Index
- 👉 [Index](index.md)
