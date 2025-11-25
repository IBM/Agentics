# Transductions


## What is a Transducible Function?

A **transducible function** `T: X → Y` satisfies:
- **Totality:** every valid input produces a valid output.
- **Local Evidence:** each output field is generated only from its evidence subset.
- **Slot-Level Provenance:** explicit mapping between input and output fields.
#### Transducible Functions

A **transducible function** \(T : X \to Y\) is an explainable function that also satisfies:

1. **Totality**  
   Every valid input \(x \in \llbracket X \rrbracket\) produces a valid output of type \(Y\).

2. **Local Evidence**  
   Each output slot \(y_i\) is computed only from its evidence subset \(\mathcal{E}_i(x)\).  
   No slot is generated “from nothing.”

3. **Slot-Level Provenance**  
   The mapping between input and output slots is explicit:  
   \[
   \mathcal{T}(y_i) = \mathcal{E}_i.
   \]  
   This induces a bipartite graph that serves as the explainability trace.

Transducible functions extend ordinary functions with **structural transparency at the slot level**.



## Implementing Transducible Functions


### Defining Source and Target Types
Agentics uses typed schemas (Pydantic models), structured prompting, and validation to ensure that every transformation is explainable. Evidence subsets determine which inputs can influence which outputs.

```bash
from pydantic import BaseModel
from typing import Optional

class UserMessage(BaseModel):
    content: Optional[str] = None

class Email(BaseModel):
    to: Optional[str] = None
    subject:Optional[str]=None
    body: Optional[str]=None
```

Note: although it is not strictly needed, it is highly recommended that you declare all pydantic fields as Optional and assign them to Null, because when used in transduction, the llm should be able to judge whether those attributes should be transduced from their input source or there is not enough evidence. 


### Defining Transducible Functions

Trasducible functions are async python functions that accept exactly one instance of the Source type X as an input and return exactly one instance of the Target Type Y. Transducible functions can be defined in 2 ways:
- by applying the @transducible() decorator to a transducible python function. 
- by dynamically generating them from any two pair of Source and Target types, with optional instructions and other parameters. 

#### The @transducible() decorator

Agentics allows you to wrap any Python function and turn it into a transducible function, giving it native LLM-powered transformation capabilities. When decorated by @transducible(), they can also return either of the two outputs:
- an instance of the target type, following regular python behaviour
- an instance of type **Transduce** that wraps an instances of the source type, which will be ultimately send to the LLM to generate the required output.

Below is an example of a transducible function invoking an LLM 

```python
from agentics.core.transducible_functions import transducible, Transduce

@transducible()
async def write_an_email_with_llm(state: UserMessage) -> Email:
    """Write an email about the provided content. Elaborate on that and make up content as needed"""
    # example code to modify states before transduction
    return Transduce(state)

@transducible()
async def programmatically(state: UserMessage) -> Email:  
    match = re.match(r"^(Hi|Dear|Hello|Hey)\s+([^,]+),\s*(.+)$", state.content)
    if match:
        greeting, name, body = match.groups()
        return Email(body= body, to=name)
    else: return Email()
```

### Executing Transducible Functions


You can execute transducible functions in the same way you use any other async python function within your code

```python
message=UserMessage(content="Hi Lisa, I made great progress with the new release of Agentics 2.0")
target1 = await(write_an_email_programmatically(message)

target2 = await(write_an_email_programmatically(message)

```

target 1 will return a message like the following 
 

```json
{
  "to": "Lisa",
  "subject": "Update on Agentics 2.0 Release",
  "body": "Hi Lisa,
  I wanted to share some exciting news about the new release of Agentics 2.0. Over the past week, ...
  I'm eager to keep the momentum going and ensure a successful launch.
  Best regards,
  [Your Name]
  [Your Position]
  [Your Contact Information]"
}
```

while target2 will return (deterministically)

```json
{
  "to": "Lisa",
  "subject": null,
  "body": "I made great progress with the new release of Agentics 2.0"
}
```


Note that due to stochastic behaviour of llm, different run on the same function might return different outputs. However, all of them have to be logically transducible and generate explanations from the same source. Therefore the expected behaviour for them is to be semantically equivalent.

On the other hand, fully deterministic output are predictable, but brittle. You'll need to balance the use of both while conding. 

### Map Reduce Framework


When wrapped by the @transducible() operator, transducible functions are overloaded to accept a list of instances of the source type. When executed this way, they return a correspondent list of instances of the target type. 

```python

multiple_mails = await write_an_email([GenericInput(content=f"Hi John, I have made great progress with agentics"),
                                        GenericInput(content=f"Hi Lisa, I have made great progress with agentics")])

```


Under the hood, agentics uses an aMap function, that apply the transducible function asynchronously over a list of SOURCE-type objects.

This enables highly scalable workloads, such as batch inference, dataset scanning, or parallel evidence extraction in Map–Reduce pipelines.
