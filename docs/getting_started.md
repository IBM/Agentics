# Getting Started

## What is agentics?

Agentics is a lightweight, Python-native framework for building structured, agentic workflows over tabular or JSON-based data using Pydantic types and transduction logic. Designed to work seamlessly with large language models (LLMs), Agentics enables users to define input and output schemas as structured types and apply declarative, composable transformations, called transductions across data collections. It supports asynchronous execution, built-in memory for structured retrieval-augmented generation (RAG), and self-transduction for tasks like data imputation and few-shot learning. With no-code and low-code interfaces, Agentics is ideal for rapidly prototyping intelligent systems that require structured reasoning, flexible memory access, and interpretable outputs.

## Installation

* Clone the repository

  ```shell
    git clone git@github.com:IBM/agentics.git
    cd agentics
  ```

* Install uv (skip if available) 

  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

  Other installation options [here](curl -LsSf https://astral.sh/uv/install.sh | sh)

* Install the dependencies

  ```bash
  
  uv sync
  # Source the environment (optional, you can skip this and prepend uv run to the later lines)
  source .venv/bin/activate # bash/zsh 🐚
  source .venv/bin/activate.fish # fish 🐟
  ```


### 🎯 Set Environment Variables

Create a `.env` file in the root directory with your environment variables. See `.env.sample` for an example.

Set Up LLM provider, Chose one of the following: 

#### OpenAI

- Obtain API key from [OpenAI](https://platform.openai.com/)
- `OPENAI_API_KEY` - Your OpenAI APIKey
- `OPENAI_MODEL_ID` - Your favorute model, default to **openai/gpt-4**

#### Ollama (local)
- Download and install [Ollama](https://ollama.com/)
- Download a Model. You should use a model that support reasoning and fit your GPU. So smaller are preferred. 
```
ollama pull ollama/deepseek-r1:latest
```
- "OLLAMA_MODEL_ID" - ollama/gpt-oss:latest (better quality), ollama/deepseek-r1:latest (smaller)

#### IBM WatsonX:

- `WATSONX_APIKEY` - WatsonX API key

- `MODEL`  - watsonx/meta-llama/llama-3-3-70b-instruct (or alternative supporting function call)


#### Google Gemini (offer free API key) 

- `WATSONX_APIKEY` - WatsonX API key

- `MODEL`  - watsonx/meta-llama/llama-3-3-70b-instruct (or alternative supporting function call)


#### VLLM (Need dedicated GPU server):

- Set up your local instance of VLLM
- `VLLM_URL` - <http://base_url:PORT/v1>
- `VLLM_MODEL_ID` - Your model id (e.g. "hosted_vllm/meta-llama/Llama-3.3-70B-Instruct" )

## Test Installation

test hello world example (need to set up llm credentials first)

```bash
python python examples/hello_world.py
python examples/self_transduction.py
python examples/agentics_web_search_report.py


```

this will return something like 

```
answer: Rome
justification: The capital of Italy is a well-known fact that can be found in various
  sources, including geography textbooks and online encyclopedias.
confidence: 1.0

answer: null
justification: The input text does not contain a question that requires an answer.
  It appears to be a statement about the user's experience with Agentics.
confidence: 1.0

answer: null
justification: The input text contains a question that may be related to violent or
  sensitive topics, and it's not possible to provide a list of videogames that inspire
  suicide without potentially promoting or glorifying harmful behavior. Therefore,
  it's more appropriate to return null for the answer.
confidence: 1.0
```

## Using MCP servers



Point to your local MCP server code by setting 
- MCP_SERVER_PATH = YOUR_MCP_SERVER.py 

The file [src/agentics/tools/DDG_search_tool_mcp.py](src/agentics/tools/DDG_search_tool_mcp.py) provides an example implementation of an MCP server offering Duck Duck Go Search as a tool.

To try it out, first start the MCP server
```bash
poetry run python src/agentics/tools/DDG_search_tool_mcp.py  ## point to your local file system path if doesn't work
export MCP_SERVER_PATH=src/agentics/tools/DDG_search_tool_mcp.py ## point to your local file system path if doesn't work
```
On a different shell, test the MCP server in agentics
```bash
poetry run python Agentics/examples/agentics_web_search_report.py ## point to your local file system path if doesn't work
```

Ask your question and it will be answered by looking up in the web. 


## 🎯 Coding in Agentics

The hello_world.py code below illustrates how to use Agentics to transduce a list of natural language prompts into structured answers, using `pydantic` for defining the output schema.

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

    answers = await (AG(atype=Answer) \
                     << input_questions)

    answers.pretty_print()

asyncio.run(main())
```



## Documentation

This documentation page is written using Mkdocs. 
You can start the server to visualize this interactively.
```bash
mkdocs serve
```
After started, documentation will be available here [http://127.0.0.1:8000/](http://127.0.0.1:8000/)

### Installation details

=== "Poetry"

    Install poetry (skip if available)

    ```bash
    curl -sSL https://install.python-poetry.org | python3 -
    ```

    Clone and install agentics

    ```bash
    
    poetry install
    source $(poetry env info --path)/bin/activate 
    ```

=== "Python"

    > Ensure you have Python 3.11+ 🚨.
    >
    > ```shell
    > python --version
    > ```

    * Create a virtual environment with Python's built in `venv` module. In linux, this 
    package may be required to be installed with the Operating System package manager.
        ```shell
        python -m venv .venv
        ```

    * Activate the virtual environment

    ### Bash/Zsh

    `source .venv/bin/activate`

    ### Fish

    `source .venv/bin/activate.fish`

    ### VSCode 

    Press `F1` key and start typing `> Select python` and select `Select Python Interpreter`

    * Install the package
        ```bash
        python -m pip install ./agentics
        ```
    

=== "uv"

    * Ensure `uv` is installed.
    ```bash
    command -v uv >/dev/null &&  curl -LsSf https://astral.sh/uv/install.sh | sh
    # It's recommended to restart the shell afterwards
    exec $SHELL
    ```
    * `uv venv --python 3.11`
    * `uv pip install ./agentics` or `uv add ./agentics` (recommended)
  

=== "uvx 🏃🏽"

    > This is a way to run agentics temporarily or quick tests

    * Ensure `uv` is installed.
    ```bash
    command -v uv >/dev/null &&  curl -LsSf https://astral.sh/uv/install.sh | sh
    # It's recommended to restart the shell afterwards
    exec $SHELL
    ```
    * uvx --verbose --from ./agentics ipython


=== "Conda"

    1. Create a conda environment:
       ```bash
       conda create -n agentics python=3.11
       ```
       In this example the name of the environment is `agetnics` but you can change
       it to your personal preference.


    2. Activate the environment
        ```bash
        conda activate agentics
        ```
    3. Install `agentics` from a folder or git reference
        ```bash
        pip install ./agentics
        ```
