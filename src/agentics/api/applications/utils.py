from pathlib import Path
from agentics.core.atype import import_pydantic_from_code


def load_predefined_type(name: str):
    path = Path(__file__).parent / "predefined_types" / f"{name}.py"
    if not path.exists():
        raise ValueError(f"Type {name} not found")
    with open(path, "r") as f:
        code = f.read()
    return import_pydantic_from_code(code)
