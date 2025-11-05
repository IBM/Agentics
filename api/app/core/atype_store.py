from __future__ import annotations
import os
from pathlib import Path
from typing import List

from agentics.core.atype import import_pydantic_from_code

BASE_PATH = Path(
    os.getenv("ATYPES_PATH", Path(__file__).parent.parent.parent / "predefined_types")
)
BASE_PATH.mkdir(parents=True, exist_ok=True)


def list_names() -> List[str]:
    return [p.stem for p in BASE_PATH.glob("*.py") if not p.stem.startswith("__")]


def load_code(name: str) -> str:
    fp = BASE_PATH / f"{name}.py"
    if not fp.exists():
        raise FileNotFoundError(name)
    return fp.read_text()


def save_code(name: str, code: str) -> None:
    (BASE_PATH / f"{name}.py").write_text(code)


def delete(name: str) -> None:
    (BASE_PATH / f"{name}.py").unlink(missing_ok=False)


def code_to_type(code: str):
    return import_pydantic_from_code(code)
