"""
function_store.py
=================
In-memory store for ``TransducibleFunction`` objects and composed pipelines.

The store is intentionally simple — it holds named functions and pipelines
in plain dicts so the Streamlit app can reference them by name across
reruns (stored in ``st.session_state``).

Usage
-----
::

    from function_store import FunctionStore
    from agentics.core.transducible_functions import TransducibleFunction

    store = FunctionStore()

    # Register a single function
    store.add_function("summarise_review", summarise_review)

    # Register a composed pipeline
    pipeline = classify_sentiment << summarise_review
    store.add_pipeline("classify_then_summarise", pipeline)

    # Retrieve
    fn = store.get_function("summarise_review")
    pl = store.get_pipeline("classify_then_summarise")
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


class FunctionStore:
    """
    Thread-safe (GIL-protected) in-memory registry for transducible functions
    and composed pipelines.
    """

    def __init__(self) -> None:
        # {name: TransducibleFunction}
        self._functions: Dict[str, Any] = {}
        # {name: TransducibleFunction}  (composed via <<)
        self._pipelines: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Functions
    # ------------------------------------------------------------------

    def add_function(self, name: str, fn: Any) -> None:
        """Register a transducible function under *name*."""
        self._functions[name] = fn

    def remove_function(self, name: str) -> bool:
        """Remove a function by name. Returns ``True`` if it existed."""
        return self._functions.pop(name, None) is not None

    def get_function(self, name: str) -> Optional[Any]:
        """Return the function registered as *name*, or ``None``."""
        return self._functions.get(name)

    def list_functions(self) -> List[str]:
        """Return sorted list of registered function names."""
        return sorted(self._functions.keys())

    def function_info(self, name: str) -> Optional[Dict[str, str]]:
        """
        Return a dict with ``input_model``, ``target_model``, and ``doc``
        for the named function, or ``None`` if not found.
        """
        fn = self._functions.get(name)
        if fn is None:
            return None
        return _fn_info(fn)

    # ------------------------------------------------------------------
    # Pipelines (composed functions)
    # ------------------------------------------------------------------

    def add_pipeline(self, name: str, pipeline: Any) -> None:
        """Register a composed pipeline under *name*."""
        self._pipelines[name] = pipeline

    def remove_pipeline(self, name: str) -> bool:
        """Remove a pipeline by name. Returns ``True`` if it existed."""
        return self._pipelines.pop(name, None) is not None

    def get_pipeline(self, name: str) -> Optional[Any]:
        """Return the pipeline registered as *name*, or ``None``."""
        return self._pipelines.get(name)

    def list_pipelines(self) -> List[str]:
        """Return sorted list of registered pipeline names."""
        return sorted(self._pipelines.keys())

    def pipeline_info(self, name: str) -> Optional[Dict[str, str]]:
        """
        Return a dict with ``input_model``, ``target_model``, and ``doc``
        for the named pipeline, or ``None`` if not found.
        """
        pl = self._pipelines.get(name)
        if pl is None:
            return None
        return _fn_info(pl)

    # ------------------------------------------------------------------
    # Composition helper
    # ------------------------------------------------------------------

    def compose(self, names: List[str], result_name: str) -> Any:
        """
        Compose a list of function names (left-to-right application order)
        using the ``<<`` operator and store the result as *result_name*.

        The ``<<`` operator applies right-to-left in the mathematical sense
        (``f << g`` means "apply g first, then f"), so to apply functions
        in the order ``[f1, f2, f3]`` we build ``f1 << f2 << f3``.

        Returns the composed ``TransducibleFunction``.

        Raises ``KeyError`` if any name is not found.
        Raises ``ValueError`` if fewer than 2 names are provided.
        """
        if len(names) < 2:
            raise ValueError("compose() requires at least 2 function names")

        fns = []
        for n in names:
            fn = self._functions.get(n) or self._pipelines.get(n)
            if fn is None:
                raise KeyError(f"No function or pipeline named '{n}'")
            fns.append(fn)

        # Build left-to-right: fns[0] << fns[1] << fns[2] ...
        result = fns[0]
        for fn in fns[1:]:
            result = result << fn

        self._pipelines[result_name] = result
        return result

    # ------------------------------------------------------------------
    # Bulk access
    # ------------------------------------------------------------------

    def all_callables(self) -> Dict[str, Any]:
        """Return a merged dict of all functions and pipelines."""
        merged = dict(self._functions)
        merged.update(self._pipelines)
        return merged

    def summary(self) -> List[Tuple[str, str, str, str]]:
        """
        Return a list of ``(kind, name, input_model, target_model)`` tuples
        for all registered functions and pipelines.
        """
        rows: List[Tuple[str, str, str, str]] = []
        for name, fn in sorted(self._functions.items()):
            info = _fn_info(fn)
            rows.append(("function", name, info["input_model"], info["target_model"]))
        for name, pl in sorted(self._pipelines.items()):
            info = _fn_info(pl)
            rows.append(("pipeline", name, info["input_model"], info["target_model"]))
        return rows

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"FunctionStore("
            f"functions={self.list_functions()}, "
            f"pipelines={self.list_pipelines()})"
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _fn_info(fn: Any) -> Dict[str, str]:
    """Extract display info from a TransducibleFunction (or any callable)."""
    input_model = "?"
    target_model = "?"
    doc = ""

    try:
        im = getattr(fn, "input_model", None)
        if im is not None:
            input_model = getattr(im, "__name__", str(im))
    except Exception:
        pass

    try:
        tm = getattr(fn, "target_model", None)
        if tm is not None:
            target_model = getattr(tm, "__name__", str(tm))
    except Exception:
        pass

    try:
        doc = (fn.__doc__ or "").strip().splitlines()[0]
    except Exception:
        pass

    return {"input_model": input_model, "target_model": target_model, "doc": doc}


# Made with Bob
