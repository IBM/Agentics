"""
Fast-loading AG wrapper for Flink UDFs.
Only imports the real AG class when actually instantiated.
This reduces import time from ~6.6s to <0.1s.
"""

_AG_CLASS = None


def get_ag_class():
    """Lazy import of the real AG class."""
    global _AG_CLASS
    if _AG_CLASS is None:
        from agentics.core.agentics import AG as RealAG

        _AG_CLASS = RealAG
    return _AG_CLASS


class AG:
    """
    Fast-loading AG wrapper that defers heavy imports until instantiation.

    Usage:
        from agentics.fast_ag import AG  # Fast (<0.1s)
        ag = AG(atype=MyModel)  # Slow (imports real AG, ~6s)
        result = ag << "input"  # Normal speed
    """

    def __new__(cls, *args, **kwargs):
        # When instantiated, create the real AG instance
        RealAG = get_ag_class()
        return RealAG(*args, **kwargs)


# Made with Bob
