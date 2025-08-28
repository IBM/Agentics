def test_import():
    """Checks that main Agentics class has no import issues"""
    from agentics import (
        Agentics as AG,  # pylint: disable=import-outside-toplevel,unused-import
    )
