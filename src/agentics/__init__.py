# CRITICAL: Suppress CrewAI prompts BEFORE any imports
import os

os.environ["CREWAI_TRACING_ENABLED"] = "false"
os.environ["CREWAI_DISABLE_TELEMETRY"] = "true"
os.environ["CREWAI_DISABLE_TRACING"] = "true"
os.environ["CREWAI_TELEMETRY"] = "false"
os.environ["OTEL_SDK_DISABLED"] = "true"
os.environ["CREWAI_TRACING_DISABLED"] = "true"
os.environ["CREWAI_SILENT"] = "true"
os.environ["CREWAI_STORAGE_DIR"] = "/tmp/crewai"
os.environ["CREWAI_ALLOW_STACK_TRACES"] = "false"

# Monkey-patch input() to prevent CrewAI prompts from blocking
import builtins

_original_input = builtins.input


def _silent_input(prompt=""):
    """Always return 'N' to prevent blocking on any prompts"""
    return "N"


builtins.input = _silent_input

try:
    __import__("pysqlite3")
    import sys

    sys.modules["sqlite3"] = sys.modules.pop("pysqlite3")

except:
    pass

from .core import *
