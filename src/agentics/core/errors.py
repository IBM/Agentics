class AgenticsError(Exception):
    """Base class for all custom exceptions in Agentics."""

    pass


class InvalidStateError(AgenticsError):
    pass


class TransductionError(AgenticsError):
    pass
