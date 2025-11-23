from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type
from pydantic import BaseModel

from agentics.api.models import AppMetadata, UIOption


class AgenticsApp(ABC):
    """
    Base class for all Agentics applications.
    Wraps the logic of an Agentics workflow into a stateless execution unit
    that operates on a stateful Session.
    """

    @property
    @abstractmethod
    def metadata(self) -> AppMetadata:
        pass

    @abstractmethod
    def get_input_schema(self) -> Dict[str, Any]:
        """Return JSON schema for the execution input."""
        pass

    def get_options(self) -> Dict[str, UIOption]:
        """
        Return dynamic options for UI dropdowns.
        Returns a dict where key = field_name, value = UIOption definition.
        """
        return {}

    async def perform_action(self, session_id: str, action: str, payload: dict) -> Any:
        """
        Handle auxiliary actions.
        """
        raise NotImplementedError(f"Action '{action}' not supported by this app.")

    async def perform_action(self, session_id: str, action: str, payload: dict) -> Any:
        """
        Handle auxiliary actions (e.g., drafting schemas, specialized updates).
        Default implementation raises an error implying action is not supported.
        """
        raise NotImplementedError(f"Action '{action}' not supported by this app.")

    @abstractmethod
    async def execute(
        self, session_id: str, input_data: BaseModel | dict
    ) -> Dict[str, Any]:
        """
        Core logic hook.
        Args:
            session_id: The ID of the active session (used to retrieve AG state).
            input_data: Validated input based on get_input_schema.
        """
        pass
