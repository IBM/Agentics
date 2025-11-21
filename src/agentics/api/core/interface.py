from abc import ABC, abstractmethod
from typing import Any, Dict, Type, Optional
from pydantic import BaseModel


class AgenticApp(ABC):
    """
    Base class for all Agentics applications exposed via API.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def slug(self) -> str:
        """URL-friendly identifier (e.g. 'text-2-sql')."""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        pass

    @abstractmethod
    def get_input_model(self) -> Type[BaseModel]:
        """Returns the Pydantic model used to validate configuration inputs."""
        pass

    @abstractmethod
    def get_output_model(self) -> Type[BaseModel]:
        """Returns the Pydantic model used to structure the response."""
        pass

    async def get_options(self) -> Dict[str, Any]:
        """
        Optional. Returns dynamic choices for UI fields.
        Key: field name, Value: list of choices or metadata.
        """
        return {}

    @abstractmethod
    async def run(self, input_data: BaseModel, files: Dict[str, Any] = None) -> Any:
        """
        Execute the agentic workflow.
        :param input_data: Validated instance of get_input_model()
        :param files: Dictionary of uploaded files (if any)
        """
        pass
