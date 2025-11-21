from typing import Dict, List
from .interface import AgenticApp


class AppRegistry:
    _instance = None
    _apps: Dict[str, AgenticApp] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppRegistry, cls).__new__(cls)
        return cls._instance

    def register(self, app: AgenticApp):
        """Register a new application adapter."""
        if app.slug in self._apps:
            raise ValueError(f"App with slug '{app.slug}' already registered.")
        self._apps[app.slug] = app

    def get(self, slug: str) -> AgenticApp:
        """Retrieve an app by slug."""
        return self._apps.get(slug)

    def list_apps(self) -> List[AgenticApp]:
        """List all registered apps."""
        return list(self._apps.values())


# Global instance
registry = AppRegistry()
