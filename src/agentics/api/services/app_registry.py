from typing import Dict, List, Type
from agentics.api.applications.base import AgenticsApp

class AppRegistry:
    _apps: Dict[str, AgenticsApp] = {}

    def register(self, app_cls: Type[AgenticsApp]):
        instance = app_cls()
        self._apps[instance.metadata.id] = instance

    def get_app(self, app_id: str) -> AgenticsApp | None:
        return self._apps.get(app_id)

    def list_apps(self) -> List[Dict]:
        return [app.metadata.model_dump() for app in self._apps.values()]

app_registry = AppRegistry()