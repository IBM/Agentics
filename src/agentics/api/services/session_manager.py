import uuid
import time
from typing import Any, Dict, Optional
from loguru import logger
import os

from agentics import AG


class SessionData:
    def __init__(self, app_id: str):
        self.app_id = app_id
        self.created_at = time.time()
        self.ag_instance: Optional[AG] = None
        self.state: Dict[str, Any] = {}  # For arbitrary app data
        self.files: Dict[str, str] = {}  # file_id -> local_path


class SessionManager:
    """
    In-memory session manager.
    In production, AG instances would need pickling to Redis/Memcached.
    """

    _sessions: Dict[str, SessionData] = {}

    def create_session(self, app_id: str) -> str:
        session_id = str(uuid.uuid4())
        self._sessions[session_id] = SessionData(app_id)
        logger.debug(f"Created session {session_id} for app {app_id}")
        return session_id

    def get_session(self, session_id: str) -> Optional[SessionData]:
        return self._sessions.get(session_id)

    def delete_session(self, session_id: str):
        if session_id in self._sessions:
            session = self._sessions[session_id]

            # Cleanup Files
            for file_path in session.files.values():
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        logger.debug(f"Deleted temp file: {file_path}")
                    except OSError as e:
                        logger.error(f"Error deleting {file_path}: {e}")

            del self._sessions[session_id]
            logger.debug(f"Deleted session {session_id}")

    def get_ag(self, session_id: str) -> Optional[AG]:
        session = self.get_session(session_id)
        return session.ag_instance if session else None

    def set_ag(self, session_id: str, ag: AG):
        session = self.get_session(session_id)
        if session:
            session.ag_instance = ag


session_manager = SessionManager()
