import uuid
import time
from typing import Any, Dict, Optional
from loguru import logger

from agentics import AG
from agentics.api.services.storage import get_storage_provider


class SessionData:
    def __init__(self, app_id: str):
        self.app_id = app_id
        self.created_at = time.time()
        self.ag_instance: Optional[AG] = None
        self.state: Dict[str, Any] = {}
        # Stores file_reference (Local Path OR S3 Key)
        self.files: Dict[str, str] = {}


class SessionManager:
    _sessions: Dict[str, SessionData] = {}

    def __init__(self):
        # Initialize storage provider
        self.storage = get_storage_provider()

    def create_session(self, app_id: str) -> str:
        session_id = str(uuid.uuid4())
        self._sessions[session_id] = SessionData(app_id)
        logger.debug(f"Created session {session_id}")
        return session_id

    def get_session(self, session_id: str) -> Optional[SessionData]:
        return self._sessions.get(session_id)

    def delete_session(self, session_id: str):
        if session_id in self._sessions:
            session = self._sessions[session_id]

            # Use Storage Provider to clean up
            for file_ref in session.files.values():
                self.storage.delete(file_ref)

            del self._sessions[session_id]
            logger.debug(f"Deleted session {session_id}")

    def get_file_path(self, session_id: str, filename: str) -> str:
        """
        Helper to get a strictly local path for a file, downloading
        from S3 if necessary.
        """
        session = self.get_session(session_id)
        if not session or filename not in session.files:
            raise FileNotFoundError("File not in session")

        file_ref = session.files[filename]
        return self.storage.download_path(file_ref)


session_manager = SessionManager()
