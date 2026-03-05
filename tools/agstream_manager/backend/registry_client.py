"""
registry_client.py
==================
Thin wrapper around AGStream's schema-registry methods, providing a clean
interface for the Streamlit editor app to:

  * list all registered subjects (schema names)
  * fetch a dynamically-reconstructed Pydantic model for any subject
  * check registry connectivity

All calls are synchronous and return plain Python values so they can be
called directly from Streamlit callbacks.
"""

from __future__ import annotations

import sys
from typing import Dict, List, Optional, Tuple, Type

import requests
from pydantic import BaseModel

from agentics.core.streaming import AGStream
from agentics.core.streaming_utils import get_atype_from_registry

# ---------------------------------------------------------------------------
# RegistryClient
# ---------------------------------------------------------------------------


class RegistryClient:
    """
    Lightweight client for the Karapace / Confluent Schema Registry.

    Parameters
    ----------
    registry_url:
        Base URL of the schema registry (default: ``http://localhost:8081``).
    kafka_server:
        Kafka bootstrap server used when constructing temporary ``AGStream``
        instances for schema lookups (default: ``localhost:9092``).
    """

    def __init__(
        self,
        registry_url: str = "http://localhost:8081",
        kafka_server: str = "localhost:9092",
    ) -> None:
        self.registry_url = registry_url.rstrip("/")
        self.kafka_server = kafka_server

    # ------------------------------------------------------------------
    # Connectivity
    # ------------------------------------------------------------------

    def is_reachable(self) -> bool:
        """Return ``True`` if the registry responds to a ``/subjects`` GET."""
        try:
            r = requests.get(f"{self.registry_url}/subjects", timeout=3)
            return r.status_code == 200
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Subject listing
    # ------------------------------------------------------------------

    def list_subjects(self) -> List[str]:
        """
        Return all subject names registered in the schema registry.

        Returns an empty list on error.
        """
        try:
            r = requests.get(f"{self.registry_url}/subjects", timeout=5)
            if r.status_code == 200:
                return sorted(r.json())
            sys.stderr.write(f"⚠️  list_subjects: HTTP {r.status_code} — {r.text}\n")
            return []
        except Exception as exc:
            sys.stderr.write(f"⚠️  list_subjects error: {exc}\n")
            return []

    def list_versions(self, subject: str) -> List[int]:
        """
        Return all version numbers for *subject*.

        Returns an empty list if the subject does not exist or on error.
        """
        try:
            r = requests.get(
                f"{self.registry_url}/subjects/{subject}/versions", timeout=5
            )
            if r.status_code == 200:
                return r.json()
            return []
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Schema / atype retrieval
    # ------------------------------------------------------------------

    def get_schema_json(self, subject: str, version: str = "latest") -> Optional[Dict]:
        """
        Fetch the raw JSON Schema dict for *subject* at *version*.

        Returns ``None`` on error.
        """
        import json

        try:
            url = f"{self.registry_url}/subjects/{subject}/versions/{version}"
            r = requests.get(url, timeout=5)
            if r.status_code != 200:
                sys.stderr.write(
                    f"⚠️  get_schema_json: HTTP {r.status_code} for '{subject}'\n"
                )
                return None
            payload = r.json()
            schema_str = payload.get("schema")
            if not schema_str:
                return None
            return json.loads(schema_str)
        except Exception as exc:
            sys.stderr.write(f"⚠️  get_schema_json error: {exc}\n")
            return None

    def get_atype(
        self,
        subject: str,
        version: str = "latest",
    ) -> Optional[Type[BaseModel]]:
        """
        Dynamically reconstruct a Pydantic ``BaseModel`` class from the
        JSON Schema stored under *subject* in the registry.

        Uses :func:`streaming_utils.get_atype_from_registry` so no Kafka
        connection is required.

        The *subject* is passed as-is (``add_suffix=False``) so that the
        exact registry subject name is used without appending ``-value``.

        Returns ``None`` on error.
        """
        return get_atype_from_registry(
            atype_name=subject,
            schema_registry_url=self.registry_url,
            version=version,
            add_suffix=False,
        )

    def get_all_atypes(self) -> Dict[str, Optional[Type[BaseModel]]]:
        """
        Return a mapping of ``{subject_name: PydanticClass}`` for every
        subject currently registered.

        Subjects whose schema cannot be reconstructed map to ``None``.
        """
        subjects = self.list_subjects()
        result: Dict[str, Optional[Type[BaseModel]]] = {}
        for subject in subjects:
            result[subject] = self.get_atype(subject)
        return result

    # ------------------------------------------------------------------
    # Convenience: build an AGStream pre-wired to a subject pair
    # ------------------------------------------------------------------

    def make_agstream(
        self,
        input_topic: str,
        output_topic: str,
        source_atype_name: Optional[str] = None,
        target_atype_name: Optional[str] = None,
    ) -> AGStream:
        """
        Construct an ``AGStream`` instance wired to the given topics and
        optional registry subject names.

        The returned stream has no ``atype`` set — callers should assign
        one (e.g. via ``get_atype``) before calling listener methods.
        """
        return AGStream(
            kafka_server=self.kafka_server,
            schema_registry_url=self.registry_url,
            input_topic=input_topic,
            output_topic=output_topic,
            source_atype_name=source_atype_name,
            target_atype_name=target_atype_name,
        )

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:  # pragma: no cover
        status = "✓ reachable" if self.is_reachable() else "✗ unreachable"
        return f"RegistryClient(url={self.registry_url!r}, {status})"


# Made with Bob
