"""
listener_manager.py  (transducible_functions_editor)
=====================================================
Re-exports ``ListenerManager`` and ``ListenerInfo`` from the core library
and provides a thin ``make_listener_manager`` helper that wires the
``RegistryClient`` factory into a ``ListenerManager`` instance.

All listener management logic now lives in
``agentics.core.listener_manager``.
"""

from __future__ import annotations

from typing import Optional

from registry_client import RegistryClient

from agentics.core.listener_manager import ListenerInfo, ListenerManager

__all__ = ["ListenerInfo", "ListenerManager", "make_listener_manager"]


def make_listener_manager(registry_client: RegistryClient) -> ListenerManager:
    """
    Build a :class:`ListenerManager` whose ``agstream_factory`` delegates to
    *registry_client*.

    Parameters
    ----------
    registry_client:
        A ``RegistryClient`` instance used to build ``AGStream`` objects.

    Returns
    -------
    ListenerManager
    """

    def _factory(
        input_topic: str,
        output_topic: str,
        source_atype_name: Optional[str] = None,
        target_atype_name: Optional[str] = None,
        **_kw,
    ):
        return registry_client.make_agstream(
            input_topic=input_topic,
            output_topic=output_topic,
            source_atype_name=source_atype_name,
            target_atype_name=target_atype_name,
        )

    return ListenerManager(agstream_factory=_factory)


# Made with Bob
