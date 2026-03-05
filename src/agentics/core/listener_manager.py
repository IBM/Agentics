"""
listener_manager.py
===================
Registry of active Kafka listener threads for ``AGStream`` pipelines.

Each listener runs either ``AGStream.listen``,
``AGStream.transducible_function_listener``, or a custom callable in a
daemon thread.  A ``threading.Event`` signals clean shutdown and a
``queue.Queue`` collects log lines that callers can drain.

Typical usage
-------------
::

    from agentics.core.listener_manager import ListenerManager
    from agentics.core.streaming import AGStream

    def make_ag(input_topic, output_topic, **kw):
        return AGStream(
            atype=MyInputType,
            kafka_server="localhost:9092",
            input_topic=input_topic,
            output_topic=output_topic,
            schema_registry_url="http://localhost:8081",
            instructions="Summarise the input.",
        )

    mgr = ListenerManager(agstream_factory=make_ag)

    listener_id = mgr.start(
        fn=my_pipeline_fn,
        input_topic="movies",
        output_topic="movie-summaries",
        source_atype_name="Movie-value",
        target_atype_name="MovieSummary-value",
    )

    # Poll logs
    logs = mgr.drain_logs(listener_id)

    # Stop
    mgr.stop(listener_id)

You can also pass a pre-built ``AGStream`` instance directly via the
``agstream`` keyword argument of :meth:`ListenerManager.start` /
:meth:`ListenerManager.start_simple_listener`, bypassing the factory.
"""

from __future__ import annotations

import queue
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ListenerInfo:
    """Metadata and runtime handles for a single listener thread."""

    listener_id: str
    name: str
    input_topic: str
    output_topic: str
    source_atype_name: Optional[str]
    target_atype_name: Optional[str]
    fn_name: str

    # Runtime handles (set when the thread starts)
    thread: Optional[threading.Thread] = field(default=None, repr=False)
    stop_event: threading.Event = field(default_factory=threading.Event, repr=False)
    log_queue: queue.Queue = field(default_factory=queue.Queue, repr=False)

    # Status
    started_at: float = field(default_factory=time.time)
    stopped: bool = False
    error: Optional[str] = None

    # Stored kwargs so the listener can be restarted
    _start_kwargs: Dict[str, Any] = field(default_factory=dict, repr=False)
    # "transducible" or "simple" — determines which start method to use on restart
    _listener_kind: str = field(default="transducible", repr=False)

    @property
    def is_alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    @property
    def status(self) -> str:
        if self.error:
            return f"error: {self.error}"
        if self.stopped:
            return "stopped"
        if self.is_alive:
            return "running"
        return "idle"


# ---------------------------------------------------------------------------
# ListenerManager
# ---------------------------------------------------------------------------


class ListenerManager:
    """
    Registry of active Kafka listener threads.

    Parameters
    ----------
    agstream_factory:
        Optional callable with signature
        ``(input_topic, output_topic, source_atype_name, target_atype_name, **kw) -> AGStream``.
        Used to build ``AGStream`` instances when one is not supplied directly
        to :meth:`start` / :meth:`start_simple_listener`.
        If ``None``, callers **must** pass a pre-built ``agstream`` instance.
    """

    def __init__(
        self,
        agstream_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self._factory = agstream_factory
        self._listeners: Dict[str, ListenerInfo] = {}
        self._counter = 0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_agstream(
        self,
        input_topic: str,
        output_topic: str,
        source_atype_name: Optional[str],
        target_atype_name: Optional[str],
        agstream: Optional[Any] = None,
    ) -> Any:
        """Return *agstream* if provided, otherwise call the factory."""
        if agstream is not None:
            return agstream
        if self._factory is None:
            raise ValueError(
                "No AGStream instance provided and no agstream_factory configured. "
                "Pass agstream=<AGStream instance> or supply agstream_factory to "
                "ListenerManager.__init__."
            )
        return self._factory(
            input_topic=input_topic,
            output_topic=output_topic,
            source_atype_name=source_atype_name,
            target_atype_name=target_atype_name,
        )

    def _next_id(self) -> str:
        with self._lock:
            self._counter += 1
            return f"listener-{self._counter}"

    # ------------------------------------------------------------------
    # Start / stop
    # ------------------------------------------------------------------

    def start(
        self,
        fn: Any,
        input_topic: str,
        output_topic: str,
        name: Optional[str] = None,
        source_atype_name: Optional[str] = None,
        target_atype_name: Optional[str] = None,
        group_id: Optional[str] = None,
        validate_schema: bool = True,
        produce_results: bool = True,
        verbose: bool = True,
        agstream: Optional[Any] = None,
    ) -> str:
        """
        Start a ``transducible_function_listener`` thread for *fn*.

        Parameters
        ----------
        fn:
            The transducible function to apply to each incoming message.
        input_topic:
            Kafka topic to consume from.
        output_topic:
            Kafka topic to produce results to.
        name:
            Human-readable display name for the listener.
        source_atype_name:
            Registry subject name for the input type (e.g. ``"Movie-value"``).
        target_atype_name:
            Registry subject name for the output type (e.g. ``"MovieSummary-value"``).
        group_id:
            Kafka consumer group ID.  Auto-generated if ``None``.
        validate_schema:
            Whether to validate incoming messages against the registry schema.
        produce_results:
            Whether to produce transduced results to *output_topic*.
        verbose:
            Whether to emit detailed log lines.
        agstream:
            Pre-built ``AGStream`` instance.  When supplied the factory is
            not called.

        Returns
        -------
        str
            The ``listener_id`` that can be used to query status, drain logs,
            or stop the listener.
        """
        listener_id = self._next_id()
        fn_name = getattr(fn, "__name__", repr(fn))
        display_name = name or f"{fn_name}@{input_topic}"

        info = ListenerInfo(
            listener_id=listener_id,
            name=display_name,
            input_topic=input_topic,
            output_topic=output_topic,
            source_atype_name=source_atype_name,
            target_atype_name=target_atype_name,
            fn_name=fn_name,
            _listener_kind="transducible",
            _start_kwargs=dict(
                fn=fn,
                input_topic=input_topic,
                output_topic=output_topic,
                name=name,
                source_atype_name=source_atype_name,
                target_atype_name=target_atype_name,
                group_id=group_id,
                validate_schema=validate_schema,
                produce_results=produce_results,
                verbose=verbose,
                agstream=agstream,
            ),
        )

        ag = self._make_agstream(
            input_topic=input_topic,
            output_topic=output_topic,
            source_atype_name=source_atype_name,
            target_atype_name=target_atype_name,
            agstream=agstream,
        )

        def _run() -> None:
            try:
                info.log_queue.put_nowait(
                    f"▶ Listener '{display_name}' starting "
                    f"({input_topic} → {output_topic})\n"
                )
                ag.transducible_function_listener(
                    fn=fn,
                    group_id=group_id or f"agstream-{listener_id}",
                    validate_schema=validate_schema,
                    produce_results=produce_results,
                    verbose=verbose,
                    stop_event=info.stop_event,
                    log_queue=info.log_queue,
                )
            except Exception as exc:
                info.error = str(exc)
                info.log_queue.put_nowait(f"❌ Listener error: {exc}\n")
                sys.stderr.write(f"[ListenerManager] {display_name} error: {exc}\n")
            finally:
                info.stopped = True
                info.log_queue.put_nowait(f"■ Listener '{display_name}' stopped.\n")

        thread = threading.Thread(target=_run, daemon=True, name=listener_id)
        info.thread = thread

        with self._lock:
            self._listeners[listener_id] = info

        thread.start()
        return listener_id

    def start_simple_listener(
        self,
        input_topic: str,
        output_topic: str,
        source_atype_name: Optional[str] = None,
        target_atype_name: Optional[str] = None,
        instructions: str = "",
        group_id: Optional[str] = None,
        validate_schema: bool = True,
        produce_results: bool = True,
        verbose: bool = True,
        name: Optional[str] = None,
        agstream: Optional[Any] = None,
    ) -> str:
        """
        Start an LLM-based ``listen()`` thread (no custom function required).

        The source and target types are resolved from the schema registry via
        *source_atype_name* and *target_atype_name*.

        Parameters
        ----------
        input_topic:
            Kafka topic to consume from.
        output_topic:
            Kafka topic to produce results to.
        source_atype_name:
            Registry subject name for the input type.
        target_atype_name:
            Registry subject name for the output type.
        instructions:
            Natural-language instructions for the LLM transduction step.
        group_id:
            Kafka consumer group ID.  Auto-generated if ``None``.
        validate_schema:
            Whether to validate incoming messages against the registry schema.
        produce_results:
            Whether to produce transduced results to *output_topic*.
        verbose:
            Whether to emit detailed log lines.
        name:
            Human-readable display name for the listener.
        agstream:
            Pre-built ``AGStream`` instance.  When supplied the factory is
            not called.

        Returns
        -------
        str
            The ``listener_id``.
        """
        listener_id = self._next_id()
        display_name = name or f"simple@{input_topic}→{output_topic}"

        info = ListenerInfo(
            listener_id=listener_id,
            name=display_name,
            input_topic=input_topic,
            output_topic=output_topic,
            source_atype_name=source_atype_name,
            target_atype_name=target_atype_name,
            fn_name="listen",
            _listener_kind="simple",
            _start_kwargs=dict(
                input_topic=input_topic,
                output_topic=output_topic,
                source_atype_name=source_atype_name,
                target_atype_name=target_atype_name,
                instructions=instructions,
                group_id=group_id,
                validate_schema=validate_schema,
                produce_results=produce_results,
                verbose=verbose,
                name=name,
                agstream=agstream,
            ),
        )

        ag = self._make_agstream(
            input_topic=input_topic,
            output_topic=output_topic,
            source_atype_name=source_atype_name,
            target_atype_name=target_atype_name,
            agstream=agstream,
        )
        if instructions:
            ag = ag.model_copy(update={"instructions": instructions})

        def _run() -> None:
            try:
                info.log_queue.put_nowait(
                    f"▶ Simple listener '{display_name}' starting "
                    f"({input_topic} → {output_topic})\n"
                )
                ag.listen(
                    source_atype_name=source_atype_name,
                    group_id=group_id or f"agstream-simple-{listener_id}",
                    validate_schema=validate_schema,
                    produce_results=produce_results,
                    verbose=verbose,
                    stop_event=info.stop_event,
                    log_queue=info.log_queue,
                )
            except Exception as exc:
                info.error = str(exc)
                info.log_queue.put_nowait(f"❌ Listener error: {exc}\n")
                sys.stderr.write(f"[ListenerManager] {display_name} error: {exc}\n")
            finally:
                info.stopped = True
                info.log_queue.put_nowait(
                    f"■ Simple listener '{display_name}' stopped.\n"
                )

        thread = threading.Thread(target=_run, daemon=True, name=listener_id)
        info.thread = thread

        with self._lock:
            self._listeners[listener_id] = info

        thread.start()
        return listener_id

    def stop(self, listener_id: str, timeout: float = 5.0) -> bool:
        """
        Signal the listener to stop and wait up to *timeout* seconds.

        Returns ``True`` if the thread terminated within the timeout.
        """
        info = self._listeners.get(listener_id)
        if info is None:
            return False
        info.stop_event.set()
        if info.thread is not None:
            info.thread.join(timeout=timeout)
        info.stopped = True
        return not (info.thread is not None and info.thread.is_alive())

    def stop_all(self, timeout: float = 5.0) -> None:
        """Stop all running listeners."""
        for lid in list(self._listeners.keys()):
            self.stop(lid, timeout=timeout)

    # ------------------------------------------------------------------
    # Log draining
    # ------------------------------------------------------------------

    def drain_logs(self, listener_id: str, max_lines: int = 200) -> List[str]:
        """
        Drain and return up to *max_lines* log lines from the listener's queue.

        Returns an empty list if the listener is not found.
        """
        info = self._listeners.get(listener_id)
        if info is None:
            return []
        lines: List[str] = []
        try:
            while len(lines) < max_lines:
                lines.append(info.log_queue.get_nowait())
        except queue.Empty:
            pass
        return lines

    # ------------------------------------------------------------------
    # Status queries
    # ------------------------------------------------------------------

    def get_info(self, listener_id: str) -> Optional[ListenerInfo]:
        """Return the ``ListenerInfo`` for *listener_id*, or ``None``."""
        return self._listeners.get(listener_id)

    def list_listeners(self) -> List[ListenerInfo]:
        """Return all registered ``ListenerInfo`` objects."""
        return list(self._listeners.values())

    def running_ids(self) -> List[str]:
        """Return IDs of currently running (alive) listeners."""
        return [lid for lid, info in self._listeners.items() if info.is_alive]

    def has_listener_for(self, input_topic: str) -> bool:
        """
        Return ``True`` if at least one alive listener is consuming *input_topic*.

        This is the check used by :meth:`AGStream.aproduce_and_collect` to
        verify that a worker will process the messages it produces.
        """
        return any(
            info.input_topic == input_topic and info.is_alive
            for info in self._listeners.values()
        )

    def remove(self, listener_id: str) -> bool:
        """
        Remove a stopped listener from the registry.

        Returns ``False`` if the listener is still running or not found.
        """
        info = self._listeners.get(listener_id)
        if info is None:
            return False
        if info.is_alive:
            return False
        del self._listeners[listener_id]
        return True

    # ------------------------------------------------------------------
    # Restart
    # ------------------------------------------------------------------

    def restart(self, listener_id: str) -> Optional[str]:
        """
        Restart a stopped listener using its original start parameters.

        Returns the new ``listener_id`` if successful, or ``None`` if the
        listener is still running, not found, or has no stored kwargs.
        """
        info = self._listeners.get(listener_id)
        if info is None or info.is_alive:
            return None
        kwargs = info._start_kwargs
        if not kwargs:
            return None
        if info._listener_kind == "simple":
            return self.start_simple_listener(**kwargs)
        else:
            return self.start(**kwargs)

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:  # pragma: no cover
        running = len(self.running_ids())
        total = len(self._listeners)
        return f"ListenerManager(running={running}/{total})"


# Made with Bob
