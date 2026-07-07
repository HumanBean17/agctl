"""LogClient with entry-point backend dispatch (DESIGN §9.2).

Selects a :class:`LogBackend` implementation by the source's ``type`` field,
discovering third-party backends via the ``agctl.logs_backends`` entry-point
group while always falling back to the built-in ``file`` backend.
"""

from __future__ import annotations

import importlib.metadata
from datetime import datetime
from typing import Protocol

from ..errors import ConfigError
from .log_backends.ndjson_file import NdjsonFileBackend
from .log_backend_protocol import (
    AwaitResult,
    LogFilter,
    ScanResult,
    SchemaDescriptor,
)

#: Entry-point group used to discover third-party log backends.
LOG_BACKEND_ENTRY_POINT_GROUP = "agctl.logs_backends"

#: Built-in backends always available even without entry-point registration.
BUILTIN_BACKENDS: dict[str, type] = {"file": NdjsonFileBackend}


class LogClient:
    """High-level log client that delegates to a discovered backend.

    The backend is selected by ``source["type"]``:

    - If ``backend`` is injected (DI), it is used directly and no lookup occurs.
    - Otherwise the backend class is looked up in ``backends`` (or
      :meth:`load_backends` by default) and instantiated with the source.
    """

    def __init__(
        self,
        source,
        *,
        backend=None,
        backends: dict[str, type] | None = None,
    ) -> None:
        # Normalize the source into a plain dict (pydantic models expose
        # model_dump(); plain dicts pass through unchanged).
        self._source_dict = getattr(source, "model_dump", lambda: source)()

        if backend is not None:
            self._backend = backend
        else:
            available = backends if backends is not None else self.load_backends()
            src_type = self._source_dict.get("type")
            if not src_type or src_type not in available:
                raise ConfigError(
                    f"Unknown logs backend type: {src_type}", {"type": src_type}
                )
            backend_class = available[src_type]
            self._backend = backend_class(source)

        # Validate the source config (command-entry guard)
        self._backend.validate_config()

    @classmethod
    def load_backends(cls) -> dict[str, type]:
        """Discover log backends via entry points, merging built-ins.

        Returns a ``{type_name: backend_class}`` mapping. The built-in
        ``file`` backend is always present. Broken third-party backends
        (``.load()`` raising) are skipped rather than crashing discovery.
        """
        backends: dict[str, type] = {}
        try:
            eps = importlib.metadata.entry_points()
            group = (
                eps.select(group=LOG_BACKEND_ENTRY_POINT_GROUP)
                if hasattr(eps, "select")
                else eps.get(LOG_BACKEND_ENTRY_POINT_GROUP, [])
            )
        except Exception:  # pragma: no cover - defensive; shouldn't happen
            group = []

        for ep in group:
            try:
                backend_class = ep.load()
            except Exception:
                # A broken third-party backend must not break discovery.
                continue
            backends[ep.name] = backend_class

        # Built-ins are always available and win over any registration gaps.
        backends.update(BUILTIN_BACKENDS)
        return backends

    def validate_config(self) -> None:
        """Delegate to backend's validate_config."""
        return self._backend.validate_config()

    def scan(
        self,
        filt: LogFilter,
        *,
        since: datetime | None,
        until: datetime | None,
        limit: int,
        tail_lines: int,
    ) -> ScanResult:
        """Delegate to backend's scan."""
        return self._backend.scan(
            filt,
            since=since,
            until=until,
            limit=limit,
            tail_lines=tail_lines,
        )

    def await_one(
        self,
        filt: LogFilter,
        *,
        since: datetime | None,
        timeout_s: float,
        poll_interval_ms: int,
    ) -> AwaitResult:
        """Delegate to backend's await_one."""
        return self._backend.await_one(
            filt,
            since=since,
            timeout_s=timeout_s,
            poll_interval_ms=poll_interval_ms,
        )

    def follow(self, filt: LogFilter, *, stop_event, poll_interval_ms: int):
        """Delegate to backend's follow."""
        return self._backend.follow(
            filt,
            stop_event=stop_event,
            poll_interval_ms=poll_interval_ms,
        )

    def sample_schema(self, *, sample_lines: int = 100) -> SchemaDescriptor:
        """Delegate to backend's sample_schema."""
        return self._backend.sample_schema(sample_lines=sample_lines)
