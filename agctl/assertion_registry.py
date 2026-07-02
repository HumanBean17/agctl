"""Pluggable assertion-mode registry (DESIGN §9.3).

New assertion modes for ``db assert`` / ``kafka assert`` are added by
subclassing :class:`Assertion` and registering an instance (or the class) with
an :class:`AssertionRegistry` keyed by its ``.name``. Third-party modes are
discovered via the ``agctl.assertions`` entry-point group.

This module is PRAGMATIC: it provides the extension point + discovery. The
built-in ``db assert`` / ``kafka assert`` command logic continues to live in
the command modules (``db_commands.py`` / ``kafka_commands.py``) — those modes
are also registered here by NAME so that:

(a) known built-in mode names are discoverable (``registry.names()``),
(b) an unknown mode resolves to a clear :class:`TemplateNotFound` error, and
(c) third-party entry points can extend the set of available modes.

The registry guarantees ``registry.get(name)`` returns an :class:`Assertion`
instance for any registered mode; calling ``evaluate`` on a built-in mode
raises :class:`NotImplementedError` because the real logic is in the command
layer.
"""

from __future__ import annotations

import importlib.metadata
from typing import Any, Iterable, Union

from .errors import TemplateNotFound

#: Entry-point group for third-party assertion modes (DESIGN §9.3).
ASSERTION_ENTRY_POINT_GROUP = "agctl.assertions"


def _entry_points(group: str) -> list:
    """Return entry points registered under ``group`` (3.11+ shim).

    Mirrors :func:`agctl.cli._entry_points`. Factored out so tests can
    monkeypatch discovery. Returns ``[]`` on any failure.
    """
    try:
        eps = importlib.metadata.entry_points()
        if hasattr(eps, "select"):
            return list(eps.select(group=group))
        return list(eps.get(group, []))
    except Exception:
        return []


class Assertion:
    """Base class for pluggable assertion modes (DESIGN §9.3).

    Subclasses set :attr:`name` (the mode key, e.g. ``'expect_rows'`` or a
    third-party ``'json_schema'``) and implement :meth:`evaluate`.

    ``evaluate`` receives a free-form ``context`` dict and returns a result
    dict with at least ``{"passed": bool}`` plus any assertion-specific
    fields. Either raise :class:`AssertionFailure` on failure (the command
    layer translates that) or return ``{"passed": False, ...}`` and let the
    command raise. Keep the contract simple: ``evaluate`` returns a dict;
    ``passed=False`` means fail.
    """

    name: str = ""

    def evaluate(self, context: dict) -> dict:  # pragma: no cover - interface
        raise NotImplementedError


class _BuiltInMode(Assertion):
    """Marker subclass for built-in modes.

    Built-in modes are implemented inline in the command layer; calling
    ``evaluate`` on one raises so a caller can't accidentally bypass the
    command's validated path. The registry registers these by NAME purely for
    discovery and unknown-mode rejection.
    """

    def evaluate(self, context: dict) -> dict:
        raise NotImplementedError(
            f"built-in mode '{self.name}' is implemented in the command layer"
        )


# The built-in mode names registered in the default registry.
_BUILT_IN_MODES: tuple[str, ...] = (
    "expect_rows",
    "expect_value",
    "contains",
    "match",
    "pattern",
)


class AssertionRegistry:
    """Registry of assertion modes keyed by :attr:`Assertion.name`."""

    def __init__(self) -> None:
        self._modes: dict[str, Assertion] = {}

    def register(
        self, assertion_cls_or_instance: Union[type, Assertion]
    ) -> None:
        """Register an :class:`Assertion` subclass or instance, keyed by ``.name``.

        A class is instantiated once; an instance is used as-is. A blank
        ``.name`` is ignored (no-op) so a misconfigured entry point can't
        shadow real modes.
        """
        if isinstance(assertion_cls_or_instance, Assertion):
            instance = assertion_cls_or_instance
        elif isinstance(assertion_cls_or_instance, type) and issubclass(
            assertion_cls_or_instance, Assertion
        ):
            instance = assertion_cls_or_instance()
        else:
            # Not an Assertion at all: ignore rather than corrupt the registry.
            return
        name = getattr(instance, "name", "") or ""
        if not name:
            return
        self._modes[name] = instance

    def get(self, name: str) -> Assertion:
        """Resolve a mode by name.

        Raises :class:`TemplateNotFound` (DESIGN §4.1) for unknown modes so the
        command layer's existing error handling maps it to a clean envelope.
        """
        try:
            return self._modes[name]
        except KeyError:
            raise TemplateNotFound(
                f"Unknown assertion mode: {name}", {"mode": name}
            )

    def names(self) -> list[str]:
        """Sorted list of registered mode names."""
        return sorted(self._modes)

    def load_entry_points(self) -> "AssertionRegistry":
        """Load third-party modes from the ``agctl.assertions`` group.

        Each load is wrapped in try/except so a single broken entry point is
        skipped (logged to stderr) rather than bricking the registry. Returns
        ``self`` for chaining.
        """
        import sys

        for ep in _entry_points(ASSERTION_ENTRY_POINT_GROUP):
            try:
                obj = ep.load()
            except Exception as exc:  # noqa: BLE001 - entry-point isolation
                print(
                    f"agctl: failed to load assertion mode {ep.name}: {exc}",
                    file=sys.stderr,
                )
                continue
            self.register(obj)
        return self


# --- module-level default registry -----------------------------------------


def _default_registry() -> AssertionRegistry:
    """Build a fresh registry with the built-in modes registered.

    Does NOT load entry points — entry-point loading happens once in
    :func:`get_default_registry` so the cached registry reflects the real
    environment.
    """
    reg = AssertionRegistry()
    for mode_name in _BUILT_IN_MODES:
        # A distinct subclass per name keeps each registered instance's
        # ``.name`` accurate for introspection.
        cls = type(f"_BuiltIn_{mode_name}", (_BuiltInMode,), {"name": mode_name})
        reg.register(cls)
    return reg


_DEFAULT_REGISTRY: AssertionRegistry | None = None


def get_default_registry() -> AssertionRegistry:
    """Return the cached default registry (built-ins + loaded entry points).

    Built once on first call; subsequent calls return the same instance so
    entry-point loading is not repeated.
    """
    global _DEFAULT_REGISTRY
    if _DEFAULT_REGISTRY is None:
        reg = _default_registry()
        reg.load_entry_points()
        _DEFAULT_REGISTRY = reg
    return _DEFAULT_REGISTRY


def evaluate_custom(name: str, context: dict, registry: "AssertionRegistry | None" = None):
    """Resolve ``name`` and run its ``evaluate(context)`` (DESIGN §9.3).

    This is the bridge that makes third-party ``agctl.assertions`` modes
    reachable from ``db assert --assertion <name>`` / ``kafka assert --assertion
    <name>``. ``context`` is a free-form dict the command builds for the mode
    (rows/messages + metadata).

    Returns ``(True, detail)`` on success. Raises:
    - :class:`TemplateNotFound` (exit 2) for an unknown mode name.
    - :class:`ConfigError` (exit 2) if ``name`` is a built-in mode (those have
      dedicated flags and intentionally raise NotImplementedError from
      ``evaluate``).
    - :class:`AssertionFailure` (exit 1) if the mode returns ``passed=False``
      or itself raises.

    ``registry`` defaults to :func:`get_default_registry` (monkeypatchable).
    """
    from .errors import AssertionFailure, ConfigError

    reg = registry if registry is not None else get_default_registry()
    mode = reg.get(name)  # TemplateNotFound for unknown name
    try:
        result = mode.evaluate(context)
    except AssertionFailure:
        raise
    except NotImplementedError:
        raise ConfigError(
            f"Built-in assertion mode '{name}' has dedicated flags; "
            "do not invoke it via --assertion",
            {"mode": name},
        )
    except Exception as exc:  # noqa: BLE001 - third-party mode isolation
        raise AssertionFailure(f"assertion '{name}' raised: {exc}", {"mode": name})

    if not isinstance(result, dict):
        raise AssertionFailure(
            f"assertion '{name}' returned a non-dict result", {"mode": name}
        )
    detail = {k: v for k, v in result.items() if k != "passed"}
    if not result.get("passed"):
        raise AssertionFailure(
            result.get("message") or f"assertion '{name}' did not pass",
            {"mode": name, **detail},
        )
    return True, detail
