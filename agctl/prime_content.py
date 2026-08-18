"""The prime content channel — wheel-hosted agent documentation (spec:
``2026-08-19-agctl-prime-skills-replacement-design``).

All agent-facing knowledge ships as markdown data files under
``agctl/data/prime/`` (plus the router skill stub under
``agctl/data/skills/``) and is emitted on demand by ``agctl prime``. Because
the content rides the wheel, it can never contradict the installed binary.

This module is content access only — loading, the topic registry, and
rendering. Presentation lives in :mod:`agctl.commands.prime_commands`.

Reading mirrors :func:`agctl.commands.config_commands._load_sample`:
``importlib.resources`` (wheel + editable installs) with CRLF collapsed to LF
so a Windows checkout stays byte-consistent.
"""

from __future__ import annotations

import importlib.resources
from typing import Final

from . import __version__
from .errors import ConfigError

__all__ = [
    "HOOK_SETTINGS_SNIPPET",
    "TOPIC_ORDER",
    "hook_pointer",
    "read_resource",
    "render_all",
    "render_core",
    "topic_text",
]


#: Ordered topic registry. ``agctl prime --all`` emits these in this order and
#: ``core.md``'s depth index lists them; tests pin the two against the packaged
#: files (no dangling entries, no orphan files).
TOPIC_ORDER: tuple[str, ...] = (
    "gotchas",
    "mock",
    "listen",
    "grpc",
    "config",
    "config-http",
    "config-kafka",
    "config-db",
    "config-db-write",
    "config-mocks",
    "config-logs",
    "config-init",
    "runbook-write",
    "runbook-run",
)


def read_resource(*parts: str) -> str:
    """Read a packaged text resource under the ``agctl`` package.

    Applies the same LF normalization as the sample-config loader (see
    :func:`agctl.commands.config_commands._load_sample`) so CRLF checkouts on
    Windows read byte-identically to LF checkouts. Raises
    :class:`ConfigError` when the resource is missing (incomplete build).
    """
    try:
        root = importlib.resources.files("agctl")
        text = root.joinpath(*parts).read_text(encoding="utf-8")
        return text.replace("\r\n", "\n").replace("\r", "\n")
    except (FileNotFoundError, OSError) as err:
        raise ConfigError(
            f"Resource not found in the agctl package: {err}",
            detail={"resource": "/".join(parts)},
        ) from err


def _render(text: str) -> str:
    """Substitute the ``{version}`` token.

    Uses ``str.replace`` — NOT ``str.format`` — because the content contains
    JSON/YAML braces that ``format`` would try to interpret.
    """
    return text.replace("{version}", __version__)


def render_core() -> str:
    """The default ``agctl prime`` output: the lean core manual."""
    return _render(read_resource("data", "prime", "core.md"))


def topic_text(name: str) -> str | None:
    """Raw text of one topic, or ``None`` for an unregistered name."""
    if name not in TOPIC_ORDER:
        return None
    return read_resource("data", "prime", f"{name}.md")


def render_all() -> str:
    """``--all``: the core manual followed by every topic, registry order."""
    topics = "\n\n".join(topic_text(name) for name in TOPIC_ORDER)
    return render_core() if not topics else f"{render_core()}\n\n{topics}"


def hook_pointer() -> str:
    """The SessionStart pointer: a ≤ 800-char nudge toward ``agctl prime``.

    Wired as ``agctl prime --hook-json`` in the consumer's
    ``.claude/settings.json`` (see :data:`HOOK_SETTINGS_SNIPPET`). Deliberately
    tiny — a hook fires every session in the repo, so it must cost less than
    the one skill-description it replaces.
    """
    return _render(read_resource("data", "prime", "hook.md"))


#: Ready-to-paste ``.claude/settings.json`` fragment for the SessionStart
#: hook. Printed by ``agctl config init`` (never written to settings.json —
#: wiring a hook is the consumer's deliberate choice) and mirrored in the
#: README (a drift test pins the two together).
HOOK_SETTINGS_SNIPPET: Final[str] = (
    '{"hooks": {"SessionStart": '
    '[{"hooks": [{"type": "command", "command": "agctl prime --hook-json"}]}]}}'
)
