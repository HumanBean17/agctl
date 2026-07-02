"""Protocol plugin contract (DESIGN §9.2).

Third-party protocol plugins — new top-level command groups such as gRPC,
GraphQL, or WebSocket — register a :class:`Plugin` via the ``agctl.plugins``
entry-point group. ``agctl.cli`` loads each plugin's ``command_group`` onto the
root CLI and invokes ``validate_config`` during ``agctl config validate`` so a
plugin can reject bad configuration at startup.

The loader is duck-typed: a plugin object need only expose ``command_group`` to
be registered, and ``name`` / ``validate_config`` are optional but recommended.
A third-party plugin is therefore free to explicitly implement this
:class:`Protocol` or simply expose the same attributes/methods.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import click


@runtime_checkable
class Plugin(Protocol):
    """Contract for ``agctl.plugins`` entry points (DESIGN §9.2).

    Attributes:
        name: Subcommand name registered on the root CLI. When absent the loader
            falls back to ``command_group.name`` and then the entry-point name.
        command_group: The :class:`click.Group` exposing this plugin's commands.

    Methods:
        validate_config: Validate the fully-resolved configuration (a plain
            ``dict``). Return a list of human-readable error strings; an empty
            list (or ``None``) means valid. Invoked during ``agctl config
            validate`` with the resolved config so a plugin can reject bad
            config for its own section before any command runs.
    """

    name: str
    command_group: click.Group

    def validate_config(self, config: dict) -> list[str]: ...
