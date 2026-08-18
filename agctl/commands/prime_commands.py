"""``agctl prime`` — emit the agent manual (spec:
``2026-08-19-agctl-prime-skills-replacement-design``).

A **config-free** documentation command, like ``discover``/``gen``: it reads
packaged markdown from :mod:`agctl.prime_content` and needs no ``agctl.yaml``.

prime is exempt from the one-JSON-envelope-per-invocation contract (a
``--help``-class exception — its consumer is an agent reading markdown, not a
program parsing results): raw markdown on stdout, exit 0 on success, exit 2
(``UsageError``) on bad usage only. It asserts nothing, so it never exits 1.
"""

from __future__ import annotations

import click

from ..prime_content import render_core

__all__ = ["prime"]


@click.command("prime")
def prime() -> None:
    """Print the agent manual (output envelope, exit codes, intent→command
    map, gotchas; `--help` of subcommands is the flag spec)."""
    click.echo(render_core())
