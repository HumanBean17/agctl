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

from ..prime_content import TOPIC_ORDER, render_all, render_core, topic_text

__all__ = ["prime"]


@click.command("prime")
@click.option(
    "--topic",
    "topics",
    multiple=True,
    help="Emit one depth topic verbatim (repeatable; see `agctl prime` for the list).",
)
@click.option(
    "--all",
    "all_",
    is_flag=True,
    default=False,
    help="Emit the core manual plus every topic (registry order).",
)
def prime(topics: tuple[str, ...], all_: bool) -> None:
    """Print the agent manual (output envelope, exit codes, intent→command
    map, gotchas; `--help` of subcommands is the flag spec)."""
    if all_ and topics:
        raise click.UsageError("--all and --topic are mutually exclusive.")
    if all_:
        click.echo(render_all())
        return
    if not topics:
        click.echo(render_core())
        return
    texts = []
    for name in topics:
        text = topic_text(name)
        if text is None:
            raise click.UsageError(
                f"Unknown topic '{name}'. "
                f"Valid topics: {', '.join(TOPIC_ORDER)}"
            )
        texts.append(text)
    click.echo("\n\n".join(texts))
