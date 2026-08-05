"""``agctl gen uuid|ts|rand`` command group (template-vars Task 3).

A **config-free** group: each subcommand calls a Task-1 generator from
:data:`agctl.template_vars.BUILTIN_GENERATORS` directly and shapes the result.
They do NOT call :func:`agctl.command.load_config_or_raise` and succeed with no
``agctl.yaml`` present. They ignore ``--no-template-vars`` — they ARE generation.

Each subcommand is a thin Click wrapper around a ``_core`` function that is
wrapped by :func:`agctl.command.envelope`, mirroring the house pattern in
:mod:`agctl.commands.check_commands` (envelope tags ``gen.uuid`` / ``gen.ts`` /
``gen.rand``). Bad flags (non-integer / ``< 1`` / ``--ms`` + ``--iso``) become a
:class:`ConfigError` (exit 2) through the envelope: ``--count`` and ``--length``
are intentionally string-typed so the generators' own
:func:`~agctl.template_vars._parse_positive_int` validates them and raises
:class:`ConfigError`, rather than Click rejecting the value pre-envelope.
"""

from __future__ import annotations

import click

from ..command import envelope
from ..errors import ConfigError
from ..template_vars import BUILTIN_GENERATORS

__all__ = ["gen_group", "gen_uuid", "gen_ts", "gen_rand"]


# --- cores (pure; no config, no I/O beyond generation) --------------------- #


def _gen_uuid_core(count: str, upper: bool) -> dict:
    """Generate ``count`` UUIDs; shape single vs. multi.

    ``count`` is the raw string from Click; ``BUILTIN_GENERATORS["uuid"]``
    validates it (raising :class:`ConfigError` on non-int / ``< 1``).
    ``upper`` uppercases each UUID (command-only convenience).
    """
    parts = BUILTIN_GENERATORS["uuid"](str(count))
    if upper:
        parts = [p.upper() for p in parts]
    if len(parts) == 1:
        return {"value": parts[0]}
    return {"values": parts}


def _gen_ts_core(ms: bool, iso: bool) -> dict:
    """Generate one timestamp; ``--ms`` and ``--iso`` are mutually exclusive."""
    if ms and iso:
        raise ConfigError("Specify at most one of --ms and --iso", {})
    opts = "ms" if ms else ("iso" if iso else None)
    parts = BUILTIN_GENERATORS["ts"](opts)
    return {"value": parts[0]}


def _gen_rand_core(length: str) -> dict:
    """Generate one hex string of ``length`` chars.

    ``length`` is the raw string from Click; ``BUILTIN_GENERATORS["rand"]``
    validates it (raising :class:`ConfigError` on non-int / ``< 1``).
    """
    parts = BUILTIN_GENERATORS["rand"](str(length))
    return {"value": parts[0]}


# --- Click commands -------------------------------------------------------- #


@click.command("uuid")
@click.option(
    "--count",
    "count",
    type=str,
    default="1",
    metavar="INT",
    help="Number of UUIDs to generate (>= 1).",
)
@click.option(
    "--upper",
    "upper",
    is_flag=True,
    default=False,
    help="Uppercase the generated UUID(s).",
)
def gen_uuid(count: str, upper: bool) -> None:
    """Generate RFC-4122 v4 UUID(s)."""
    _gen_uuid_envelope(count, upper)


@click.command("ts")
@click.option("--ms", "ms", is_flag=True, default=False, help="Unix milliseconds (13 digits).")
@click.option(
    "--iso",
    "iso",
    is_flag=True,
    default=False,
    help="ISO-8601 UTC, e.g. 2026-08-05T12:00:00Z (exclusive with --ms).",
)
def gen_ts(ms: bool, iso: bool) -> None:
    """Generate a current timestamp (default: Unix seconds)."""
    _gen_ts_envelope(ms, iso)


@click.command("rand")
@click.option(
    "--length",
    "length",
    type=str,
    default="16",
    metavar="INT",
    help="Number of hex chars to generate (>= 1).",
)
def gen_rand(length: str) -> None:
    """Generate a random lowercase hex string (default 16 chars)."""
    _gen_rand_envelope(length)


# Envelope-wrapped cores (tags drive the ``command`` field of the output).
_gen_uuid_envelope = envelope("gen.uuid")(_gen_uuid_core)
_gen_ts_envelope = envelope("gen.ts")(_gen_ts_core)
_gen_rand_envelope = envelope("gen.rand")(_gen_rand_core)


# --- group ----------------------------------------------------------------- #


@click.group(name="gen")
def gen_group() -> None:
    """Generate standalone template-variable values (uuid, ts, rand)."""


gen_group.add_command(gen_uuid)
gen_group.add_command(gen_ts)
gen_group.add_command(gen_rand)
