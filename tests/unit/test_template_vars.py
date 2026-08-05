"""Tests for ``agctl.template_vars`` — generator registry + ``parse_token``.

Covers Task 1 of the template-variables feature: the three built-in value
generators (``uuid`` / ``ts`` / ``rand``), the ``BUILTIN_GENERATORS`` registry,
and ``parse_token``. Substitution is Task 2 and is intentionally not tested
here.
"""

from __future__ import annotations

import re

import pytest

from agctl.errors import ConfigError
from agctl.template_vars import (
    BUILTIN_GENERATORS,
    parse_token,
)


# Regexes for individual generator output formats.
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
TS_SECONDS_RE = re.compile(r"^\d{10}$")
TS_MILLIS_RE = re.compile(r"^\d{13}$")
TS_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
RAND_HEX_RE = re.compile(r"^[0-9a-f]+$")

# Load-bearing charset invariant: every generator output (regardless of
# generator / opts) must stay within this character set so downstream consumers
# can rely on a restricted alphabet.
CHARSET_RE = re.compile(r"^[0-9a-fA-FT:Z -]+$")


# --- uuid generator -----------------------------------------------------------

def test_uuid_none_returns_single_lowercase_v4_uuid():
    out = BUILTIN_GENERATORS["uuid"](None)
    assert isinstance(out, list)
    assert len(out) == 1
    assert UUID_RE.match(out[0])


def test_uuid_count_returns_distinct_uuids():
    out = BUILTIN_GENERATORS["uuid"]("3")
    assert len(out) == 3
    assert all(UUID_RE.match(u) for u in out)
    assert len(set(out)) == 3


@pytest.mark.parametrize("opts", ["0", "-1", "abc"])
def test_uuid_invalid_opts_raises_config_error(opts):
    with pytest.raises(ConfigError):
        BUILTIN_GENERATORS["uuid"](opts)


# --- ts generator -------------------------------------------------------------

def test_ts_none_returns_unix_seconds_10_digits():
    out = BUILTIN_GENERATORS["ts"](None)
    assert len(out) == 1
    assert TS_SECONDS_RE.match(out[0])


def test_ts_ms_returns_unix_millis_13_digits():
    out = BUILTIN_GENERATORS["ts"]("ms")
    assert len(out) == 1
    assert TS_MILLIS_RE.match(out[0])


def test_ts_iso_returns_iso8601_utc():
    out = BUILTIN_GENERATORS["ts"]("iso")
    assert len(out) == 1
    assert TS_ISO_RE.match(out[0])


def test_ts_bogus_raises_config_error():
    with pytest.raises(ConfigError):
        BUILTIN_GENERATORS["ts"]("bogus")


# --- rand generator -----------------------------------------------------------

def test_rand_none_returns_16_hex_chars():
    out = BUILTIN_GENERATORS["rand"](None)
    assert len(out) == 1
    assert RAND_HEX_RE.match(out[0])
    assert len(out[0]) == 16


def test_rand_length_returns_n_hex_chars():
    out = BUILTIN_GENERATORS["rand"]("8")
    assert len(out) == 1
    assert len(out[0]) == 8
    assert RAND_HEX_RE.match(out[0])


@pytest.mark.parametrize("opts", ["0", "x"])
def test_rand_invalid_opts_raises_config_error(opts):
    with pytest.raises(ConfigError):
        BUILTIN_GENERATORS["rand"](opts)


# --- charset invariant (load-bearing property pin) ---------------------------

# Valid opts per generator drawn from the shared pool
# {None, "1", "2", "ms", "iso", "16", "32"}.
UUID_CHARSET_OPTS = [None, "1", "2", "16", "32"]
TS_CHARSET_OPTS = [None, "ms", "iso"]
RAND_CHARSET_OPTS = [None, "1", "2", "16", "32"]


@pytest.mark.parametrize(
    "gen_name, opts",
    [
        *[(g, o) for g in ("uuid",) for o in UUID_CHARSET_OPTS],
        *[(g, o) for g in ("ts",) for o in TS_CHARSET_OPTS],
        *[(g, o) for g in ("rand",) for o in RAND_CHARSET_OPTS],
    ],
)
def test_generator_output_charset_invariant(gen_name, opts):
    out = BUILTIN_GENERATORS[gen_name](opts)
    assert len(out) >= 1
    for s in out:
        assert CHARSET_RE.match(s), (
            f"{gen_name}({opts!r}) produced {s!r} outside charset"
        )


# --- parse_token --------------------------------------------------------------

@pytest.mark.parametrize(
    "interior, expected",
    [
        ("uuid", ("uuid", None)),
        ("uuid:2", ("uuid", "2")),
        ("ts:ms", ("ts", "ms")),
    ],
)
def test_parse_token_splits_on_first_colon(interior, expected):
    assert parse_token(interior) == expected


def test_parse_token_keeps_opts_verbatim_after_first_colon():
    # Everything after the first colon is opts verbatim; the generator
    # validates it, not parse_token.
    assert parse_token("rand:16") == ("rand", "16")
