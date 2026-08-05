"""Tests for ``agctl.template_vars`` — generator registry + ``parse_token``.

Covers Task 1 of the template-variables feature (the three built-in value
generators ``uuid`` / ``ts`` / ``rand``, the ``BUILTIN_GENERATORS`` registry,
and ``parse_token``) and Task 2 (the ``substitute_generators`` walker and the
``find_unknown_templates`` validator helper).
"""

from __future__ import annotations

import re

import pytest

from agctl.errors import ConfigError
from agctl.template_vars import (
    BUILTIN_GENERATORS,
    find_unknown_templates,
    parse_token,
    substitute_generators,
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


# =============================================================================
# Task 2: substitute_generators walker + find_unknown_templates
# =============================================================================


# --- substitute_generators: string values -------------------------------------

def test_substitute_no_tokens_returns_input_unchanged():
    assert substitute_generators("no tokens here", {}) == "no tokens here"


def test_substitute_single_uuid_token_returns_36_char_lowercase_uuid():
    out = substitute_generators("{{uuid}}", {})
    assert UUID_RE.match(out), f"expected a UUID, got {out!r}"
    assert len(out) == 36
    assert out == out.lower()


def test_substitute_repeated_token_reuses_memo_value():
    # The full matched text is the memo key, so two identical tokens resolve
    # to the SAME generated value.
    out = substitute_generators("id={{uuid}} again {{uuid}}", {})
    left, right = out.split(" again ")
    assert left == "id=" + right
    assert UUID_RE.match(right)


def test_substitute_different_opts_yield_different_memo_keys():
    # {{uuid}} and {{uuid:2}} are different memo keys -> different values.
    # The {{uuid:2}} half is two space-separated UUIDs.
    out = substitute_generators("{{uuid}} vs {{uuid:2}}", {})
    left, _, right = out.partition(" vs ")
    assert UUID_RE.match(left)
    parts = right.split(" ")
    assert len(parts) == 2
    assert all(UUID_RE.match(p) for p in parts)
    # Different memo keys -> the single uuid differs from both in the pair.
    assert left != parts[0]
    assert left != parts[1]


def test_substitute_ts_token_yields_10_digit_seconds():
    out = substitute_generators("a={{ts}}", {})
    assert out.startswith("a=")
    digits = out[len("a="):]
    assert TS_SECONDS_RE.match(digits), f"expected 10 digits, got {digits!r}"


def test_substitute_rand_with_length_yields_n_hex_chars():
    out = substitute_generators("{{rand:8}}", {})
    assert RAND_HEX_RE.match(out)
    assert len(out) == 8


def test_substitute_non_matching_tokens_left_literal():
    src = "{{user.name}} and {{ x }}"
    # Dots, spaces, and unbalanced braces do not match TEMPLATE_RE.
    assert substitute_generators(src, {}) == src


def test_substitute_triple_braced_unknown_left_literal_no_error():
    # {{{x}}} must NOT match (the inner {{x}} is not a valid token here because
    # it is bracketed by extra braces). No substitution, no ConfigError.
    src = "{{{x}}}"
    assert substitute_generators(src, {}) == src


def test_substitute_triple_braced_known_generator_left_literal():
    # {{{uuid}}} likewise left literal — the inner {{uuid}} must NOT be
    # generated when surrounded by extra braces.
    src = "{{{uuid}}}"
    assert substitute_generators(src, {}) == src


def test_substitute_normal_token_still_substitutes_alongside_triple_braces():
    # A normal {{uuid}} in the same string still substitutes, while the
    # triple-braced occurrence stays literal.
    out = substitute_generators("{{uuid}} {{{uuid}}}", {})
    left, _, right = out.partition(" ")
    assert UUID_RE.match(left), f"expected a UUID, got {left!r}"
    assert right == "{{{uuid}}}"


def test_substitute_unknown_generator_raises_config_error():
    with pytest.raises(ConfigError) as exc_info:
        substitute_generators("{{nope}}", {})
    msg = str(exc_info.value)
    assert "nope" in msg
    # The message lists the valid generator names.
    for valid in BUILTIN_GENERATORS:
        assert valid in msg


def test_substitute_disabled_returns_value_unchanged():
    # enabled=False is the --no-template-vars path: no scan, no errors.
    # Even a bogus generator must NOT raise.
    assert substitute_generators("{{uuid}}", {}, enabled=False) == "{{uuid}}"
    assert substitute_generators("{{nope}}", {}, enabled=False) == "{{nope}}"


# --- substitute_generators: container recursion ------------------------------

def test_substitute_recurses_dict_and_list_reusing_memo():
    value = {
        "k": "{{uuid}}",
        "k2": "{{uuid}}",  # same token -> identical to k (memo reuse)
        "list": ["{{ts}}", "plain"],
    }
    out = substitute_generators(value, {})
    assert isinstance(out, dict)
    assert UUID_RE.match(out["k"])
    assert out["k"] == out["k2"], "same token text must reuse the memo value"
    assert isinstance(out["list"], list)
    assert TS_SECONDS_RE.match(out["list"][0])
    assert out["list"][1] == "plain"


def test_substitute_disabled_returns_container_unchanged():
    value = {"k": "{{uuid}}", "list": ["{{ts}}", "plain"]}
    out = substitute_generators(value, {}, enabled=False)
    assert out is value


# --- find_unknown_templates ---------------------------------------------------

def test_find_unknown_no_tokens_returns_empty():
    assert find_unknown_templates("no tokens here") == []


def test_find_unknown_known_generator_returns_empty():
    assert find_unknown_templates("{{uuid}} and {{ts:ms}}") == []


def test_find_unknown_returns_full_token_text():
    assert find_unknown_templates("{{nope}}") == ["{{nope}}"]


def test_find_unknown_dedupes_insertion_ordered():
    out = find_unknown_templates("{{a}} {{b}} {{a}} {{c}} {{b}}")
    assert out == ["{{a}}", "{{b}}", "{{c}}"]


def test_find_unknown_does_not_match_invalid_token_shapes():
    # Dots / spaces do not match TEMPLATE_RE -> not reported as unknown.
    assert find_unknown_templates("{{user.name}} and {{ x }}") == []


def test_find_unknown_recurses_containers():
    value = {
        "k": "{{nope}}",
        "list": ["{{uuid}}", "{{other}}", {"nested": "{{third}}"}],
    }
    out = find_unknown_templates(value)
    assert out == ["{{nope}}", "{{other}}", "{{third}}"]


def test_find_unknown_does_not_raise_on_bogus_opts():
    # No generation, no validation: even nonsense opts must not raise.
    assert find_unknown_templates("{{uuid:not-an-int}}") == []
    assert find_unknown_templates("{{nope:bogus}}") == ["{{nope:bogus}}"]
