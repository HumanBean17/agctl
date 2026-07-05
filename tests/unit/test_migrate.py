"""Tests for `agctl config migrate` (Task 6).

Scope: the pure helper :func:`agctl.config.migrate.migrate_match_exprs` rewrites
a v1 config to dialect ``"2"`` (prepend ``.body | `` to HTTP ``match.jq``,
``.value | `` to Kafka ``match``, bump ``version``). The ``config migrate``
Click command backs up the file and writes the rewrite (or previews with
``--dry-run``).

Layering: ``migrate_match_exprs`` is a pure dict→dict transform in
``agctl/config/migrate.py``; the Click command in ``config_commands.py`` does
the file I/O and envelope emit (mirroring ``config_validate`` /
``config_init`` — it does its OWN load+emit, no ``@envelope``).
"""

import json

import yaml
from click.testing import CliRunner

from agctl.cli import cli
from agctl.config import load_config
from agctl.config.migrate import migrate_match_exprs


# --- Step 1: helper tests ----------------------------------------------------


def test_migrate_http_stub_match_jq():
    """A v1 config with ``mocks.http.stubs.<name>.match.jq`` → prefix
    ``.body | ``, bump version to "2", record one rewrite."""
    config = {
        "version": "1",
        "mocks": {"http": {"stubs": {"s": {"match": {"jq": ".amount > 1000"}}}}},
    }
    result = migrate_match_exprs(config)
    assert result.config["version"] == "2"
    assert result.already_v2 is False
    assert result.from_version == "1"
    assert result.to_version == "2"
    assert (
        result.config["mocks"]["http"]["stubs"]["s"]["match"]["jq"]
        == ".body | .amount > 1000"
    )
    assert result.rewrites == [
        {
            "path": "mocks.http.stubs.s.match.jq",
            "before": ".amount > 1000",
            "after": ".body | .amount > 1000",
        }
    ]


def test_migrate_reactor_match():
    """A v1 Kafka reactor ``match`` (string) → prefix ``.value | ``."""
    config = {
        "version": "1",
        "mocks": {"kafka": {"reactors": {"r": {"match": ".command == \"X\""}}}},
    }
    result = migrate_match_exprs(config)
    assert (
        result.config["mocks"]["kafka"]["reactors"]["r"]["match"]
        == ".value | .command == \"X\""
    )
    assert result.rewrites == [
        {
            "path": "mocks.kafka.reactors.r.match",
            "before": ".command == \"X\"",
            "after": ".value | .command == \"X\"",
        }
    ]


def test_migrate_kafka_pattern_match():
    """A v1 ``kafka.patterns.<name>.match`` → prefix ``.value | ``."""
    config = {
        "version": "1",
        "kafka": {"patterns": {"p": {"match": ".eventType == \"Y\""}}},
    }
    result = migrate_match_exprs(config)
    assert (
        result.config["kafka"]["patterns"]["p"]["match"]
        == ".value | .eventType == \"Y\""
    )
    assert result.rewrites == [
        {
            "path": "kafka.patterns.p.match",
            "before": ".eventType == \"Y\"",
            "after": ".value | .eventType == \"Y\"",
        }
    ]


def test_migrate_idempotent_and_already_v2():
    """Re-running on an already-migrated config is a no-op; a v2-native config
    with a v2-style expr like ``.body.amount`` is NOT touched."""
    # (a) Idempotent: feed output of test 1 back in.
    once = migrate_match_exprs(
        {
            "version": "1",
            "mocks": {"http": {"stubs": {"s": {"match": {"jq": ".amount > 1000"}}}}},
        }
    )
    twice = migrate_match_exprs(once.config)
    assert twice.already_v2 is True
    assert twice.rewrites == []
    assert (
        twice.config["mocks"]["http"]["stubs"]["s"]["match"]["jq"]
        == ".body | .amount > 1000"
    )

    # (b) A v2-native config with a v2-native expr (no `.body | ` prefix) is
    # NOT rewritten — already_v2 short-circuits before traversal.
    native = migrate_match_exprs(
        {"version": "2", "mocks": {"http": {"stubs": {"s": {"match": {"jq": ".body.amount"}}}}}}
    )
    assert native.already_v2 is True
    assert native.rewrites == []
    assert native.config["mocks"]["http"]["stubs"]["s"]["match"]["jq"] == ".body.amount"


def test_migrate_leaves_capture_and_match_body_untouched():
    """``match.body`` dict and ``capture.*.from`` are NOT visited by the
    migration (out of scope per the brief)."""
    config = {
        "version": "1",
        "mocks": {
            "http": {
                "stubs": {
                    "s": {
                        "match": {"body": {"a": 1}, "jq": ".x"},
                        "capture": {"c": {"from": ".body.c"}},
                    }
                }
            }
        },
    }
    result = migrate_match_exprs(config)
    stub = result.config["mocks"]["http"]["stubs"]["s"]
    assert stub["match"]["jq"] == ".body | .x"
    # match.body dict is left as-is.
    assert stub["match"]["body"] == {"a": 1}
    # capture.*.from is left as-is.
    assert stub["capture"]["c"]["from"] == ".body.c"
    # Only the jq site shows up in rewrites.
    assert len(result.rewrites) == 1
    assert result.rewrites[0]["path"] == "mocks.http.stubs.s.match.jq"


def test_migrate_missing_sections_no_crash():
    """A v1 config with no ``mocks``/``kafka`` sections: no rewrites, version
    still bumped to "2", no crash."""
    result = migrate_match_exprs({"version": "1"})
    assert result.rewrites == []
    assert result.config["version"] == "2"
    assert result.already_v2 is False


# --- Step 5: command tests ---------------------------------------------------


def _migrate(tmp_path, args):
    """Invoke `agctl config migrate` and return the CliRunner result."""
    return CliRunner().invoke(cli, ["config", "migrate", *args])


def test_config_migrate_dry_run_writes_nothing(tmp_path):
    """``--dry-run`` reports the rewrite but does NOT touch the file, and no
    ``.bak`` is created."""
    cfg = tmp_path / "agctl.yaml"
    original = (
        'version: "1"\n'
        "mocks:\n"
        "  http:\n"
        "    stubs:\n"
        "      s:\n"
        "        match:\n"
        "          jq: \".amount > 1000\"\n"
    )
    cfg.write_text(original)

    result = _migrate(tmp_path, ["--config", str(cfg), "--dry-run"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ok"] is True
    assert payload["result"]["already_v2"] is False
    assert payload["result"]["rewritten"][0]["after"] == ".body | .amount > 1000"

    # File unchanged on disk; no backup written.
    assert cfg.read_text() == original
    assert not (tmp_path / "agctl.yaml.bak").exists()


def test_config_migrate_writes_file_and_backup(tmp_path):
    """Without ``--dry-run``: a ``.bak`` of the original is created, the file
    is rewritten with `version: "2"` and the prefixed exprs, and the rewritten
    file round-trips through ``load_config`` (proving a valid v2 config).

    The v1 fixture is a *complete* config exercising all three match-site
    families (HTTP stub ``match.jq``, Kafka reactor ``match``, a
    ``kafka.patterns`` entry) plus a ``capture`` (which migration must leave
    untouched) — so the migrated file passes Pydantic validation on load."""
    cfg = tmp_path / "agctl.yaml"
    original = (
        'version: "1"\n'
        "mocks:\n"
        "  http:\n"
        "    stubs:\n"
        "      s:\n"
        "        method: POST\n"
        "        path: /charge\n"
        "        match:\n"
        '          jq: ".amount > 1000"\n'
        "        capture:\n"
        "          cid:\n"
        "            from: .body.correlationId\n"
        "        response:\n"
        "          status: 200\n"
        "  kafka:\n"
        "    reactors:\n"
        "      r:\n"
        "        topic: commands\n"
        '        match: \'.command == "X"\'\n'
        "        reaction:\n"
        "          topic: events\n"
        "          value: ok\n"
        "kafka:\n"
        "  patterns:\n"
        "    p:\n"
        "      topic: orders\n"
        '      match: \'.eventType == "Y"\'\n'
    )
    cfg.write_text(original)

    result = _migrate(tmp_path, ["--config", str(cfg)])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["rewritten"][0]["after"] == ".body | .amount > 1000"

    # Backup exists with the ORIGINAL content.
    bak = tmp_path / "agctl.yaml.bak"
    assert bak.exists()
    assert bak.read_text() == original

    # Rewritten file: version 2, all three match exprs prefixed.
    rewritten = yaml.safe_load(cfg.read_text())
    assert rewritten["version"] == "2"
    assert (
        rewritten["mocks"]["http"]["stubs"]["s"]["match"]["jq"]
        == ".body | .amount > 1000"
    )
    assert (
        rewritten["mocks"]["kafka"]["reactors"]["r"]["match"]
        == '.value | .command == "X"'
    )
    assert (
        rewritten["kafka"]["patterns"]["p"]["match"]
        == '.value | .eventType == "Y"'
    )
    # capture.from is NOT a match site — migration leaves it untouched.
    assert (
        rewritten["mocks"]["http"]["stubs"]["s"]["capture"]["cid"]["from"]
        == ".body.correlationId"
    )

    # Round-trip: the rewritten file loads cleanly via the v2 gate.
    loaded = load_config(str(cfg), env={})
    assert loaded.mocks.http.stubs["s"].match.jq == ".body | .amount > 1000"
    assert loaded.mocks.kafka.reactors["r"].match == '.value | .command == "X"'
    assert loaded.kafka.patterns["p"].match == '.value | .eventType == "Y"'


def test_config_migrate_already_v2(tmp_path):
    """A v2 config: ``already_v2=True``, no rewrites, no ``.bak``, file
    unchanged."""
    cfg = tmp_path / "agctl.yaml"
    original = (
        'version: "2"\n'
        "mocks:\n"
        "  http:\n"
        "    stubs:\n"
        "      s:\n"
        "        match:\n"
        "          jq: \".body | .amount > 1000\"\n"
    )
    cfg.write_text(original)

    result = _migrate(tmp_path, ["--config", str(cfg)])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["already_v2"] is True
    assert payload["result"]["rewritten"] == []
    assert not (tmp_path / "agctl.yaml.bak").exists()
    assert cfg.read_text() == original


def test_config_migrate_result_carries_cli_flags_note(tmp_path):
    """The emitted ``result`` includes a ``cli_flags_note`` reminding the
    operator that CLI ``--match`` flags are NOT rewritten by this command."""
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "1"\n'
        "mocks:\n"
        "  http:\n"
        "    stubs:\n"
        "      s:\n"
        "        match:\n"
        "          jq: \".amount > 1000\"\n"
    )

    result = _migrate(tmp_path, ["--config", str(cfg), "--dry-run"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    note = payload["result"]["cli_flags_note"]
    assert isinstance(note, str)
    assert "--match" in note
