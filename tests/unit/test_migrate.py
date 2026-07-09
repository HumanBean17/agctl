"""Tests for `agctl config migrate` (v1/v2 -> v3).

Scope: the pure helper :func:`agctl.config.migrate.migrate_config` rewrites a
v1/v2 config to dialect ``"3"`` — v1 inputs get the jq-prefix rewrite
(``.body | `` on HTTP ``match.jq``, ``.value | `` on Kafka ``match``) AND the
structural lift of a flat ``kafka:`` block into ``kafka.clusters.default``; v2
inputs get only the structural lift (their match exprs are already
envelope-rooted). ``version`` bumps to ``"3"``. The ``config migrate`` Click
command backs up the file and writes the rewrite (or previews with
``--dry-run``).

Layering: ``migrate_config`` is a pure dict->dict transform in
``agctl/config/migrate.py``; the Click command in ``config_commands.py`` does
the file I/O and envelope emit (mirroring ``config_validate`` /
``config_init`` -- it does its OWN load+emit, no ``@envelope``).
"""

import copy
import json

import yaml
from click.testing import CliRunner

from agctl.cli import cli
from agctl.config import load_config
from agctl.config.migrate import TO_VERSION, MigrateResult, migrate_config


# --- helper (jq-prefix) tests ------------------------------------------------


def test_migrate_http_stub_match_jq():
    """A v1 config with ``mocks.http.stubs.<name>.match.jq`` -> prefix
    ``.body | ``, bump version to "3", record one rewrite."""
    config = {
        "version": "1",
        "mocks": {"http": {"stubs": {"s": {"match": {"jq": ".amount > 1000"}}}}},
    }
    result = migrate_config(config)
    assert result.config["version"] == "3"
    assert result.already_current is False
    assert result.from_version == "1"
    assert result.to_version == "3"
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
    """A v1 Kafka reactor ``match`` (string) -> prefix ``.value | ``."""
    config = {
        "version": "1",
        "mocks": {"kafka": {"reactors": {"r": {"match": ".command == \"X\""}}}},
    }
    result = migrate_config(config)
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


def test_migrate_kafka_pattern_match_no_flat_block():
    """A v1 ``kafka.patterns.<name>.match`` whose ``kafka:`` block carries ONLY
    patterns (no flat broker keys) -> prefix ``.value | ``; no structural lift
    (nothing to lift)."""
    config = {
        "version": "1",
        "kafka": {"patterns": {"p": {"match": ".eventType == \"Y\""}}},
    }
    result = migrate_config(config)
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


def test_to_version_is_three():
    """TO_VERSION constant and the default to_version field are both "3"."""
    assert TO_VERSION == "3"
    # A migrated result's to_version always equals TO_VERSION.
    result = migrate_config({"version": "1"})
    assert result.to_version == "3"


def test_migrate_idempotent_v3():
    """Re-running on an already-v3 config is a no-op: already_current True,
    rewrites empty, deep-copied unchanged."""
    already = {
        "version": "3",
        "kafka": {
            "clusters": {"main": {"brokers": ["h:9092"]}},
            "default_cluster": "main",
        },
    }
    result = migrate_config(already)
    assert isinstance(result, MigrateResult)
    assert result.already_current is True
    assert result.rewrites == []
    assert result.config == already
    assert result.config is not already  # deep copy, not the same object


def test_migrate_v2_does_not_jq_rewrite_envelope_exprs():
    """A v2 config's envelope-rooted expr (``.body.amount``, no prefix) is NOT
    touched: jq-prefix walkers run only on v1 sources. The version still bumps
    to "3" (v2 is no longer current), but the expr is unchanged."""
    config = {
        "version": "2",
        "mocks": {"http": {"stubs": {"s": {"match": {"jq": ".body.amount"}}}}},
    }
    result = migrate_config(config)
    assert result.already_current is False
    assert result.from_version == "2"
    # No jq rewrite recorded.
    assert result.rewrites == []
    assert result.config["version"] == "3"
    # The v2-native expr is unchanged -- not double-prefixed.
    assert (
        result.config["mocks"]["http"]["stubs"]["s"]["match"]["jq"]
        == ".body.amount"
    )


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
    result = migrate_config(config)
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
    still bumped to "3", no crash."""
    result = migrate_config({"version": "1"})
    assert result.rewrites == []
    assert result.config["version"] == "3"
    assert result.already_current is False


def test_migrate_strips_version_whitespace():
    """``migrate_config`` must compute ``source_major`` with ``.strip()``
    exactly as the loader's ``_check_version`` does. A loader-accepted
    ``version: " 3 "`` is therefore already-current here too -- a v3-native expr
    is NOT force-rewritten."""
    config = {
        "version": " 3 ",
        "kafka": {"clusters": {"main": {"brokers": ["h:9092"]}}},
    }
    result = migrate_config(config)
    assert result.already_current is True
    assert result.rewrites == []


# --- structural lift (v2 -> v3) ---------------------------------------------


def test_migrate_flat_v2_kafka_lifted_to_clusters_default():
    """A flat v2 ``kafka:`` block is lifted into ``kafka.clusters.default``;
    ``default_cluster`` is set; ``patterns`` preserved; version bumped to "3".

    v2 source -> no jq rewrite (exprs already envelope-rooted), only the
    structural lift fires.
    """
    config = {
        "version": "2",
        "kafka": {
            "brokers": ["h:9092"],
            "timeout_seconds": 30,
            "default_consumer_group": "g",
            "patterns": {"p": {"topic": "t", "match": ".value.x"}},
        },
    }
    result = migrate_config(config)
    assert result.already_current is False
    assert result.from_version == "2"
    assert result.config["version"] == "3"
    assert result.config["kafka"]["clusters"]["default"]["brokers"] == ["h:9092"]
    assert result.config["kafka"]["clusters"]["default"]["timeout_seconds"] == 30
    assert (
        result.config["kafka"]["clusters"]["default"]["default_consumer_group"]
        == "g"
    )
    assert result.config["kafka"]["default_cluster"] == "default"
    # patterns preserved unchanged (the v2 match is already envelope-rooted).
    assert result.config["kafka"]["patterns"] == {
        "p": {"topic": "t", "match": ".value.x"}
    }

    # Structural rewrites, deterministic order:
    # brokers, timeout_seconds, default_consumer_group, default_cluster.
    structural = [r for r in result.rewrites if r["path"].startswith("kafka.")]
    assert structural == [
        {
            "path": "kafka.clusters.default.brokers",
            "before": ["h:9092"],
            "after": ["h:9092"],
        },
        {
            "path": "kafka.clusters.default.timeout_seconds",
            "before": 30,
            "after": 30,
        },
        {
            "path": "kafka.clusters.default.default_consumer_group",
            "before": "g",
            "after": "g",
        },
        {"path": "kafka.default_cluster", "before": None, "after": "default"},
    ]


def test_migrate_flat_v2_full_lift_order():
    """All five flat keys lift in deterministic order
    (brokers, ssl, timeout_seconds, default_consumer_group, schema_registry_url),
    then default_cluster is set."""
    config = {
        "version": "2",
        "kafka": {
            "brokers": ["h:9092"],
            "ssl": {"ca_location": "/ca.pem"},
            "timeout_seconds": 15,
            "default_consumer_group": "grp",
            "schema_registry_url": "http://sr:8081",
        },
    }
    result = migrate_config(config)
    cluster = result.config["kafka"]["clusters"]["default"]
    assert cluster["brokers"] == ["h:9092"]
    assert cluster["ssl"] == {"ca_location": "/ca.pem"}
    assert cluster["timeout_seconds"] == 15
    assert cluster["default_consumer_group"] == "grp"
    assert cluster["schema_registry_url"] == "http://sr:8081"

    paths = [r["path"] for r in result.rewrites]
    assert paths == [
        "kafka.clusters.default.brokers",
        "kafka.clusters.default.ssl",
        "kafka.clusters.default.timeout_seconds",
        "kafka.clusters.default.default_consumer_group",
        "kafka.clusters.default.schema_registry_url",
        "kafka.default_cluster",
    ]


def test_migrate_v1_one_pass_jq_and_structural():
    """A v1 config (flat kafka + a ``.value``-less kafka pattern match) migrates
    to v3 in ONE pass: the jq prefix is applied AND the structural lift occurs.
    """
    config = {
        "version": "1",
        "kafka": {
            "brokers": ["h:9092"],
            "patterns": {
                "p": {"topic": "t", "match": '.eventType == "Y"'}
            },
        },
    }
    result = migrate_config(config)
    assert result.config["version"] == "3"
    assert result.from_version == "1"
    # jq prefix applied.
    assert (
        result.config["kafka"]["patterns"]["p"]["match"]
        == '.value | .eventType == "Y"'
    )
    # structural lift applied.
    assert result.config["kafka"]["clusters"]["default"]["brokers"] == ["h:9092"]
    assert result.config["kafka"]["default_cluster"] == "default"

    paths = [r["path"] for r in result.rewrites]
    # jq rewrite first, then structural rewrites.
    assert paths[0] == "kafka.patterns.p.match"
    assert "kafka.clusters.default.brokers" in paths
    assert "kafka.default_cluster" in paths


def test_migrate_already_clustered_kafka_not_lifted():
    """A v2 config whose ``kafka:`` already has a ``clusters`` key is NOT
    re-lifted (defensive: never double-lift). Only version bumps."""
    config = {
        "version": "2",
        "kafka": {
            "clusters": {"main": {"brokers": ["h:9092"]}},
            "default_cluster": "main",
        },
    }
    result = migrate_config(config)
    assert result.config["version"] == "3"
    # No structural rewrites (already clustered).
    assert [r for r in result.rewrites if r["path"].startswith("kafka.")] == []
    assert result.config["kafka"]["clusters"]["main"]["brokers"] == ["h:9092"]


def test_migrate_does_not_mutate_input():
    """``migrate_config`` never mutates its input dict (deep-copy first)."""
    config = {
        "version": "2",
        "kafka": {"brokers": ["h:9092"], "patterns": {"p": {"topic": "t"}}},
    }
    snapshot = copy.deepcopy(config)
    result = migrate_config(config)
    assert config == snapshot  # input untouched
    # The output is a separate object.
    assert result.config is not config
    assert result.config["kafka"] is not config["kafka"]


# --- command tests -----------------------------------------------------------


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
        '          jq: ".amount > 1000"\n'
    )
    cfg.write_text(original)

    result = _migrate(tmp_path, ["--config", str(cfg), "--dry-run"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ok"] is True
    assert payload["result"]["already_current"] is False
    assert payload["result"]["rewritten"][0]["after"] == ".body | .amount > 1000"

    # File unchanged on disk; no backup written.
    assert cfg.read_text() == original
    assert not (tmp_path / "agctl.yaml.bak").exists()


def test_config_migrate_writes_file_and_backup(tmp_path):
    """Without ``--dry-run``: a ``.bak`` of the original is created, the file
    is rewritten with `version: "3"` and the prefixed exprs, and the rewritten
    file round-trips through ``load_config`` (proving a valid v3 config).

    The v1 fixture is a *complete* config exercising all three match-site
    families (HTTP stub ``match.jq``, Kafka reactor ``match``, a
    ``kafka.patterns`` entry) plus a ``capture`` (which migration must leave
    untouched) -- so the migrated file passes Pydantic validation on load."""
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

    # Rewritten file: version 3, all three match exprs prefixed.
    rewritten = yaml.safe_load(cfg.read_text())
    assert rewritten["version"] == "3"
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
    # capture.from is NOT a match site -- migration leaves it untouched.
    assert (
        rewritten["mocks"]["http"]["stubs"]["s"]["capture"]["cid"]["from"]
        == ".body.correlationId"
    )

    # Round-trip: the rewritten file loads cleanly via the v3 gate.
    loaded = load_config(str(cfg), env={})
    assert loaded.mocks.http.stubs["s"].match.jq == ".body | .amount > 1000"
    assert loaded.mocks.kafka.reactors["r"].match == '.value | .command == "X"'
    assert loaded.kafka.patterns["p"].match == '.value | .eventType == "Y"'


def test_config_migrate_already_v3(tmp_path):
    """A v3 config: ``already_current=True``, no rewrites, no ``.bak``, file
    unchanged."""
    cfg = tmp_path / "agctl.yaml"
    original = (
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    main:\n"
        "      brokers:\n"
        "        - host:9092\n"
    )
    cfg.write_text(original)

    result = _migrate(tmp_path, ["--config", str(cfg)])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["already_current"] is True
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
        '          jq: ".amount > 1000"\n'
    )

    result = _migrate(tmp_path, ["--config", str(cfg), "--dry-run"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    note = payload["result"]["cli_flags_note"]
    assert isinstance(note, str)
    assert "--match" in note


def test_config_migrate_cli_flags_note_v1_includes_prefix_instruction(tmp_path):
    """A v1->v3 migration DID jq-prefix the config's match expressions, so the
    ``cli_flags_note`` must carry the manual ``.body |`` / ``.value |`` prefix
    guidance for the CLI flags it cannot reach."""
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "1"\n'
        "mocks:\n"
        "  http:\n"
        "    stubs:\n"
        "      s:\n"
        "        match:\n"
        '          jq: ".amount > 1000"\n'
    )

    result = _migrate(tmp_path, ["--config", str(cfg), "--dry-run"])

    assert result.exit_code == 0
    note = json.loads(result.output)["result"]["cli_flags_note"]
    assert "--match" in note
    assert ".body | " in note
    assert ".value | " in note


def test_config_migrate_cli_flags_note_v2_omits_prefix_instruction(tmp_path):
    """A v2->v3 migration does NOT jq-prefix (v2 exprs are already
    envelope-rooted), so the ``cli_flags_note`` must NOT tell the operator to
    prefix — that would steer them into double-prefixing working scripts. The
    base reminder (flags live in scripts/prompts, not rewritten) is still
    present. Uses a flat-kafka v2 config so a real structural lift runs (i.e.
    this is a genuine migration, not already_current)."""
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "2"\n'
        "kafka:\n"
        "  brokers:\n"
        "    - host:9092\n"
    )

    result = _migrate(tmp_path, ["--config", str(cfg), "--dry-run"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["already_current"] is False
    # A structural rewrite ran -> this was a real v2->v3 migration.
    assert payload["result"]["rewritten"]
    note = payload["result"]["cli_flags_note"]
    # Base reminder present.
    assert "--match" in note
    # Prefix instruction MUST be absent for v2->v3.
    assert ".body | " not in note
    assert ".value | " not in note


def test_config_migrate_round_trip_proves_behavior(tmp_path):
    """After migrating a v1 config and loading the result, the rewritten HTTP
    stub ``match.jq`` actually matches an envelope with a qualifying body and
    rejects one without. Guards that the ``.body | `` prepend is semantically
    correct (envelope-rooted), not just syntactically present."""
    from agctl.assertions import jq_bool

    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "1"\n'
        "mocks:\n"
        "  http:\n"
        "    stubs:\n"
        "      s:\n"
        "        method: POST\n"
        "        path: /charge\n"
        "        match:\n"
        '          jq: ".amount > 1000"\n'
        "        response:\n"
        "          status: 200\n"
    )

    result = _migrate(tmp_path, ["--config", str(cfg)])
    assert result.exit_code == 0

    loaded = load_config(str(cfg), env={})
    expr = loaded.mocks.http.stubs["s"].match.jq
    assert expr == ".body | .amount > 1000"

    matching = {
        "method": "POST",
        "path": "/x",
        "headers": {},
        "body": {"amount": 2000},
    }
    nonmatching = {
        "method": "POST",
        "path": "/x",
        "headers": {},
        "body": {"amount": 500},
    }
    assert jq_bool(matching, expr) is True
    assert jq_bool(nonmatching, expr) is False


def test_config_migrate_result_carries_formatting_note(tmp_path):
    """When the command actually writes the file, the result includes a
    ``formatting_note`` warning that ``yaml.safe_dump`` reformats / drops
    comments and the original is in ``<path>.bak``."""
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "1"\n'
        "mocks:\n"
        "  http:\n"
        "    stubs:\n"
        "      s:\n"
        "        match:\n"
        '          jq: ".amount > 1000"\n'
    )

    result = _migrate(tmp_path, ["--config", str(cfg)])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    note = payload["result"]["formatting_note"]
    assert isinstance(note, str)
    assert "safe_dump" in note
    assert ".bak" in note


def test_config_migrate_formatting_note_null_on_dry_run_and_already_v3(tmp_path):
    """``formatting_note`` is ``null`` when nothing is reformatted (--dry-run
    or already_current) -- the caveat would be noise in those cases."""
    # --dry-run: no write.
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "1"\n'
        "mocks:\n"
        "  http:\n"
        "    stubs:\n"
        "      s:\n"
        "        match:\n"
        '          jq: ".amount > 1000"\n'
    )
    dry = _migrate(tmp_path, ["--config", str(cfg), "--dry-run"])
    assert json.loads(dry.output)["result"]["formatting_note"] is None

    # already_current (v3): no write.
    cfg2 = tmp_path / "agctl2.yaml"
    cfg2.write_text(
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    main:\n"
        "      brokers: [host:9092]\n"
    )
    already = _migrate(tmp_path, ["--config", str(cfg2)])
    assert json.loads(already.output)["result"]["formatting_note"] is None


def test_migrate_preserves_env_interpolation(tmp_path):
    """``${VAR:-default}`` tokens are resolved at LOAD time, not migrate time --
    the rewritten file on disk must still contain them verbatim. Do NOT call
    ``load_config`` here (env unresolved)."""
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "1"\n'
        "services:\n"
        "  s:\n"
        '    base_url: "${ORDER_SERVICE_URL:-http://fallback}"\n'
        "mocks:\n"
        "  http:\n"
        "    stubs:\n"
        "      s:\n"
        "        match:\n"
        '          jq: ".amount > 1000"\n'
    )

    result = _migrate(tmp_path, ["--config", str(cfg)])
    assert result.exit_code == 0

    text = cfg.read_text()
    assert "${ORDER_SERVICE_URL:-http://fallback}" in text


def test_config_migrate_refuses_to_clobber_existing_backup(tmp_path):
    """A pre-existing ``<path>.bak`` is NOT silently overwritten (that would
    destroy the safety net). The command emits ``ok:false`` /
    ``error.type ConfigError`` / exit 2 and leaves BOTH files untouched."""
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "1"\n'
        "mocks:\n"
        "  http:\n"
        "    stubs:\n"
        "      s:\n"
        "        match:\n"
        '          jq: ".amount > 1000"\n'
    )
    original = cfg.read_text()

    bak = tmp_path / "agctl.yaml.bak"
    bak.write_text("PREVIOUS BACKUP - DO NOT LOSE\n")

    result = _migrate(tmp_path, ["--config", str(cfg)])

    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert payload["error"]["detail"]["backup"] == str(bak)

    # Both files untouched: config not rewritten, backup preserved.
    assert cfg.read_text() == original
    assert bak.read_text() == "PREVIOUS BACKUP - DO NOT LOSE\n"
