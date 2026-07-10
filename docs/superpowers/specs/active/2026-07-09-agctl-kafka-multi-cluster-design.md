# Design: Multi-Cluster Kafka Support

**Status:** Approved (design) — ready for implementation plan
**Date:** 2026-07-09
**Author:** brainstorming session
**Affects:** `agctl/config/models.py`, `agctl/config/loader.py`, `agctl/config/migrate.py`, `agctl/config/validator.py`, `agctl/commands/kafka_commands.py`, `agctl/commands/mock_commands.py`, `agctl/commands/discover_commands.py`, `agctl/mock/engine.py`, `agctl/data/sample-config.yaml`
**Relation to docs:** **Changes the `agctl` config contract.** `DESIGN.md` §2.1 (`kafka:` schema), §3.2 (`kafka` CLI), §3.5 (`mocks.kafka.reactors`), and `ARCHITECTURE.md` §5 (load/version pipeline) and §8 (KafkaClient construction) require updates — this is **not** a docs no-op. `docs-watcher` runs after implementation per CLAUDE.md "Docs Sync".

---

## 1. Background & Problem

`agctl` drives every named resource through a **named map**: `services` (`Config.services: dict[str, ServiceConfig]`), `database.connections` (`dict[str, DatabaseConnection]`), `logs.sources` (`dict[str, LogSource]`). The one exception is Kafka.

`Config.kafka` is a **single object**, not a map (`config/models.py:308` → `KafkaConfig`, `models.py:113`): one `brokers` list, one `ssl` block, one `timeout_seconds`, one `default_consumer_group`, one `patterns` map. Every `kafka` command resolves that single object with no selector — `kafka_commands.py` builds the client via `new_kafka_client(cfg.kafka, ...)` in `produce` (`:128`), `consume` (`:208`), and `assert` (`:421`, `:462`). There is **no `--cluster` flag** anywhere.

This means `agctl` cannot talk to two Kafka brokers in one config — a real gap for systems that span a primary cluster and an analytics/mirror cluster, or prod + staging side-by-side. It is also asymmetric with `database.connections`, which already solved the named-instance + default + per-template-binding problem.

A side effect: `mocks.kafka.reactors` (`KafkaReactor`, `models.py:256`) are pinned to that one broker too. The validator errors when reactors exist with no top-level `kafka.brokers` (`validator.py:100`); the `MockEngine` builds each reactor's client against `cfg.kafka` (`mock/engine.py:196-203`). So multi-cluster must reach the mock layer too.

## 2. Goals

- Support **N named Kafka clusters** in one `agctl.yaml`, each with its own brokers / TLS / timeout / consumer group.
- Mirror the proven `database.connections` model exactly: a named map, a default, and per-binding selection — so the pattern is familiar and the resolution rules are identical in spirit to `resolve_connection_name` (`db_commands.py:45`).
- Let a `kafka.patterns.<name>` and a `mocks.kafka.reactors.<name>` self-locate their cluster (like a DB template's `connection` field), so a `--pattern`/reactor doesn't need a flag every time.
- Make the upgrade **non-silent and mechanical**: a structured `config migrate` rewrite lifts the old flat block into a named cluster, exactly as the v1→v2 jq-dialect migration already does.

## 3. Scope & Design Constraints

- **Named clusters under `kafka:`** — `kafka.clusters.<name>`. Per-broker knobs (`brokers`, `ssl`, `timeout_seconds`, `default_consumer_group`, `schema_registry_url`) move *inside* each cluster. `patterns` stays a top-level map under `kafka:`, now cluster-aware.
- **Cluster selection mirrors DB connection selection.** Precedence: explicit CLI flag > binding's own field > configured default. Resolution is a new helper alongside `resolve_connection_name`, sharing the same contract shape.
- **Version bump `2 → 3`.** The `version` field is the major-schema switch already enforced by `_check_version` (`loader.py`). v3 is the clusters schema; a v2/v1 config is rejected at load with a pointer to `config migrate` — the same forcing function used today for v1-under-v2.
- **`config migrate` extended** to lift the flat `kafka:` block into `kafka.clusters.<name>` and bump the version. v1 inputs are carried through the existing jq-dialect rewrite first, so v1 → v3 works in one pass.
- **Mock reactors included.** Each reactor gains an optional `cluster` field (defaults via the same resolution rules). `MockEngine` builds each reactor's client against that reactor's resolved cluster.
- **`KafkaSSL` unchanged and reused** per cluster. No new TLS surface.

## 4. Non-Goals

- **Not changing the jq dialect.** v3 carries the same envelope-rooted `match` semantics as v2. The version bump is structural (the `kafka:` shape), not a match-root change.
- **Not a broker-HA / connection-pooling feature.** A cluster is a named broker *configuration*; we do not add failover, caching, or shared client lifecycle across invocations (per the stateless-invocation invariant, ARCHITECTURE §2).
- **Not multi-tenant auth or per-topic ACLs.** `ssl`/SASL config stays at cluster granularity. SASL beyond `security.protocol` remains deferred (today's `KafkaSSL` enum already allows `SASL_SSL`/`SASL_PLAINTEXT` but supplies no SASL mechanism fields — out of scope here).
- **Not topic→cluster glob routing.** Selection is by named cluster (flag or binding field), not by a routing table over topic names (rejected, §10).
- **Not wiring `schema_registry_url`.** It moves per-cluster (broker-scoped in reality) but stays **parsed-but-unused**, as today (ARCHITECTURE §15 "Known Limitations").

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Named clusters map `kafka.clusters.<name>`, mirroring `database.connections`.** A new `KafkaCluster` model holds `{brokers, ssl, timeout_seconds, default_consumer_group, schema_registry_url}`; `KafkaConfig` becomes `{clusters, default_cluster, patterns}`. | Direct symmetry with the already-proven DB model. One pattern, one mental model; the resolution and validation logic are near-copies of the DB equivalents, minimizing new design surface and new failure modes. |
| D2 | **Cluster resolution mirrors `resolve_connection_name` (`db_commands.py:45`).** Precedence: explicit `--cluster` > pattern/reactor `.cluster` > `kafka.default_cluster`. None resolved → `ConfigError`. Resolved name unknown → `ConfigError`. | Identical contract to DB connection resolution (explicit > template > default > error), so behavior is predictable and the two systems read the same way. |
| D3 | **Single-cluster auto-default.** When `default_cluster` is unset **and** exactly one cluster is defined, that cluster is the default. When >1 cluster and no `default_cluster`, an unresolvable command/reactor is a `ConfigError`. | A micro-deviation from DB (which always requires `defaults.database_connection`): the overwhelmingly common config has one cluster, and forcing `default_cluster` boilerplate on it is hostile. The >1 case still fails loudly. |
| D4 | **Default lives at `kafka.default_cluster`, not `defaults.kafka_cluster`.** | Keeps all Kafka concerns self-contained under `kafka:` (the approved design). DB's default lives at `defaults.database_connection` because DB has no enclosing `database.default_*` slot; here the enclosing `kafka:` block is the natural home. Flagged as an intentional asymmetry. |
| D5 | **Version bump `2 → 3` + `config migrate` rewrite.** `TOOL_MAJOR_VERSION` → `"3"`; a v2/v1 config is rejected at load pointing at `config migrate`. Migrate lifts the flat `kafka:` into `kafka.clusters.<name>` (default name `"default"`), sets `default_cluster`, preserves `patterns`, and bumps the version. | The repo's established upgrade path: the v1→v2 jq-dialect migration already does backup → rewrite → bump with `--dry-run`. A structural change to a top-level block warrants the same non-silent, mechanical treatment. Rejecting old configs at load prevents a silently-misinterpreted shape. |
| D6 | **`new_kafka_client` takes a resolved `KafkaCluster`, not `cfg.kafka`.** `_kafka_ssl_conf`, `_resolve_timeout`, `_resolve_group` read off the resolved cluster. | The client only ever needs one broker's config; passing the whole `KafkaConfig` is the current coupling that blocks multi-cluster. Narrowing the input to the resolved cluster is the minimal seam change and keeps the existing test fakes (`kafka_commands.new_kafka_client` monkeypatch) working. |
| D7 | **Each `KafkaReactor` gains optional `cluster` (resolved like a command). The `MockEngine` builds per-reactor clients against resolved clusters.** | A reactor must join a specific broker; under multi-cluster it cannot keep the implicit single-broker assumption. Resolving per-reactor (not one broker for all reactors) lets a mock span clusters — the whole point. |
| D8 | **`patterns` stays a top-level map under `kafka:`, cluster-aware via an optional `cluster` field per pattern.** | A pattern is "a named filter on a topic"; the topic lives on one cluster, so the binding is per-pattern. Keeping `patterns` global (not nested under each cluster) matches DB templates (global map, `connection` field) and avoids duplicating shared patterns across clusters. |

## 6. Core: Config Schema & Cluster Resolution

**New `kafka:` shape (contract):**

```yaml
version: "3"

kafka:
  clusters:
    main:
      brokers: ["${KAFKA_HOST}:9092"]
      ssl: { ca_location: "${KAFKA_SSL_CA:-}", ... }   # KafkaSSL, unchanged
      timeout_seconds: 30
      default_consumer_group: agctl-consumer
      schema_registry_url: "${SCHEMA_REGISTRY_URL:-}"  # parsed, still unused
    analytics:
      brokers: ["${KAFKA_AN}:9092"]
      timeout_seconds: 15

  default_cluster: main        # required only when >1 cluster (D3)

  patterns:                    # global map, now cluster-aware
    order-created:
      cluster: analytics       # optional; default = default_cluster
      description: "An ORDER_CREATED event"
      topic: orders.created
      match: '.value.eventType == "ORDER_CREATED"'
```

**Model changes (`config/models.py`, design-level — field names, no bodies):**

- New `KafkaCluster(BaseModel)` = `{brokers: list[str], ssl: KafkaSSL | None, timeout_seconds: int | None, default_consumer_group: str | None, schema_registry_url: str | None}` (the fields currently on `KafkaConfig`, `models.py:113-119`).
- `KafkaConfig` restructured to `{clusters: dict[str, KafkaCluster], default_cluster: str | None, patterns: dict[str, KafkaPattern]}`.
- `KafkaPattern` (`models.py:73`) gains `cluster: str | None = None`.
- `KafkaReactor` (`models.py:256`) gains `cluster: str | None = None`.
- `KafkaSSL` (`models.py:79`) **unchanged**.
- `Defaults` (`models.py:62`) **unchanged** — the Kafka default lives under `kafka:` (D4).

**Cluster resolution (new helper, contract mirrors `resolve_connection_name`):**

- Inputs: the CLI-provided cluster name (optional), a binding's `.cluster` (optional, from a pattern or reactor), and `cfg.kafka`.
- Precedence: CLI name > binding `.cluster` > `cfg.kafka.default_cluster` > (if exactly one cluster) that single cluster.
- None resolvable and >1 cluster and no default → `ConfigError("No kafka cluster specified")`.
- Resolved name not in `cfg.kafka.clusters` → `ConfigError("Unknown kafka cluster: <name>")` with `detail.cluster`.
- Returns a cluster **name** (callers then index `cfg.kafka.clusters[name]`); the resolved `KafkaCluster` feeds `new_kafka_client`.

**Command wiring (`commands/kafka_commands.py`):**

- `produce` / `consume` / `assert` gain an optional `--cluster <name>` Click option.
- `_resolve_timeout` / `_resolve_group` / `_kafka_ssl_conf` / `new_kafka_client` operate on the resolved `KafkaCluster`.
- `assert` with `--pattern`: the pattern's `.cluster` participates in resolution (so a pattern both supplies the topic **and** the cluster, like a DB template supplies SQL + connection).

## 7. Mock Reactors

- `KafkaReactor.cluster` (optional) resolves via the same helper as commands (reactor.cluster → default_cluster → single-cluster).
- `MockEngine` (`mock/engine.py:196-203`) builds each reactor's `KafkaClient` against that reactor's resolved cluster's brokers/TLS, instead of one `cfg.kafka`.
- The validator's "mocks.kafka requires kafka.brokers" check (`validator.py:100-110`) becomes: every reactor's resolved cluster must exist and define a non-empty `brokers`.

## 8. Migration (`v2 → v3`)

`agctl config migrate` (`config/migrate.py`, design-level steps — no algorithm bodies):

1. **v1 input** → run the existing jq-dialect rewrite (`.body |` / `.value |` prefixes on the three match-site families) first; the input is now logically v2.
2. **Structural lift** → move `kafka.{brokers, ssl, timeout_seconds, default_consumer_group, schema_registry_url}` into `kafka.clusters.default.{...}`; set `kafka.default_cluster: "default"`; leave `kafka.patterns` in place.
3. **Version bump** → set `version: "3"`.
4. **Safety** → back up original to `<path>.bak` (refuse to clobber an existing `.bak`); support `--dry-run`; idempotent (an already-v3 config with a `clusters` map is a clean no-op, `already_v3: true`).
5. **Result shape** extends today's `config.migrate` envelope with the structural rewrites list (path/before/after) alongside the existing jq rewrites.

`config/loader.py::_check_version`: `TOOL_MAJOR_VERSION` becomes `"3"`; the version is now "config schema version" (the jq dialect is a *consequence* of v2+, not its sole meaning — documented in DESIGN §2 / ARCHITECTURE §5).

## 9. CLI Surface

- **`--cluster <name>`** — new optional option on `kafka produce`, `kafka consume`, `kafka assert`. Resolved per D2/D3; omitted on a single-cluster config just works.
- **`config migrate`** — extended to perform the v2→v3 structural lift (and v1→v3 in one pass). `config validate` / `config show` otherwise unchanged (they consume the new typed `Config`).
- **`discover`** — `kafka-patterns` item detail surfaces the pattern's resolved cluster name (the summary count is unchanged).
- **Exit codes unchanged:** `0` success, `1` assertion fail, `2` config/tool/env error (unknown cluster, no-cluster-specified, migrate failure, v2-under-v3 load rejection).

## 10. Rejected Alternatives (ADR-style)

- **Flat clusters, global patterns, `--cluster` required every call (approach B from brainstorm).** Rejected (D1/D8): forces the flag on every command, prevents a `--pattern` from self-locating, and diverges from the DB model's per-binding field. More friction, less consistency.
- **Topic→cluster glob routing (approach C from brainstorm).** Rejected (Non-Goals): adds a routing table to design, validate, and debug; selection by explicit name is simpler and matches DB. Routing is trivially addable later if a real need appears.
- **Dual-parse (accept flat `kafka:` and `kafka.clusters:` forever, no version bump).** Rejected (D5): two valid shapes to support indefinitely, loader/validator branching, and ambiguous errors when both appear. The repo's culture is one current schema + a mechanical migrate, not perpetual dual support.
- **Hard break, no migrate.** Rejected (D5): worst UX — every consumer hand-edits on upgrade. The migrate command is cheap and already templated by v1→v2.
- **Defer mock reactors to a follow-up.** Rejected (D7): leaves reactors implicitly pinned to "the" broker, which no longer exists under multi-cluster — an immediate inconsistency and a false-green surface (a reactor silently joining the wrong/default cluster). Bundling is the consistent choice.
- **Nest `patterns` under each cluster.** Rejected (D8): would duplicate shared patterns and diverge from DB templates (global map + binding field). A pattern is a named filter, not cluster-owned state.
- **Put the default at `defaults.kafka_cluster`.** Rejected (D4): the enclosing `kafka:` block is the natural home; mirroring DB's `defaults.*` placement here would split Kafka config across two top-level sections for no gain.

## 11. Validation Strategy

- **Unit — model/restructure:** `KafkaConfig` accepts the clusters shape; `KafkaCluster` carries the migrated fields; `KafkaPattern`/`KafkaReactor` accept `cluster`.
- **Unit — resolution precedence (mirrors DB `resolve_connection_name` tests):** `--cluster` > binding `.cluster` > `default_cluster`; single-cluster auto-default; unknown-cluster error; no-cluster-specified error (>1 cluster, no default, no flag/binding).
- **Unit — migrate:** flat `kafka:` → `kafka.clusters.default` + `default_cluster`; `version` 2→3; v1 input carried through jq rewrite then lifted to v3 in one pass; idempotency on an already-v3 config; `--dry-run` no-write; `.bak` clobber refusal. Golden before/after YAML.
- **Unit — validator cross-refs:** pattern `.cluster` → unknown cluster = error; reactor `.cluster` → unknown = error; `default_cluster` → unknown = error; reactor whose resolved cluster lacks `brokers` = error; existing pattern description-warning loop unchanged.
- **Unit — kafka commands (fake client, no broker):** `produce`/`consume`/`assert` build the client against the **resolved** cluster (brokers/ssl/timeout/group read off it); `assert --pattern` resolves topic **and** cluster from the pattern.
- **Unit — mock engine (fake client):** each reactor's client is built against that reactor's resolved cluster; reactors with different `cluster` values get different broker configs.
- **Integration — default cluster unchanged:** the existing `AGCTL_TEST_KAFKA_BROKER` suite runs against the default cluster with no `--cluster` flag (single-cluster auto-default); self-skipping when the broker is absent, as today.
- **Integration — second cluster:** when `AGCTL_TEST_KAFKA_BROKER_2` is set, a `produce`/`assert` round-trip targets cluster 2 by name; self-skipping otherwise (no new hard CI dependency).
- **Drift guard:** `sample-config.yaml` is migrated to the v3 clusters shape and stays `${ENV}`-clean (validates with no env beyond what it already needs).

After implementation, `docs-watcher` runs (CLAUDE.md "Docs Sync") — **expected real updates** to `DESIGN.md` §2.1 (kafka schema), §3.2 (`--cluster`), §3.5 (reactor `cluster`), §5 (version semantics), and `ARCHITECTURE.md` §5 (v3 guard / migrate) and §8 (`new_kafka_client` cluster input), plus the `skills/agctl*` consumer references.

## 12. Docs & Skill Impact

- **`DESIGN.md`** §2.1: rewrite the `kafka:` block to the clusters shape; document `default_cluster`, per-pattern/reactor `cluster`, and the v3 version. §3.2: add `--cluster` to `produce`/`consume`/`assert`; document the resolution precedence. §3.5: document reactor `cluster`. §5: broaden "version = jq-dialect switch" to "version = config schema version."
- **`ARCHITECTURE.md`** §5: note `TOOL_MAJOR_VERSION=3` and the structural migrate step. §8: `new_kafka_client(cluster, ...)` and the resolution helper alongside `resolve_connection_name`.
- **`skills/` tree:** `agctl-config` (consumer reference) gains the clusters shape, `--cluster`, and the v2→v3 migrate step; `agctl-write-test-runbook` / `agctl-run-test-runbook` gain `--cluster` awareness where Kafka commands are authored/run. All ship as portable consumer artifacts under `skills/`.
- **`config migrate`** result shape and `cli_flags_note` are extended for the structural lift; CLI `--cluster` flags in scripts/runbooks are **not** rewritten by migrate (out of scope, same caveat as today's `--match`).
