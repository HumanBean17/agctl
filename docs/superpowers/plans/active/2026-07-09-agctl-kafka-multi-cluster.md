# Multi-Cluster Kafka Support — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let one `agctl.yaml` define N named Kafka clusters and let `kafka produce/consume/assert` and `mocks.kafka.reactors` target any of them, mirroring the existing `database.connections` model.

**Architecture:** Restructure the single `Config.kafka` object into `kafka.clusters.<name>` (a named map) plus `kafka.default_cluster`; add a `resolve_cluster_name` helper that mirrors `resolve_connection_name`; thread a resolved `KafkaCluster` (not the whole `KafkaConfig`) into `new_kafka_client`; bump the config schema `2 → 3` with a `config migrate` rewrite that lifts the flat `kafka:` block into a named cluster.

**Tech Stack:** Python ≥3.11, Pydantic v2, Click, pytest, optional `confluent-kafka` extra (lazy-imported).

## Global Constraints

(From the spec; every task's requirements implicitly include these.)

- Python ≥3.11; Pydantic v2 models; no new hard dependencies — `confluent-kafka`/`jq` stay behind the `kafka` extra, lazy-imported inside client constructors/methods only.
- Config stays stateless and `${ENV}`-interpolatable; for `kafka.ssl.*`, an empty string counts as unset (never silently downgrade TLS).
- One-JSON-envelope-per-invocation contract holds; exit codes `0` ok / `1` assertion / `2` config-tool-env error. `kafka produce/consume/assert` remain `@envelope`-wrapped.
- Unit tests run **without** `confluent_kafka` installed (clients built with `consumer_factory=`/`producer_factory=` fakes; `new_kafka_client` monkeypatched).
- Test conventions (mirror exactly): no `tests/unit/conftest.py`; per-file local helpers/fakes; pure helpers tested by building dicts/`Config` directly; commands tested via `CliRunner().invoke(cli, [...])` + `json.loads(result.output)` envelope assertions; tmp configs via `tmp_path / "agctl.yaml"`. Shared fixture at `tests/fixtures/agctl.yaml`.
- After the final task, run the `docs-watcher` subagent (CLAUDE.md "Docs Sync") to sync `DESIGN.md` / `ARCHITECTURE.md` / `skills/`.

---

## File Structure

**Modify:**
- `agctl/config/models.py` — restructure `KafkaConfig`; add `KafkaCluster`; add `cluster` field to `KafkaPattern` and `KafkaReactor`.
- `agctl/config/loader.py` — bump `TOOL_MAJOR_VERSION` to `"3"`; update `_check_version` advice text.
- `agctl/config/migrate.py` — rename `migrate_match_exprs` → `migrate_config`; `TO_VERSION="3"`; `MigrateResult.already_v2` → `already_current`; add structural `kafka:` lift (v1/v2 → v3).
- `agctl/config/validator.py` — replace the flat `kafka.brokers` check with cluster cross-refs; add `pattern.cluster` / `default_cluster` / `reactor.cluster` dangling-ref checks.
- `agctl/commands/kafka_commands.py` — add `resolve_cluster_name`; change `new_kafka_client` to take a resolved `KafkaCluster`; add `--cluster` flag to produce/consume/assert; consume `KafkaPattern.cluster`.
- `agctl/commands/config_commands.py` — update `config_migrate` consumer for the renamed API + `already_current` envelope key + v3 docstring.
- `agctl/commands/mock_commands.py` — build a per-reactor client map (resolving each reactor's cluster); pass to the engine.
- `agctl/commands/discover_commands.py` — surface a pattern's resolved cluster in the kafka-patterns item detail.
- `agctl/mock/engine.py` — accept a per-reactor client map; build each reactor against its own client.
- `agctl/data/sample-config.yaml` — migrate to the v3 clusters shape.
- `tests/unit/test_models.py`, `test_migrate.py`, `test_loader.py`, `test_validator.py`, `test_kafka_commands.py`, `test_mock_engine.py`, `test_mock_kafka_reactor.py`, `test_discover_command.py` — update/add per task.

**Create:** none (no new modules; resolution helper lives in `kafka_commands.py` next to its DB analog).

---

## Task 1: Config schema v3 — `kafka.clusters`, version bump, and `config migrate`

This task restructures the schema, bumps the version, and extends migration so a flat v1/v2 config is rewritten to v3. It keeps the whole tree green: single-cluster configs work exactly as before via the new `clusters` shape, because the consumers (kafka_commands/mock_commands/engine/discover) are updated in the same task to resolve the default/single cluster. Multi-cluster *selection* (the `--cluster` flag and pattern/reactor bindings) lands in Tasks 2–3.

**Files:**
- Modify: `agctl/config/models.py:113-119` (`KafkaConfig`), `:73-76` (`KafkaPattern`), `:256-264` (`KafkaReactor`); add `KafkaCluster`.
- Modify: `agctl/config/loader.py:215` (`TOOL_MAJOR_VERSION`), `:334-360` (`_check_version`).
- Modify: `agctl/config/migrate.py` (full: rename function, `TO_VERSION`, `MigrateResult`, add structural lift).
- Modify: `agctl/commands/config_commands.py:32` (import), `:317-365` (`config_migrate`).
- Modify: `agctl/commands/kafka_commands.py:44-108` (ssl/timeout/group/new_kafka_client operate on a cluster), add `resolve_cluster_name`.
- Modify: `agctl/commands/mock_commands.py:472-478` (resolve default cluster, build one client, guard on the resolved cluster's brokers).
- Modify: `agctl/config/validator.py:100-112` (default-cluster brokers check for reactors), `:88-96` (unchanged location — pattern warnings).
- Modify: `agctl/data/sample-config.yaml`.
- Test: `tests/unit/test_models.py`, `tests/unit/test_migrate.py`, `tests/unit/test_loader.py`, `tests/unit/test_validator.py`, `tests/unit/test_kafka_commands.py`, `tests/unit/test_mock_engine.py`.

**Interfaces:**

- **Consumes:** nothing from later tasks. This is the foundation.
- **Produces (contracts later tasks rely on):**
  - `KafkaCluster(BaseModel)` with fields `{brokers: list[str] = [], ssl: KafkaSSL | None = None, timeout_seconds: int | None = None, default_consumer_group: str | None = None, schema_registry_url: str | None = None}`. `KafkaSSL` is reused unchanged.
  - `KafkaConfig` restructured to `{clusters: dict[str, KafkaCluster] = {}, default_cluster: str | None = None, patterns: dict[str, KafkaPattern] = {}}`.
  - `KafkaPattern` gains `cluster: str | None = None`. `KafkaReactor` gains `cluster: str | None = None`. (Fields added here; consumed in Tasks 2–3.)
  - `TOOL_MAJOR_VERSION == "3"`.
  - `resolve_cluster_name(cfg_kafka: KafkaConfig, explicit: str | None, binding_cluster: str | None = None) -> str` — returns a cluster **name** present in `cfg_kafka.clusters`. Precedence: `explicit` > `binding_cluster` > `cfg_kafka.default_cluster` > the single cluster when exactly one is defined. Raise `ConfigError("No kafka cluster specified", {})` when none resolve and >1 cluster / no default. Raise `ConfigError("Unknown kafka cluster: <name>", {"cluster": <name>})` when a resolved name is absent from `clusters`. (Mirrors `resolve_connection_name` in `db_commands.py:45`.)
  - `new_kafka_client(cluster: KafkaCluster, group_id: str | None = None) -> KafkaClient` — builds the client from `cluster.brokers` + `_kafka_ssl_conf(cluster)`. The module-level `new_kafka_client` remains the test seam (monkeypatched; factory signature changes from `(cfg_kafka, group_id=None)` to `(cluster, group_id=None)`).
  - `migrate_config(config: dict) -> MigrateResult` (renamed from `migrate_match_exprs`). `MigrateResult` fields: `{config: dict, from_version: str, to_version: str = "3", rewrites: list[dict] = [], already_current: bool = False}`. The `config.migrate` envelope key `already_v2` becomes `already_current`.
  - `MigrateResult.rewrites` entries come in two shapes: jq-prefix rewrites `{"path": str, "before": str, "after": str}` (existing) and structural rewrites `{"path": "kafka.clusters.<name>.<field>", "before": <value>, "after": <value>}` — at minimum one record per lifted scalar (brokers/ssl/timeout_seconds/default_consumer_group/schema_registry_url) plus the `default_cluster` set, in deterministic order (brokers, ssl, timeout_seconds, default_consumer_group, schema_registry_url, default_cluster).

- [ ] **Step 1: Write failing model + migrate + version tests**

  In `tests/unit/test_models.py` add: a `KafkaConfig` built from a `clusters` dict parses, and `cfg.kafka.clusters["main"].brokers == ["h:9092"]`; `cfg.kafka.default_cluster == "main"`; a `KafkaPattern` accepts `cluster="analytics"`; a `KafkaReactor` accepts `cluster="analytics"`. A `KafkaConfig()` default has empty `clusters`, `default_cluster is None`, empty `patterns`.

  In `tests/unit/test_migrate.py`: rename usages to `migrate_config`; assert `TO_VERSION`/`to_version == "3"`; assert `MigrateResult.already_current` (not `already_v2`). Add a test: a flat v2 config `{"version":"2","kafka":{"brokers":["h:9092"],"timeout_seconds":30,"default_consumer_group":"g","patterns":{"p":{"topic":"t","match":".value.x"}}}}` migrates to `config["version"]=="3"`, `config["kafka"]["clusters"]["default"]["brokers"]==["h:9092"]`, `config["kafka"]["clusters"]["default"]["timeout_seconds"]==30`, `config["kafka"]["clusters"]["default"]["default_consumer_group"]=="g"`, `config["kafka"]["default_cluster"]=="default"`, and `config["kafka"]["patterns"]` preserved unchanged. Assert `already_current is False` and `from_version == "2"`. Add a test: a v1 config (flat kafka + a `.value`-less kafka pattern match) migrates to v3 in one pass — both the jq prefix is applied AND the structural lift occurs. Add `test_migrate_idempotent_v3`: a config already `{"version":"3","kafka":{"clusters":{"main":{...}}}}` returns `already_current is True`, `rewrites == []`, deep-copied unchanged. Add a test: `migrate_config` never mutates the input dict.

  In `tests/unit/test_loader.py`: update `test_version_mismatch_raises` to assert `exc.value.detail["tool_major"] == "3"` and `"config migrate" in exc.value.message`; add `test_v2_config_rejected_pointing_at_migrate` (a `version: "2"` tmp config raises `ConfigError`, message mentions migrate). Existing happy-path loader tests must keep passing once the shared fixture is v3 (update the fixture in Step 9).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_models.py tests/unit/test_migrate.py tests/unit/test_loader.py -v`
  Expected: FAIL (missing `KafkaCluster`; `migrate_config` undefined; `TOOL_MAJOR_VERSION` still "2"; `already_v2`/`already_current` mismatches).

- [ ] **Step 3: Implement models + version + migrate (minimal)**

  `models.py`: define `KafkaCluster` (fields above, reusing `KafkaSSL`); restructure `KafkaConfig`; add `cluster` to `KafkaPattern` and `KafkaReactor`. `loader.py`: set `TOOL_MAJOR_VERSION = "3"`; rewrite `_check_version`'s mismatch message to advise running `agctl config migrate` (drop the hardcoded `.body |`/`.value |` hand-prefixing advice, since v3 migrate is now structural+automatic). `migrate.py`: rename `migrate_match_exprs` → `migrate_config`; set `TO_VERSION = "3"`; rename `MigrateResult.already_v2` → `already_current` (idempotency now `source_major == "3"`); keep the three jq-prefix walkers but gate them to run **only when the source major is `"1"`** (v2→v3 needs no jq rewrite — exprs are already envelope-rooted); add a structural lift step `_lift_kafka_clusters(config, rewrites)` that, when `config["kafka"]` is a dict containing a top-level `brokers` (or any of the five flat keys) and no `clusters` key, moves `{brokers, ssl, timeout_seconds, default_consumer_group, schema_registry_url}` into `config["kafka"]["clusters"]["default"]` and sets `config["kafka"]["default_cluster"] = "default"`, recording one structural rewrite per lifted key. Defensive `.get()` chains; a missing/already-clustered `kafka` contributes no rewrites and never raises. The function never mutates its input (deep-copy first).

  `config_commands.py`: update the import to `migrate_config`; in `config_migrate` read `result.already_current` and emit envelope key `"already_current"`; update the Click command docstring to "Rewrite a v1/v2 agctl.yaml to v3 (named kafka clusters; envelope-rooted match)." Keep `.bak` backup, clobber-refusal, `--dry-run`, and `yaml.safe_dump` write exactly as today; `will_write = not result.already_current and not dry_run`.

- [ ] **Step 4: Run model/migrate/loader tests to verify pass**

  Run: `pytest tests/unit/test_models.py tests/unit/test_migrate.py tests/unit/test_loader.py -v`
  Expected: PASS.

- [ ] **Step 5: Write failing consumer/resolution tests (default-cluster path stays green)**

  In `tests/unit/test_kafka_commands.py`: update the `install_fake` factory signature from `factory(cfg_kafka, group_id=None)` to `factory(cluster, group_id=None)` (the fake client is returned regardless of cluster). Add `test_kafka_produce_single_cluster_no_flag`: a config with one cluster `main` (brokers via `KAFKA_BROKER`) and no `--cluster` flag produces successfully (envelope `ok:true`), proving default/single-cluster resolution. Add `test_kafka_assert_unknown_cluster_error`: `--cluster ghost` → exit 2, `payload["error"]["type"]=="ConfigError"`, `payload["error"]["detail"]["cluster"]=="ghost"`. Add `test_kafka_consume_no_default_multi_cluster_error`: a config with two clusters, no `default_cluster`, no `--cluster` → exit 2, message contains "No kafka cluster specified".

  In `tests/unit/test_validator.py` (`_cfg(**overrides)` helper): update the local `KafkaConfig()` construction to the new shape. Add `test_reactor_default_cluster_missing_brokers_errors`: a `MocksConfig` with one reactor and a default cluster whose `brokers == []` → one error at path `"mocks.kafka"`. Add `test_default_cluster_dangling_ref_errors`: `cfg.kafka.default_cluster="ghost"` with empty clusters → error at `"kafka.default_cluster"`. Add `test_pattern_cluster_dangling_ref_errors`: a `KafkaPattern(cluster="ghost")` → error at `"kafka.patterns.<name>.cluster"`.

  In `tests/unit/test_mock_engine.py`: existing tests construct `MockEngine(kafka_client=...)` directly; in Task 1 this stays a single client (no signature change yet), so only the config consumed by any test that builds a `MocksConfig`/`KafkaReactor` must use the clusters shape. Add/adjust so a reactor-driven engine start still emits `started`.

- [ ] **Step 6: Run consumer tests to verify they fail**

  Run: `pytest tests/unit/test_kafka_commands.py tests/unit/test_validator.py tests/unit/test_mock_engine.py -v`
  Expected: FAIL (`cfg.kafka.brokers` AttributeError in kafka_commands/mock_commands; validator's old flat check; factory signature mismatch).

- [ ] **Step 7: Wire consumers to the resolved default cluster**

  `kafka_commands.py`: add `resolve_cluster_name(cfg_kafka, explicit, binding_cluster=None)` per the Produces contract. Change `_kafka_ssl_conf`, `_resolve_timeout`, `_resolve_group`, and `new_kafka_client` to take a `KafkaCluster` (read `cluster.ssl`, `cluster.timeout_seconds`, `cluster.default_consumer_group`, `cluster.brokers`). In each `_core` (`_kafka_produce_core`, `_kafka_consume_core`, `_kafka_assert_core`): resolve the cluster with `explicit=None`, `binding_cluster=None` for now (Task 2 passes the real values), then `cluster = cfg.kafka.clusters[name]` and `new_kafka_client(cluster, group_id=group)`. Keep all jq/contains/predicate logic unchanged.

  `mock_commands.py` (`mock_run`, ~line 472): replace `if not cfg.kafka.brokers:` with resolving the default cluster (`resolve_cluster_name(cfg.kafka, None)`) and guarding `if not cluster.brokers:`; build `kafka_client = new_kafka_client(cluster)`. (Per-reactor clients come in Task 3; here all reactors still share the single default client, and the engine signature is unchanged.)

  `validator.py`: replace the `not cfg.kafka.brokers` block (lines 100-112) with: when `cfg.mocks.kafka.reactors` is non-empty, resolve a default cluster via the same precedence used by `resolve_cluster_name` (inline: `cfg.kafka.default_cluster`, else the single cluster if exactly one); if none resolves → error at `"mocks.kafka"` "reactors require a resolvable default cluster"; if the resolved cluster exists but `brokers` is empty → error at `"mocks.kafka"` "kafka mocks require kafka.clusters.<name>.brokers". Add a `default_cluster` dangling-ref check (path `"kafka.default_cluster"`) and a `KafkaPattern.cluster` dangling-ref check (path `"kafka.patterns.<name>.cluster"`). (Reactor.cluster dangling-ref is deferred to Task 3 where it is consumed.)

- [ ] **Step 8: Run the full unit suite**

  Run: `pytest tests/unit -v`
  Expected: PASS (all previously-passing tests, now on the v3 schema).

- [ ] **Step 9: Migrate sample config + shared fixtures to v3**

  `agctl/data/sample-config.yaml`: restructure the `kafka:` block to `clusters.<name>` (one cluster, e.g. `default`, carrying brokers/ssl/timeout_seconds/default_consumer_group), set `default_cluster`, set `version: "3"`. Must still validate with no env beyond what it already needs (keep `${...:-}` optionals). Update `tests/fixtures/agctl.yaml` to the same v3 shape so loader/kafka/db/mock unit tests that load it keep passing (this fixture is the shared `FIXTURE`).

  Run: `pytest tests/unit -v` then `python -m agctl config validate --config agctl/data/sample-config.yaml` (with the env the sample needs).
  Expected: PASS; validate exit 0.

- [ ] **Step 10: Commit**

  Run: `git add agctl/config/models.py agctl/config/loader.py agctl/config/migrate.py agctl/commands/config_commands.py agctl/commands/kafka_commands.py agctl/commands/mock_commands.py agctl/config/validator.py agctl/data/sample-config.yaml tests/unit/`
  Run: `git commit -m "feat(kafka): restructure Config.kafka to named clusters (v3) + migrate"` (end the message with a blank line then `Co-Authored-By: Claude <noreply@anthropic.com>`).

---

## Task 2: `--cluster` flag + per-pattern cluster binding

Adds the CLI selection surface. After this, `kafka produce/consume/assert --cluster <name>` targets a specific cluster, and `kafka assert --pattern <name>` resolves both topic and cluster from the pattern.

**Files:**
- Modify: `agctl/commands/kafka_commands.py` (Click options on the three commands + `_core` signatures + resolution call sites + pattern `cluster` consumption).
- Test: `tests/unit/test_kafka_commands.py`.

**Interfaces:**

- **Consumes:** `resolve_cluster_name`, `new_kafka_client(cluster, ...)`, `KafkaPattern.cluster` (from Task 1).
- **Produces:** the `--cluster <name>` option on `kafka produce`/`consume`/`assert`; `kafka assert --pattern` resolves the cluster from `cfg.kafka.patterns[<pattern>].cluster` when `--cluster` is absent. No new public helpers.

- [ ] **Step 1: Write failing tests**

  Add to `tests/unit/test_kafka_commands.py`:
  - `test_kafka_produce_explicit_cluster`: config with clusters `main` (broker A) and `analytics` (broker B); `kafka produce --topic t --message '{}' --cluster analytics` succeeds and the captured fake client was built from `analytics`'s brokers (assert the factory received the `analytics` cluster — capture it in the patched `new_kafka_client`). Expected: exit 0, captured cluster's `.brokers == ["<B>"]`.
  - `test_kafka_assert_pattern_resolves_cluster`: a `KafkaPattern("ord", cluster="analytics", topic="orders", match=".value.x")` with two clusters; `kafka assert --pattern ord --timeout 2` (no `--cluster`, no `--topic`) resolves cluster `analytics` (captured) and topic `orders`. Expected: the fake client's consumed topic is `orders` and the captured cluster is `analytics`.
  - `test_kafka_consume_cluster_flag_overrides_pattern`: `--cluster main` overrides a pattern's `cluster: analytics` (precedence). Expected: captured cluster is `main`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_kafka_commands.py -k "explicit_cluster or resolves_cluster or overrides_pattern" -v`
  Expected: FAIL (no `--cluster` option; pattern cluster not consumed).

- [ ] **Step 3: Implement the flag and binding**

  Add `@click.option("--cluster", "cluster", default=None, help="Named kafka cluster (default: kafka.default_cluster)")` to `kafka_produce`, `kafka_consume`, `kafka_assert`. Thread a `cluster: str | None` param through each `_core`. In each `_core`, determine the binding cluster: for `assert` with `--pattern`, `binding_cluster = cfg.kafka.patterns[pattern].cluster`; otherwise `binding_cluster = None`. Then `name = resolve_cluster_name(cfg.kafka, explicit=cluster, binding_cluster=binding_cluster)`; `resolved = cfg.kafka.clusters[name]`; `client = new_kafka_client(resolved, group_id=group)`. (For `produce`/`consume` there is no pattern, so `binding_cluster=None`.) No other behavior changes.

- [ ] **Step 4: Run tests to verify pass**

  Run: `pytest tests/unit/test_kafka_commands.py -v`
  Expected: PASS (new tests + all prior).

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/kafka_commands.py tests/unit/test_kafka_commands.py`
  Run: `git commit -m "feat(kafka): add --cluster flag and per-pattern cluster binding"` (append the `Co-Authored-By` trailer).

---

## Task 3: Per-cluster mock reactors

Each `mocks.kafka.reactors.<name>` can target its own cluster. The engine receives a per-reactor client map instead of one shared client.

**Files:**
- Modify: `agctl/mock/engine.py:54-90` (`__init__` signature), `:194-213` (reactor build uses per-reactor client).
- Modify: `agctl/commands/mock_commands.py:113-137` (`new_mock_engine` seam), `:472-503` (build per-cluster client map).
- Modify: `agctl/config/validator.py` (add `reactor.cluster` dangling-ref + per-reactor resolved-cluster brokers check; the Task-1 default-cluster check generalizes).
- Test: `tests/unit/test_mock_engine.py`, `tests/unit/test_mock_commands.py` (if present; otherwise `test_mock_engine.py`), `tests/unit/test_validator.py`.

**Interfaces:**

- **Consumes:** `resolve_cluster_name`, `new_kafka_client(cluster, ...)`, `KafkaReactor.cluster` (Task 1).
- **Produces:**
  - `MockEngine.__init__(..., kafka_clients: dict[str, KafkaClient] | None = None)` (replaces the single `kafka_client` param). Keyed by **reactor name**. `None` when `run_kafka=False`.
  - `new_mock_engine(..., kafka_clients: dict[str, KafkaClient] | None)` (the test seam mirrors the new signature).
  - The engine builds each reactor with `client=self._kafka_clients[reactor_name]`.
  - Validator: `reactor.cluster` → unknown cluster = error (path `"mocks.kafka.reactors.<name>.cluster"`); each reactor's resolved cluster (reactor.cluster → default → single) must exist and have non-empty `brokers`.

- [ ] **Step 1: Write failing tests**

  In `tests/unit/test_mock_engine.py`: update existing reactor constructions from `kafka_client=<client>` to `kafka_clients={<reactor_name>: <client>}`. Add `test_reactors_use_per_cluster_clients`: a `MocksConfig` with two reactors `rA` (cluster `main`) and `rB` (cluster `analytics`); construct `MockEngine(mocks=..., run_kafka=True, kafka_clients={"rA": clientA, "rB": clientB}, emit_fn=capture)`. After `start()` (with fakes that make `probe` a no-op), the started line lists both reactors, and each reactor was wired to its own client (assert via the fake clients recording which broker config / `probe` call each got — capture client identity per reactor).

  In `tests/unit/test_validator.py`: add `test_reactor_cluster_dangling_ref_errors` (`KafkaReactor(cluster="ghost")` → error at `"mocks.kafka.reactors.<name>.cluster"`); add `test_reactor_resolved_cluster_missing_brokers_errors` (reactor with `cluster="main"` but `main.brokers==[]` → error at `"mocks.kafka.reactors.<name>"`, message names the cluster).

  In `tests/unit/test_mock_engine.py` (or `test_mock_commands.py` if it exists): add `test_mock_run_builds_per_cluster_clients` — monkeypatch `mock_commands.new_kafka_client` with a factory that records the `KafkaCluster` it received per call; invoke `mock run` via `CliRunner` against a two-reactor/two-cluster config (no real broker — fake client returned for every cluster). Assert the factory was called once per distinct cluster with the right `KafkaCluster.brokers`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_engine.py tests/unit/test_validator.py -v`
  Expected: FAIL (`kafka_client=` kwarg no longer accepted; reactor.cluster not validated; per-cluster wiring absent).

- [ ] **Step 3: Implement per-reactor clients**

  `engine.py`: change `__init__` param `kafka_client` → `kafka_clients: dict[str, KafkaClient] | None = None`; store `self._kafka_clients`. In `start()` Step 1 (line ~203), build each reactor with `client=self._kafka_clients[name]` (lookup by reactor name); keep `prepare()`/probe ordering and the `_kafka_client is None` guard generalized to `self._kafka_clients is None`. `KafkaReactor` (the reactor class) and `kafka_reactor.py` are unchanged — a reactor still takes one `client`.

  `mock_commands.py`: `new_mock_engine` signature mirrors the engine (`kafka_clients=`). In `mock_run` (~line 472): when `run_kafka`, for each distinct cluster resolved across reactors (`resolve_cluster_name(cfg.kafka, None, binding_cluster=reactor.cluster)` per reactor), build one `KafkaClient` via `new_kafka_client(cluster)`; assemble `kafka_clients = {reactor_name: client_for_its_cluster}` (reusing the same client instance for reactors sharing a cluster); drop the single `kafka_client = new_kafka_client(...)` and the old single-cluster brokers guard in favor of per-reactor resolution. Pass `kafka_clients=` to `new_mock_engine`. Keep the `--only kafka` / engine-resolution logic intact.

  `validator.py`: generalize the Task-1 default-cluster reactor check into a per-reactor resolved-cluster check: for each reactor, resolve its cluster (reactor.cluster → default → single), and error if (a) the explicit `reactor.cluster` names an unknown cluster (path `"mocks.kafka.reactors.<name>.cluster"`), or (b) the resolved cluster has empty `brokers` (path `"mocks.kafka.reactors.<name>"`). Keep the `default_cluster` and `pattern.cluster` checks from Task 1.

- [ ] **Step 4: Run tests to verify pass**

  Run: `pytest tests/unit/test_mock_engine.py tests/unit/test_validator.py tests/unit/test_mock_kafka_reactor.py -v`
  Expected: PASS (reactor-level tests unchanged since `Reactor(client=...)` is untouched; engine/validator updated).

- [ ] **Step 5: Commit**

  Run: `git add agctl/mock/engine.py agctl/commands/mock_commands.py agctl/config/validator.py tests/unit/`
  Run: `git commit -m "feat(mock): per-cluster Kafka reactors via per-reactor client map"` (append the `Co-Authored-By` trailer).

---

## Task 4: Discovery surfacing + docs sync

Surfaces a pattern's resolved cluster in `discover`, then syncs all user-facing docs via `docs-watcher`.

**Files:**
- Modify: `agctl/commands/discover_commands.py` (kafka-patterns item detail).
- Modify (via `docs-watcher`): `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, `skills/agctl*`.
- Test: `tests/unit/test_discover_command.py`.

**Interfaces:**

- **Consumes:** `resolve_cluster_name`, `KafkaPattern.cluster` (Tasks 1–2).
- **Produces:** `discover --category kafka-patterns --name <n>` result includes a `"cluster"` key (the resolved cluster name). Summary counts unchanged.

- [ ] **Step 1: Write failing test**

  In `tests/unit/test_discover_command.py`: add `test_kafka_pattern_item_shows_cluster` — a config with two clusters and a pattern `cluster="analytics"`; `discover --category kafka-patterns --name <p>` returns `payload["result"]["cluster"] == "analytics"`. And `test_kafka_pattern_item_cluster_defaults` — a pattern with no `cluster` field and `default_cluster="main"` → `"cluster" == "main"`.

- [ ] **Step 2: Run test to verify it fails**

  Run: `pytest tests/unit/test_discover_command.py -k "cluster" -v`
  Expected: FAIL (no `cluster` key in the item result).

- [ ] **Step 3: Surface the resolved cluster**

  In the kafka-patterns item-detail `_core` (where `pat = cfg.kafka.patterns[name]`), add `"cluster": resolve_cluster_name(cfg.kafka, None, binding_cluster=pat.cluster)` to the result dict (after the existing `topic`/`match`/`params`/`example`). No other discover changes.

- [ ] **Step 4: Run the full unit suite**

  Run: `pytest tests/unit -v`
  Expected: PASS.

- [ ] **Step 5: Docs sync**

  Invoke the `docs-watcher` subagent to sync `DESIGN.md` (§2.1 kafka schema → clusters; §3.2 `--cluster`; §3.5 reactor `cluster`; §5 version = config schema version; §3.7 `config migrate` v3 result shape `already_current`) and `ARCHITECTURE.md` (§5 `TOOL_MAJOR_VERSION=3` + structural migrate; §8 `new_kafka_client(cluster, ...)` + `resolve_cluster_name` alongside `resolve_connection_name`; §15 remove "schema_registry_url parsed but unused" only if scope changed — it is not, keep it). Also sync the consumer `skills/agctl-config`, `skills/agctl`, `skills/agctl-write-test-runbook`, `skills/agctl-run-test-runbook` to mention `--cluster` and the v3 clusters shape where they author/run Kafka commands.

- [ ] **Step 6: Commit**

  Run: `git add agctl/commands/discover_commands.py tests/unit/test_discover_command.py docs/ skills/`
  Run: `git commit -m "feat(discover): surface kafka pattern cluster; docs sync to v3"` (append the `Co-Authored-By` trailer).

---

## Integration (not a separate task — extend the existing suite)

`tests/integration/test_kafka_commands.py`: the existing `require_kafka` / `AGCTL_TEST_KAFKA_BROKER` path already exercises the default cluster via the v3 fixture (updated in Task 1 Step 9) — it must stay green with no flag. Add a self-skipping `test_kafka_second_cluster_round_trip` gated on a new `AGCTL_TEST_KAFKA_BROKER_2` env var (mirror the `require_kafka` skip-when-absent pattern): `produce --cluster two` then `assert --cluster two --from-beginning`. If `AGCTL_TEST_KAFKA_BROKER_2` is unset, `pytest.skip()`. This adds no hard CI dependency. Fold this into Task 2 (it tests `--cluster`) as a follow-up step after Step 4 there, committed with Task 2.

---

## Self-Review (run after writing; fixes applied inline)

- **Code scan:** No method bodies / algorithms / copy-paste code in the plan — only signatures, data shapes, precedence rules, and test scenarios with expected results. ✓
- **Self-containment:** Each task's Produces block defines the exact types/signatures/error cases the next task consumes (`KafkaCluster` fields, `resolve_cluster_name` precedence + two `ConfigError` shapes, `new_kafka_client(cluster,...)`, `MigrateResult` fields, `MockEngine`/`new_mock_engine` `kafka_clients` map). ✓
- **Spec coverage:** §6 schema (Task 1), §6 resolution + §9 `--cluster` + pattern binding (Task 2), §7 reactors (Task 3), §8 migrate (Task 1), §5 validation cross-refs (Tasks 1 + 3), discover surfacing (Task 4), sample-config v3 (Task 1), docs (Task 4). ✓
- **Type consistency:** `new_kafka_client(cluster, group_id=None)` used identically in Tasks 1/2/3; `kafka_clients: dict[str, KafkaClient]` consistent across engine/seam in Task 3; `resolve_cluster_name(cfg_kafka, explicit, binding_cluster=None) -> str` consistent everywhere; `MigrateResult.already_current` consistent in Task 1 + config_commands. ✓
