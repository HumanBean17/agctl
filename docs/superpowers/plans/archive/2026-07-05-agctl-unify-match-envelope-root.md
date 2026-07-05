# Unify `match` onto the envelope root (#22) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every jq `match` root at its message envelope (peer of `capture`) by flipping the config dialect to `"2"`, and ship `agctl config migrate` to rewrite existing configs automatically.

**Architecture:** The config `version` major is the jq-dialect switch. Bumping `TOOL_MAJOR_VERSION` from `"1"` to `"2"` makes the version gate reject legacy configs with a migrate pointer; every config that loads successfully is dialect `"2"`, so runtime `match` evaluation is **always** envelope-rooted (no runtime branching on dialect — the gate does all the work). Five eval sites swap the value they feed `jq_bool`/the predicate; a new `config migrate` command applies a fixed-string prepend (`.body | ` / `.value | `) to rewrite legacy expressions.

**Tech Stack:** Python ≥3.11, Click, Pydantic v2, PyYAML, `jq` (optional extra), pytest + `click.testing.CliRunner`.

## Global Constraints

These apply to every task; each task's requirements implicitly include them.

- **Dialect v2 only at runtime.** After the bump, the loader rejects any config whose `version` major ≠ `"2"` with a `ConfigError` (exit 2) whose message contains the literal `config migrate`. There is NO v1 runtime path; runtime code simply implements envelope-rooted semantics. (Spec §3, §4 Non-Goals, §5 D1.)
- **The three v2 envelope roots** (Spec §6):
  - HTTP request envelope: `{method, path, headers(lowercased keys), body}`.
  - Kafka message envelope: `{key, value, partition, offset, timestamp, headers(case-sensitive keys as-produced)}`.
  - HTTP response envelope: `{status_code, response_time_ms, headers(lowercased keys), body, url, method}`.
- **Only jq `match` moves.** `capture.*.from` (already envelope-rooted), stub `match.body` (json_subset), and the flags `--contains`, `--path`, `--jq-path`, `--equals`, `--status` are UNCHANGED. Do not alter their rooting.
- **Prepend prefixes** (exact, with one trailing space): HTTP = `.body | ` ; Kafka = `.value | `. Idempotency check is a `str.startswith` on these exact prefixes.
- **Fail-loud preserved.** A stale payload-rooted predicate resolves to `null` → `False` → assertion failure (exit 1) or assert timeout; a v1 config → exit 2 with the pointer.
- **Branch:** all work on `22-unify-match-envelope-root` (already checked out). Commit after every task. End commit messages with `Co-Authored-By: Claude <noreply@anthropic.com>`.
- **Run unit tests with:** `pytest tests/unit -q`. No broker/HTTP services needed (clients lazy-import; tests inject fakes).

---

## File Structure

**Modified:**
- `agctl/config/loader.py` — `TOOL_MAJOR_VERSION` → `"2"`; `_check_version` message → migrate pointer.
- `agctl/mock/http_server.py` — build request envelope once; `match.jq` evals against it.
- `agctl/mock/kafka_reactor.py` — reactor `match` evals against the full `msg`.
- `agctl/commands/kafka_commands.py` — `_build_assert_predicate` match/pattern branches + consume inline predicate eval against `msg`.
- `agctl/assertions.py` — `evaluate_http_assertions` `--match` branch evals against `result`.
- `agctl/commands/config_commands.py` — new `config_migrate` command.
- `agctl/cli.py` — register `config_migrate` on the `config` group.
- `agctl/data/sample-config.yaml` + `README.md` sample block — `version: "2"` (kept byte-identical by the drift-guard test).
- `pyproject.toml` — `version = "1.0.0"`.
- `tests/fixtures/agctl.yaml` and ~13 inline `version: "1"` test fixtures → `"2"`.
- Existing match tests (`test_mock_http_server.py`, `test_mock_kafka_reactor.py`, `test_kafka_commands.py`, `test_http_commands.py`) — update payload-rooted exprs to envelope form.

**Created:**
- `agctl/config/migrate.py` — pure `migrate_match_exprs` helper.
- `tests/unit/test_migrate.py` — helper + command tests.

**Docs (final task, via docs-watcher + targeted edits):** DESIGN §2.1/§3.1/§3.2/§3.6/§4/§6/§10/§11; ARCHITECTURE §5/§9/§15; `skills/agctl-config/reference/`.

---

### Task 1: Dialect gate v2 + version fixtures + package 1.0.0

This task is the foundation: it flips the dialect switch and updates every `version: "1"` reference a test exercises, so the suite stays green. Subsequent tasks change eval semantics one site at a time.

**Files:**
- Modify: `agctl/config/loader.py:84`, `agctl/config/loader.py:101-108`
- Modify: `pyproject.toml:7`
- Modify: `tests/fixtures/agctl.yaml:1`
- Modify: `agctl/data/sample-config.yaml:3` + `README.md` (the sample `agctl.yaml` block around line 202)
- Modify (inline `version: "1"` → `version: "2"`): `tests/unit/test_loader.py` (lines 58, 67, 93, 111, 129, 143 — NOT line 50), `tests/unit/test_config_commands.py` (lines 40, 68, 98, 135), `tests/unit/test_db_commands.py:1166`, `tests/unit/test_cli.py` (lines 55, 124)
- Modify: `tests/unit/test_loader.py:48-53` (the mismatch test)
- Test: `tests/unit/test_loader.py`

**Interfaces:**
- Produces:
  - `agctl.config.loader.TOOL_MAJOR_VERSION == "2"` (a module-level `str`).
  - `_check_version(data: dict) -> None` — unchanged signature; raises `ConfigError` when `str(data.get("version","")).split(".")[0] != "2"`. The error `message` MUST contain the literal substring `config migrate` (the pointer); `detail` is `{"config_version": <raw version>, "tool_major": "2"}`.
  - `pyproject.toml` project `version = "1.0.0"`.
- Consumes: nothing new.

- [ ] **Step 1: Update the gate test to encode v2**

In `tests/unit/test_loader.py::test_version_mismatch_raises`: change the bad config from `version: "2"` to `version: "1"` (now the legacy), and change the assertion from `exc.value.detail["tool_major"] == "1"` to `== "2"`, and add `assert "config migrate" in exc.value.message`. This verifies a v1 config is now the rejected one and the message points at migration. Leave the OTHER `version: "1"` fixtures in this file as-is for this step only (they are updated in Step 3).

- [ ] **Step 2: Run the gate test to verify it fails**

Run: `pytest tests/unit/test_loader.py::test_version_mismatch_raises -q`
Expected: FAIL — the current `TOOL_MAJOR_VERSION` is still `"1"`, so a `version: "1"` config loads fine (no error raised), and the assertion on the raised `ConfigError` fails.

- [ ] **Step 3: Flip the dialect switch + bump package + update fixtures**

Behavior to implement (no code bodies here — describe to the implementer):
- In `agctl/config/loader.py`: set `TOOL_MAJOR_VERSION = "2"`. In `_check_version`'s raised `ConfigError`, change only the `message` string so it names the dialects and instructs running `agctl config migrate` (keep the existing `detail` keys `config_version`/`tool_major`; `tool_major` is now `"2"`).
- In `pyproject.toml`: set `version = "1.0.0"`.
- Update every test fixture that currently pins `version: "1"` to `version: "2"` EXCEPT the mismatch test's intentionally-bad config. Concrete sites: `tests/fixtures/agctl.yaml` line 1; `agctl/data/sample-config.yaml` line 3; the README.md sample `agctl.yaml` block (`version: "1"` → `"2"`); and the inline `'version: "1"\n'` literals in `test_loader.py` (58, 67, 93, 111, 129, 143), `test_config_commands.py` (40, 68, 98, 135), `test_db_commands.py` (1166), `test_cli.py` (55, 124). NOTE: `test_loader.py:50` is the mismatch test — keep it as `version: "1"` (the now-rejected form) per Step 1.
- If `tests/unit/test_packaging.py` (or any test) asserts the old package version `0.4.0`, update the expected value to `1.0.0`.

- [ ] **Step 4: Run the full unit suite to verify it passes**

Run: `pytest tests/unit -q`
Expected: PASS (green). Every fixture now declares `"2"`, the gate accepts it, the mismatch test confirms v1 is rejected with the pointer, and eval semantics are still payload-rooted (unchanged in this task) so existing match tests still pass with their existing payload-form exprs. If any test still fails on a `version` mismatch, it pins `version: "1"` somewhere not listed — update that site to `"2"` as well.

- [ ] **Step 5: Commit**

Run: `git add -A && git commit -m "feat(config)!: dialect v2 gate + package 1.0.0 (#22)"`
(`!` denotes the breaking dialect bump.)

---

### Task 2: Mock HTTP stub `match.jq` → request envelope

Root the HTTP stub jq predicate at the same request envelope `capture` already uses, so `.body.amount` / `.headers.authorization` are reachable and match+capture share a root.

**Files:**
- Modify: `agctl/mock/http_server.py:216-249` (match eval), `agctl/mock/http_server.py:282-290` (envelope reuse)
- Test: `tests/unit/test_mock_http_server.py`

**Interfaces:**
- Consumes: `make_handler(stubs, emit_event, semaphore)` (Task builds the envelope inside `_handle_request`).
- Produces: no new public symbol. Contract change: `jq_bool` is called with the request envelope `{method, path, headers(lowercased), body}` instead of `parsed_body`. The envelope dict is constructed ONCE per request (immediately after `parsed_body` is computed) and reused by both the `match.jq` check and `resolve_captures`, so the capture path is byte-identical to today (the four fields and their casing are unchanged).

- [ ] **Step 1: Add the failing envelope-root match test**

Add `test_stub_jq_match_envelope_root` to `tests/unit/test_mock_http_server.py` (mirror the existing `_serve` + `httpx.Client()` pattern and the `HttpStub`/`HttpMatch`/`HttpResponse` model construction used by neighboring tests): a stub with `method=POST, path="/orders", match=HttpMatch(jq=".body.amount > 1000"), response=HttpResponse(status=200)`. Assert a POST to `/orders` with JSON body `{"amount": 2000}` yields status `200` and an `http.hit` event; a POST with body `{"amount": 500}` yields `404` and `http.unmatched`. Under the current payload-rooted impl, `.body.amount` resolves against the body as `body.body.amount` → `null` → predicate false → 404 for BOTH requests, so the `2000→200` assertion fails.

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/unit/test_mock_http_server.py::test_stub_jq_match_envelope_root -q`
Expected: FAIL — the `2000` case returns 404 (predicate evaluated against the body, not the envelope).

- [ ] **Step 3: Move envelope construction up + root match at it**

Behavior to implement in `agctl/mock/http_server.py::_handle_request`:
- Immediately AFTER `parsed_body = self._parse_json_body(raw_body)` and BEFORE the stub loop, construct the request envelope dict with exactly these four fields: `method` = `self.command`; `path` = `urlsplit(self.path).path`; `headers` = `{k.lower(): v for k, v in self.headers.items()}`; `body` = `parsed_body`. (This is the SAME dict currently built at lines 283-288 inside the capture branch — relocate it, do not duplicate.)
- In the stub loop, change the `match.jq` check to evaluate the predicate against this `envelope` instead of `parsed_body`.
- In the capture branch, delete the now-redundant local envelope construction and call `resolve_captures(envelope, stub.capture)` with the single shared envelope.
- Do NOT touch the `match.body` (`json_subset`) check — it stays against `parsed_body`.

- [ ] **Step 4: Run the mock HTTP server tests to verify they pass**

Run: `pytest tests/unit/test_mock_http_server.py -q`
Expected: PASS. If a pre-existing test in this file used a stub `match.jq` with a payload-form expression (e.g. `.amount`), it will now fail because `.amount` resolves against the envelope; update that expression to `.body.<field>` so it asserts the same intent under the new root. (`match.body` tests are unaffected.)

- [ ] **Step 5: Commit**

Run: `git add -A && git commit -m "feat(mock)!: HTTP stub match.jq roots at request envelope (#22)"`

---

### Task 3: Mock Kafka reactor `match` → message envelope

Root the reactor jq predicate at the full normalized message so `.value.command`, `.key`, `.headers.<name>` are reachable (peer of reactor `capture`).

**Files:**
- Modify: `agctl/mock/kafka_reactor.py:145-148`
- Test: `tests/unit/test_mock_kafka_reactor.py` (incl. the `sample_config` fixture near line 135 and `test_reactor_match_capture_react` ~241, `test_reactor_jq_non_match_commits_no_event` ~333)

**Interfaces:**
- Consumes: `KafkaReactor.__init__(name, config, client, *, emit_event, stop_event, fail_fast, run_id)`; `_handle(msg, *, attempt, final)`.
- Produces: no new symbol. Contract change: when `self._config.match is not None`, the predicate is evaluated against the whole `msg` dict (`{key, value, partition, offset, timestamp, headers}`) instead of `value = msg.get("value")`. `value` is still extracted for the non-object skip check and the implicit capture context.

- [ ] **Step 1: Add the failing envelope-root match test**

Add `test_reactor_match_on_key_envelope` to `tests/unit/test_mock_kafka_reactor.py`: a reactor with `match='.key == "ord-1"'` and `reaction=KafkaReaction(topic="out", value={})`, fed via `FakeKafkaClient(messages=[...])` a message whose `value` is `{"command": "X"}` and `key` is `"ord-1"`. Assert the reactor produces a reaction (`kafka.reacted` emitted). Under the current value-rooted impl, `.key` against the value dict is `null` → no match → no reaction, so this fails. Also assert a message with `key="other"` produces NO reaction (still committed).

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/unit/test_mock_kafka_reactor.py::test_reactor_match_on_key_envelope -q`
Expected: FAIL — match evaluated against `value`, `.key` is null, no reaction.

- [ ] **Step 3: Root the reactor match at the message**

Behavior to implement in `KafkaReactor._handle` (around lines 145-148): change the predicate input from `value` to the full `msg` dict. Keep `value = msg.get("value")` for the non-object skip guard (Step 1) and the implicit capture context. Do not change capture (`resolve_captures(msg, ...)` already roots at `msg`).

- [ ] **Step 4: Update existing reactor match tests + run the suite**

Update the payload-form match expressions in this file to envelope form, preserving each test's intent:
- `sample_config` fixture (~line 139): `match='.command == "CREATE_ORDER"'` → `match='.value.command == "CREATE_ORDER"'`.
- `test_reactor_match_capture_react` (~243): same change (the scripted message value carries `command`).
- `test_reactor_jq_non_match_commits_no_event` (~333): `match='.command == "SHIP_ORDER"'` → `match='.value.command == "SHIP_ORDER"'` (the non-match still commits and emits nothing).

Run: `pytest tests/unit/test_mock_kafka_reactor.py -q`
Expected: PASS — all match tests now use `.value.<field>` (envelope-rooted) and the new `.key` reach test passes.

- [ ] **Step 5: Commit**

Run: `git add -A && git commit -m "feat(mock)!: Kafka reactor match roots at message envelope (#22)"`

---

### Task 4: `kafka assert/consume --match` + `kafka.patterns` → message envelope

Root the CLI/pattern jq predicate at the whole Kafka message so `.key` and `.headers.<name>` become matchable. `--contains`/`--path` stay value-rooted.

**Files:**
- Modify: `agctl/commands/kafka_commands.py:187-189` (consume inline predicate), `agctl/commands/kafka_commands.py:293-313` (`_build_assert_predicate` match + pattern branches)
- Test: `tests/unit/test_kafka_commands.py`

**Interfaces:**
- Consumes: `_build_assert_predicate(*, contains, match, path, pattern_match, params) -> Callable[[dict], bool]`; the consume inline `predicate(msg)` closure.
- Produces: unchanged signatures. Contract change: the predicate's `--match` and `--pattern` branches evaluate `jq_bool(msg, expr)` (whole message) instead of `jq_bool(value, expr)`. The `--contains`/`--path` branch STILL targets `value = msg.get("value")` — only the jq-`.`-rooted branches move.

- [ ] **Step 1: Add the failing envelope-root reach test**

Add `test_kafka_assert_match_on_key` to `tests/unit/test_kafka_commands.py`, mirroring the existing `install_fake` + `monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)` seam used by `test_kafka_assert_match_filters_with_valid_expr`: the fake client's `find_in_window` returns one message with `value={"eventType":"X"}` and `key="ord-1"`. Run the `kafka assert` Click command with `--match '.key == "ord-1"'` and assert `ok:true` / `matched:true`. Under the current value-rooted predicate, `.key` against the value is null → no match → `AssertionError`, so this fails.

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/unit/test_kafka_commands.py::test_kafka_assert_match_on_key -q`
Expected: FAIL — predicate evaluates `.key` against the value dict, no match, assertion error.

- [ ] **Step 3: Root the CLI/pattern match at the message**

Behavior to implement in `agctl/commands/kafka_commands.py`:
- In `_build_assert_predicate`'s inner `predicate(msg)`: keep `value = msg.get("value")` for the contains/path branch; change the `--match` branch to `jq_bool(msg, match)` and the `--pattern` branch to `jq_bool(msg, filled_pattern_match)`. Leave the `--contains`/`--path` branch on `value`.
- In `_kafka_consume_core`'s inline predicate: change `jq_bool(msg["value"], _expr)` to `jq_bool(msg, _expr)`.

- [ ] **Step 4: Update existing kafka match tests + run the suite**

Update payload-form `--match` expressions in this file to envelope form (`.foo` → `.value.foo`) so each test asserts the same intent under the new root. Specifically update the `--match` expressions in `test_kafka_consume_match_filters_messages`, `test_kafka_consume_filter_key_is_alias_of_match`, `test_kafka_assert_match_filters_with_valid_expr`, and any other test whose `--match` references a value field directly. Do NOT touch `--contains`/`--path` tests (their rooting is unchanged). If `kafka.patterns` are exercised in `tests/fixtures/agctl.yaml` or inline with payload-form `match`, update those pattern `match` strings to `.value.<field>` form as well.

Run: `pytest tests/unit/test_kafka_commands.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add -A && git commit -m "feat(kafka)!: --match and patterns root at message envelope (#22)"`

---

### Task 5: `http call/request --match` → response envelope

Root the HTTP `--match` predicate at the full response result so `.status_code`, `.headers.<name>`, `.body.<field>` are reachable. `--status`/`--contains`/`--jq-path`/`--equals` unchanged.

**Files:**
- Modify: `agctl/assertions.py:270-276` (the `match` branch of `evaluate_http_assertions`)
- Test: `tests/unit/test_http_commands.py`

**Interfaces:**
- Consumes: `evaluate_http_assertions(result, *, status, contains, match, jq_path, equals) -> None`; `result` is the full HttpClient response dict `{status_code, response_time_ms, headers(lowercased), body, url, method}`.
- Produces: unchanged signature. Contract change: the `--match` branch calls `jq_bool(result, match)` instead of `jq_bool(result["body"], match)`. The `--contains` branch (`json_subset(needle, result["body"])`), the `--jq-path` branch (`jq_value(result["body"], jq_path)`), and the `--status` branch are UNCHANGED.

- [ ] **Step 1: Add the failing envelope-root reach test**

Add `test_http_request_match_on_status_code` to `tests/unit/test_http_commands.py`, mirroring the `_transport_returning(status, body)` + `http_commands.set_default_transport(...)` + `CliRunner` pattern of `test_http_request_match_assertion_pass`: install a transport returning status `201` with body `{}`; run `http request` with `--match '.status_code == 201'`; assert `ok:true`. Under the current body-rooted impl, `.status_code` against the body is null → predicate false → assertion failure (exit 1), so this fails.

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/unit/test_http_commands.py::test_http_request_match_on_status_code -q`
Expected: FAIL — `.status_code` resolves against the body, predicate false, assertion error.

- [ ] **Step 3: Root the HTTP match at the response result**

Behavior to implement in `agctl/assertions.py::evaluate_http_assertions`: in the `if match is not None:` branch, change the `jq_bool` input from `result["body"]` to `result`. Leave the `contains`, `jq_path`, and `status` branches untouched.

- [ ] **Step 4: Update existing http match tests + run the suite**

Update payload-form `--match` expressions to envelope form. Specifically: `test_http_request_match_assertion_pass` (~424) `--match '.status=="PENDING"'` → `--match '.body.status=="PENDING"'`; `test_http_request_match_assertion_fail` (~454) → `--match '.body.status=="PENDING"'` (body `{"status":"PAID"}` still fails). Leave `--contains`/`--jq-path`/`--equals`/`--status` tests unchanged.

Run: `pytest tests/unit/test_http_commands.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add -A && git commit -m "feat(http)!: --match roots at response envelope (#22)"`

---

### Task 6: `agctl config migrate` subcommand

New command that rewrites a legacy config to dialect `"2"` by prepending `.body | ` (HTTP) / `.value | ` (Kafka) to every `match` expression and bumping `version`. Backs up the original; `--dry-run` previews.

**Files:**
- Create: `agctl/config/migrate.py`
- Create: `tests/unit/test_migrate.py`
- Modify: `agctl/commands/config_commands.py` (add `config_migrate` Click command)
- Modify: `agctl/cli.py` (register `config_migrate` on the `config` group, alongside `config_validate`/`config_show`/`config_init`; add to `__all__`)

**Interfaces:**
- Consumes: `discover_config_path(explicit=, env=)` and `ConfigError` from `agctl.config.loader`; `yaml.safe_load` / `yaml.safe_dump` (PyYAML).
- Produces:
  - `agctl/config/migrate.py::migrate_match_exprs(config: dict) -> MigrateResult`.
    - `MigrateResult` is a typed structure with fields: `config: dict` (a deep-copied, transformed config), `from_version: str` (the original raw `version` value, or `""` if absent), `to_version: str` (always `"2"`), `rewrites: list[dict]` (each `{"path": str, "before": str, "after": str}`, in traversal order), `already_v2: bool`.
    - Semantics: compute `source_major = str(config.get("version", "")).split(".")[0]`. If `source_major == "2"`: return `already_v2=True`, `config` unchanged (deep copy), `rewrites=[]`. Otherwise (`"1"`, `""`, or any other legacy value): `already_v2=False`; deep-copy the config; set `config["version"] = "2"`; walk the three match-site families and, for each expression that does NOT already `startswith` its transport prefix, prepend the prefix and append a rewrite record:
      - `mocks.http.stubs.<name>.match.jq` → prefix `.body | ` (only when `match` and `match.jq` are present).
      - `mocks.kafka.reactors.<name>.match` → prefix `.value | ` (only when `match` is present, a string).
      - `kafka.patterns.<name>.match` → prefix `.value | ` (only when `match` is present, a string).
    - Defensive `.get()` chains: a missing `mocks`/`kafka`/`patterns` section contributes no sites (never raises).
    - `capture.*.from` and `match.body` are NOT visited (out of scope).
  - `agctl/commands/config_commands.py::config_migrate` — a `@click.command("migrate")` taking `--config` (default None) and `--dry-run` (flag, default False), `@click.pass_context`. It mirrors `config_validate`/`config_init`: resolve `path = config_path or ctx.obj.get("config_path")`; resolve the on-disk path via `discover_config_path`; read raw text; `yaml.safe_load` to a dict (do NOT call `load_config` — that would reject a v1 config); call `migrate_match_exprs`. Emit the standard envelope:
    - `command`: `"config.migrate"`.
    - On `already_v2=True`: `result={"path": <str>, "already_v2": True, "from_version": <str>, "to_version": "2", "rewritten": [], "cli_flags_note": <see below>}`, `ok=True`, no file write even without `--dry-run`.
    - Otherwise: `result={"path": <str>, "already_v2": False, "from_version": <str>, "to_version": "2", "rewritten": <rewrites list>, "cli_flags_note": <see below>}`. If `--dry-run`: do not write. Else: write a backup of the original text to `<path>.bak` (overwrite if present), then write `yaml.safe_dump(result.config, sort_keys=False, default_flow_style=False)` back to `<path>`. `ok=True`.
    - `cli_flags_note` is a fixed string reminding the operator that CLI `--match` flags in shell scripts / agent prompts are NOT rewritten by this command and must be prefixed manually with `.body | ` (HTTP) / `.value | ` (Kafka).
    - On `ConfigError` (e.g. sample/path errors): emit `ok=False`, `error={"type":"ConfigError", ...}` and `raise SystemExit(2)`, mirroring `config_validate`'s error handling.
  - The command is registered on the `config` Click group in `agctl/cli.py` exactly as `config_validate`/`config_show`/`config_init` are.

- [ ] **Step 1: Write the failing helper tests**

Create `tests/unit/test_migrate.py`. Tests (each constructs a Python dict literal and calls `migrate_match_exprs`):
1. `test_migrate_http_stub_match_jq`: input `{"version":"1","mocks":{"http":{"stubs":{"s":{"match":{"jq":".amount > 1000"}}}}}}` → `result.config["version"]=="2"`, `result.already_v2 is False`, `result.config["mocks"]["http"]["stubs"]["s"]["match"]["jq"] == ".body | .amount > 1000"`, and `result.rewrites` has one entry `{"path":"mocks.http.stubs.s.match.jq","before":".amount > 1000","after":".body | .amount > 1000"}`.
2. `test_migrate_reactor_match`: input `{"version":"1","mocks":{"kafka":{"reactors":{"r":{"match":".command == \"X\""}}}}}` → after == `.value | .command == "X"`, path `mocks.kafka.reactors.r.match`.
3. `test_migrate_kafka_pattern_match`: input `{"version":"1","kafka":{"patterns":{"p":{"match":".eventType == \"Y\""}}}}` → after == `.value | .eventType == "Y"`, path `kafka.patterns.p.match`.
4. `test_migrate_idempotent_and_already_v2`: take the output config of test 1 (already version `"2"`, expr already prefixed) and call `migrate_match_exprs` again → `result.already_v2 is True`, `result.rewrites == []`, and the expr is unchanged. Separately, a config with `version:"2"` and a v2-native expr like `.body.amount` (no prefix) → `already_v2=True`, NOT rewritten (the helper does not touch already-v2 configs).
5. `test_migrate_leaves_capture_and_match_body_untouched`: input `{"version":"1","mocks":{"http":{"stubs":{"s":{"match":{"body":{"a":1},"jq":".x"},"capture":{"c":{"from":".body.c"}}}}}}}` → `match.jq` rewritten (`.body | .x`), `match.body` dict unchanged, `capture.c.from` unchanged (`".body.c"`).
6. `test_migrate_missing_sections_no_crash`: input `{"version":"1"}` → `result.rewrites == []`, `result.config["version"]=="2"`.

- [ ] **Step 2: Run the helper tests to verify they fail**

Run: `pytest tests/unit/test_migrate.py -q`
Expected: FAIL — `migrate_match_exprs` is not defined (ImportError).

- [ ] **Step 3: Implement the helper**

Create `agctl/config/migrate.py` with `MigrateResult` and `migrate_match_exprs(config: dict) -> MigrateResult` implementing exactly the Produces contract above. Use `copy.deepcopy` for the transformed config. Traversal order for `rewrites`: HTTP stubs first (in dict order), then Kafka reactors (in dict order), then `kafka.patterns` (in dict order).

- [ ] **Step 4: Run the helper tests to verify they pass**

Run: `pytest tests/unit/test_migrate.py -q`
Expected: PASS.

- [ ] **Step 5: Write the command tests**

Add to `tests/unit/test_migrate.py`, using `CliRunner` against `agctl.cli.cli` (mirror the `_validate` helper in `test_config_commands.py`):
1. `test_config_migrate_dry_run_writes_nothing`: write a v1 config with a stub `match.jq: ".amount > 1000"` to `tmp_path/agctl.yaml`; invoke `config migrate --config <path> --dry-run`; assert exit 0, `result.rewritten[0].after == ".body | .amount > 1000"`, and the on-disk file is UNCHANGED (still `version: "1"`, expr still `.amount > 1000`), and no `.bak` file exists.
2. `test_config_migrate_writes_file_and_backup`: invoke without `--dry-run`; assert exit 0, an `agctl.yaml.bak` exists whose content equals the original, the rewritten `agctl.yaml` now has `version: "2"` and expr `.body | .amount > 1000`, AND the rewritten file loads successfully via `load_config(path, env={})` (round-trip — proves the migration yields a valid v2 config).
3. `test_config_migrate_already_v2`: write `version: "2"` config; invoke; assert exit 0, `result.already_v2 is True`, `result.rewritten == []`, no `.bak` written, file unchanged.
4. `test_config_migrate_result_carries_cli_flags_note`: assert the emitted JSON `result` contains a `cli_flags_note` string mentioning `--match`.

- [ ] **Step 6: Run the command tests to verify they fail**

Run: `pytest tests/unit/test_migrate.py -q`
Expected: the helper tests PASS; the four command tests FAIL — `config migrate` is not a registered subcommand (`Error: No such command 'migrate'`).

- [ ] **Step 7: Implement + register the command**

Implement `config_migrate` in `agctl/commands/config_commands.py` per the Produces contract; register it on the `config` group in `agctl/cli.py` (mirror exactly how `config_validate`, `config_show`, `config_init` are imported and added), and add `config_migrate` to that module's `__all__`.

- [ ] **Step 8: Run the command tests + full suite to verify they pass**

Run: `pytest tests/unit/test_migrate.py -q && pytest tests/unit -q`
Expected: PASS (both). The full suite confirms no regression.

- [ ] **Step 9: Commit**

Run: `git add -A && git commit -m "feat(config): add 'config migrate' for v1→v2 dialect rewrite (#22)"`

---

### Task 7: Documentation sync

Sync DESIGN.md, ARCHITECTURE.md, and the consumer skill to the v2 dialect. Per CLAUDE.md, invoke the `docs-watcher` subagent for DESIGN/ARCHITECTURE altitude; make targeted edits for the concrete match examples and the `config migrate` command.

**Files:**
- Modify: `docs/DESIGN.md` (§2.1 version note + capture/match root comment; §2.1 `kafka.patterns` example; §3.1/§3.2 `--match` semantics + CLI migration note; §3.6 add `config migrate`; §4 error table dialect row; §10 remove the #22 deferred row; §11 use-case `--match`/`.match` examples; §6 AGENTS.md template examples)
- Modify: `docs/ARCHITECTURE.md` (§5 version-gate-as-dialect + `_check_version` pointer; §9 match eval sites now envelope-rooted; §15 remove the `.`-divergence bullet)
- Modify: `skills/agctl-config/reference/` (mock + kafka patterns + CLI `--match` authoring: the three envelope roots, header-casing asymmetry, the `.body | `/`.value | ` legacy form, `config migrate`, the CLI-flag migration rule)
- Modify: `skills/agctl-config/reference/init-config.md:65` (`version: "1"` → `"2"`)

**Interfaces:**
- Consumes: the implemented v2 semantics (Tasks 1-6).
- Produces: docs matching as-built behavior.

- [ ] **Step 1: Update concrete match examples to v2 form**

In `docs/DESIGN.md`: change the `kafka.patterns` example match strings from payload form (`.eventType == ...`, `.payload.orderId == ...`) to `.value.<field>` form (§2.1); change every `--match`/`.match` example in §3.1, §3.2, §11 use-cases, and the §6 `AGENTS.md` template to the v2 form (HTTP `--match` → `.body.<field>` / response form; kafka `--match` → `.value.<field>`). Update the §2.1 capture-block comment that says `match` stays payload-rooted to state both `match` and `capture` are envelope-rooted under dialect `"2"`, and set the schema-reference `version: "2"`.

- [ ] **Step 2: Document the dialect switch + config migrate**

In `docs/DESIGN.md §3.6`: add a `config migrate` subsection describing `--config`, `--dry-run`, the backup, the prepend rewrite, the `already_v2` no-op, and the CLI-flags-not-rewritten caveat (mirror Task 6's contract). In §4 error table / §10: note the dialect mismatch is a `ConfigError` exit 2 with the migrate pointer, and remove the #22 deferred roadmap row.

- [ ] **Step 3: Run docs-watcher for altitude sync**

Invoke the `docs-watcher` subagent (per CLAUDE.md "Docs Sync") to reconcile DESIGN.md (WHAT/WHY) and ARCHITECTURE.md (HOW) against the implemented change: ARCHITECTURE §5 (version guard gates dialect; `_check_version` pointer), §9 (the five `match` eval sites are now envelope-rooted; mock `match` and `capture` share a root), §15 (remove the `match`/`capture` `.`-divergence bullet; add a one-line note that `match` is envelope-rooted under dialect `"2"`).

- [ ] **Step 4: Update the consumer skill reference**

In `skills/agctl-config/reference/`: update mock, `kafka.patterns`, and CLI `--match` authoring to document the three envelope roots, the HTTP-lowercased vs Kafka-case-sensitive header asymmetry, the `.body | `/`.value | ` legacy/migrated form, and `config migrate`. Update `init-config.md` line 65 to `version: "2"`.

- [ ] **Step 5: Verify docs-driven tests still pass + commit**

Run: `pytest tests/unit -q`
Expected: PASS (no test should depend on the prose you just edited; the sample-config/README drift-guard was already satisfied in Task 1).

Run: `git add -A && git commit -m "docs: sync to dialect v2 — envelope-rooted match + config migrate (#22)"`

---

### Task 8: Integration / E2E coverage

Extend the mock integration scenarios so `match` exercises the envelope root, and add a `config migrate` end-to-end. Self-skipping without `AGCTL_TEST_LIVE=1`.

**Files:**
- Modify: `tests/integration/test_mock_commands.py` (or `test_mock_capture_e2e.py`)
- Modify/Create: a `config migrate` E2E in `tests/integration/` (or unit, if no live dependency — `config migrate` needs no services, so prefer adding it to `tests/unit/test_migrate.py` unless a live load is exercised)

**Interfaces:**
- Consumes: Tasks 1-6.
- Produces: E2E confidence that `match` and `capture` share a root on a real server/broker, and that `config migrate` round-trips.

- [ ] **Step 1: Extend mock scenarios so match uses the envelope root**

In the HTTP mock integration test: add a stub carrying BOTH `match.jq` (envelope-rooted, e.g. `.body.amount > 1000`) and a `capture` of `.body.amount`, then assert the stub matches and the captured value renders — proving match and capture share the envelope root on a real server. In the Kafka reactor integration test: add a reactor whose `match` is `.value.command == "X"` AND that captures `.headers.rqUID`, asserting the reaction is produced (match reaches `.value`, capture reaches `.headers`). Keep the `AGCTL_TEST_LIVE=1` self-skip guards intact.

- [ ] **Step 2: Add a config-migrate end-to-end (no live deps → unit)**

In `tests/unit/test_migrate.py`: add `test_config_migrate_full_round_trip_e2e` — write a v1 fixture mirroring a real mock config (HTTP stub `match.jq`, Kafka reactor `match`, a `kafka.patterns` entry, and a `capture`), run `config migrate` (no `--dry-run`), then `load_config(<rewritten path>, env={})` and assert it loads as dialect `"2"` with the match exprs prefixed and `capture.from` unchanged. (This is the acceptance proof that a migrated legacy config is a valid v2 config.)

- [ ] **Step 3: Run unit tests; note integration skip**

Run: `pytest tests/unit -q`
Expected: PASS.
Run: `pytest tests/integration -q`
Expected: the mock E2E tests SKIP (no `AGCTL_TEST_LIVE=1`). If `AGCTL_TEST_LIVE=1` is set locally, they should PASS against the live container harness.

- [ ] **Step 4: Commit**

Run: `git add -A && git commit -m "test: E2E envelope-root match + config migrate round-trip (#22)"`

---

## Self-Review (completed)

**1. Code scan:** No method bodies, algorithms, or test code appear in the plan. Each step states behavior/expected results/signatures, not code. The prepend prefixes (`.body | ` / `.value | `) and envelope field shapes are contracts (data shapes), not implementation.

**2. Self-containment:** Each task lists exact files (with line numbers), Consumes/Produces contracts (signatures, data shapes, error cases), and test scenarios with expected results. A zero-context implementer can do any task alone.

**3. Spec coverage:** D1 dialect switch → Task 1; D2 prepend + D3 `config migrate` → Task 6; D4 gate pointer → Task 1; D5 CLI-flags docs note → Task 6 (`cli_flags_note`) + Task 7; D6 reuse envelopes → Tasks 2-5; D7 semver 1.0.0 → Task 1. Spec §6 envelope roots → Tasks 2 (HTTP req), 3/4 (Kafka msg), 5 (HTTP resp). Spec §7 migration → Task 6. Spec §8 component surface → Tasks 1-6. Spec §9 error model → Tasks 1 (gate), 6 (migrate). Spec §10 testing → Tasks 2-6 unit + Task 8 E2E. Spec §13 docs → Task 7. All five match sites covered (Tasks 2-5).

**4. Placeholder scan:** No TBD/TODO/"add error handling"/"similar to Task N". Each test step names scenario + expected result; each impl step names behavior.

**5. Type consistency:** `migrate_match_exprs(config: dict) -> MigrateResult` and `MigrateResult` fields (`config`, `from_version`, `to_version`, `rewrites`, `already_v2`) are referenced consistently across Task 6's helper tests, command tests, and Task 8's round-trip. `TOOL_MAJOR_VERSION == "2"` consistent across Task 1 and the gate test. Envelope field names consistent across Tasks 2/3/4/5 and Global Constraints.
