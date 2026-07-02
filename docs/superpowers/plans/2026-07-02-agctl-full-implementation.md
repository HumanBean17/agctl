# agctl Full Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement every remaining command group and extension point in `docs/DESIGN.md` (v0.6): HTTP (`call`/`request`/`ping`), Kafka (`consume`/`produce`/`assert`), DB (`query`/`assert`), `check ready`, `discover`, the plugin/entry-point system (§9), and the config validator — so that the only stdout write path is `output.emit()` and every command returns the §4.2 result shape with deterministic exit codes.

**Architecture:** One shared infrastructure layer (`errors`, `command` envelope helper, `params`/`resolution`, `assertions`/jq) consumed by protocol clients (`clients/`) that **lazy-import** their library so the package works without the optional extra installed. Each command group lives in `commands/` and is registered onto the Click root group in `cli.py`. Protocol-command logic is unit-tested via dependency injection (DB driver, Kafka client) and `httpx.MockTransport` (HTTP); live-service tests live in `tests/integration/` and self-skip when no broker/DB is present.

**Tech Stack:** Python ≥3.11, Click 8, Pydantic v2, PyYAML, httpx (HTTP), psycopg 3 (DB), jq (predicates), confluent-kafka (Kafka), pytest. Protocol libs are optional extras (`pip install -e .[http|kafka|db]`); all are installed in this dev environment.

**Existing code to build on (already merged on `main`, 38 tests):**
- `agctl/output.py` — `emit(ok, command, result=None, error=None, duration_ms=0)` (§4.1). **Do not change its signature.**
- `agctl/config/{models,loader,resolver}.py` — pydantic schema, `${VAR}` interpolation (D3), `AGCTL_*` overrides (D4), §5 discovery, version check (M4). `ConfigError(message, detail)` currently defined in `loader.py`.
- `agctl/cli.py` — root `cli` group + `config` subgroup (`validate`/`show`) with secret masking. Flat structure today; this plan grows it into the §7 `commands/` package layout.

**Conventions every task follows:**
- **TDD:** write a failing test → implement → green → commit. One logical change per commit; conventional-commit messages (`feat:`, `fix:`, `test:`, `refactor:`).
- **stdout discipline:** the ONLY permitted stdout write is `output.emit()`. Intermediate output → stderr (or nowhere). `http ping` is the sole exception (NDJSON stream).
- **Exit codes:** 0 success · 1 assertion/timeout failure · 2 config/connection/template/internal error. Never `sys.exit()` with a bare int inside a command — raise the right `AgctlError` subclass and let the envelope wrapper emit + exit.
- **No new runtime deps** beyond the declared extras. Lazy-import protocol libs inside the client/driver method that needs them.
- Run the full suite after each task: `python -m pytest -q` must stay green.

**Testability strategy (read this before T6+):**
- HTTP: inject `transport` (an `httpx.MockTransport` or `httpx.BaseTransport`) into the client in tests. No real server.
- DB: the `DBDriver` protocol + `DbClient(driver=...)` injection. Unit tests register a `FakeDriver` or pass one directly. `psycopg` execution is exercised only in skipped integration tests.
- Kafka: split **pure matching** (`jq_bool`, `json_subset`, window logic) — unit-tested with no broker — from **consume/produce mechanics** (lazy `confluent_kafka`, tested with a `FakeConsumer`/`FakeProducer` or skipped integration tests).
- jq (no server): `--match`, `--contains`, `--path`, `--equals` coercion are all directly unit-testable.

---

## Slice A — Shared infrastructure

### Task A1: Error hierarchy + envelope command wrapper

**Files:**
- Create: `agctl/errors.py`
- Create: `agctl/command.py`
- Modify: `agctl/config/loader.py` (import `ConfigError` from `errors` instead of defining it)
- Modify: `agctl/config/__init__.py` (keep re-exporting `ConfigError`, `load_config`)
- Test: `tests/unit/test_errors.py`, `tests/unit/test_command.py`

**`agctl/errors.py`** — the error-type table from DESIGN §4.1. Each subclass carries its `type_name` and `exit_code`:

```python
class AgctlError(Exception):
    type_name = "InternalError"
    exit_code = 2
    def __init__(self, message: str, detail: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or {}
    def to_dict(self) -> dict:
        return {"type": self.type_name, "message": self.message, "detail": self.detail}

class AssertionFailure(AgctlError):   # exit 1, type "AssertionError"
    type_name = "AssertionError"; exit_code = 1
class ConfigError(AgctlError):        # exit 2, type "ConfigError"  (MOVED from loader.py)
    type_name = "ConfigError"; exit_code = 2
class ConnectionFailure(AgctlError):  # exit 2, type "ConnectionError"
    type_name = "ConnectionError"; exit_code = 2
class OperationTimeout(AgctlError):   # exit 1, type "TimeoutError"
    type_name = "TimeoutError"; exit_code = 1
class TemplateMissing(AgctlError):    # exit 2, type "TemplateNotFound"
    type_name = "TemplateNotFound"; exit_code = 2
```

`loader.py`: replace its local `class ConfigError(...)` with `from ..errors import ConfigError`. Keep the same `.message`/`.detail` attributes so existing tests pass. `config/__init__.py` must still export `ConfigError`.

**`agctl/command.py`** — the envelope wrapper every command uses:

```python
import time
from .output import emit
from .errors import AgctlError

def elapsed_ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)

def envelope(command: str):
    """Decorator: run a Click callback, emit one envelope, set exit code.

    The wrapped function returns the `result` dict (or None) on success.
    AgctlError subclasses are caught and emitted with their type/exit code.
    Any other exception becomes an InternalError (exit 2).
    """
    import functools
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            start = time.monotonic()
            try:
                result = fn(*args, **kwargs)
            except AgctlError as err:
                emit(ok=False, command=command, error=err.to_dict(), duration_ms=elapsed_ms(start))
                raise SystemExit(err.exit_code)
            except SystemExit:
                raise
            except Exception as exc:  # noqa: BLE001 — last-resort guard
                emit(ok=False, command=command,
                     error={"type": "InternalError", "message": str(exc), "detail": {}},
                     duration_ms=elapsed_ms(start))
                raise SystemExit(2)
            emit(ok=True, command=command, result=result, duration_ms=elapsed_ms(start))
        return wrapper
    return deco

def load_config_or_raise(config_path):
    """Load config for a command; ConfigError propagates (caught by envelope -> exit 2)."""
    from .config import load_config
    return load_config(config_path)
```

- [ ] **Step 1:** Write `tests/unit/test_errors.py` — assert each subclass's `type_name` and `exit_code`, and that `to_dict()` returns `{"type","message","detail"}`. Confirm `from agctl.config import ConfigError` still works and is the same class as `from agctl.errors import ConfigError`.
- [ ] **Step 2:** Write `tests/unit/test_command.py` — a fake Click command wrapped with `@envelope("demo")`: returns dict → emits `ok:true` with that result; raises `AssertionFailure` → emits `error.type=="AssertionError"`, raises `SystemExit(1)`; raises `AssertionError` python builtin is NOT caught specially (it's an Exception → InternalError exit 2)? **Decision:** Python's built-in `AssertionError` must map to our `AssertionError` envelope + exit 1 too (so a raw `assert` inside a command behaves like an assertion failure). Add `except AssertionError as err` BEFORE the generic `except Exception` that emits `type:"AssertionError"`, exit 1. Test all three paths and that `SystemExit` re-raises cleanly.
- [ ] **Step 3:** Implement `errors.py` and `command.py`; move `ConfigError` to `errors.py`; update `loader.py` import.
- [ ] **Step 4:** Run `python -m pytest -q`. All green (existing 38 + new).
- [ ] **Step 5:** Commit: `feat: AgctlError hierarchy + envelope command wrapper (DESIGN §4.1)`

> **Note for later tasks:** `cli.py`'s existing `config validate`/`show` keep their special ConfigError handling (they build the `{valid, errors:[...]}` result shape). Do NOT wrap them in `@envelope` — they have bespoke output. All NEW command groups use `@envelope`.

---

### Task A2: Param parsing + template resolution (D2, D5)

**Files:**
- Create: `agctl/params.py`
- Create: `agctl/resolution.py`
- Test: `tests/unit/test_params.py`, `tests/unit/test_resolution.py`

**`agctl/params.py`:**
```python
def parse_params(values: tuple[str, ...]) -> dict[str, str]:
    """Turn repeatable '--param k=v' tuples into a dict. 'k=v' only; bare 'k' -> ConfigError."""
```
Value may contain `=` (split on first `=`). Empty tuple → `{}`.

**`agctl/resolution.py`** — fills placeholders and merges bodies:
```python
def fill_placeholders(value, params: dict[str, str]):
    """Replace {name} in strings (and inside nested str/dict/list structures)."""

def deep_merge(base, override):
    """D5: objects merge recursively; arrays in override replace base arrays wholesale;
    scalar leaves from override win. base is not mutated. Works on dict/list/scalar."""

def convert_sql_params(sql: str) -> str:
    """D2: rewrite JDBC-style ':name' to psycopg '%(name)s'. Leave ':' inside string
    literals alone if practical (see test). Name chars: [A-Za-z_][A-Za-z0-9_]*."""
```
- `fill_placeholders`: applies to HTTP path strings and body structures (body values may be strings with `{name}`). A `{name}` with no matching param is left as the literal `{name}` (template-variable validation is deferred — DESIGN §10). 
- `deep_merge` is the **D5 body-merge** algorithm (the §5-verification scenario).

- [ ] **Step 1:** `test_params.py` — `["a=1","b=2"]`→`{"a":"1","b":"2"}`; `["x=a=b"]`→`{"x":"a=b"}`; `[]`→`{}`; `["bare"]`→`ConfigError`.
- [ ] **Step 2:** `test_resolution.py` — D5 cases: `deep_merge({"a":1,"b":{"c":2}},{"b":{"d":3}})`→`{"a":1,"b":{"c":2,"d":3}}`; arrays replaced: `deep_merge({"x":[1,2]},{"x":[3]})`→`{"x":[3]}`; scalar wins: `deep_merge({"a":1},{"a":2})`→`{"a":2}`; no mutation of base. `fill_placeholders("/orders/{order_id}",{"order_id":"o9"})`→`"/orders/o9"`; nested body fill. `convert_sql_params("WHERE id = :orderId AND s=:status")`→`"WHERE id = %(orderId)s AND s=%(status)s"`.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Tests green.
- [ ] **Step 5:** Commit: `feat: param parsing + placeholder fill + D5 deep-merge + SQL param conversion`

---

### Task A3: Assertion primitives — jq, subset, equals coercion (D8)

**Files:**
- Create: `agctl/assertions.py`
- Test: `tests/unit/test_assertions.py`

This module backs Kafka `--match`/`--contains`/`--path` and DB `--path`/`--equals`. jq is importable now (no server).

```python
def jq_bool(value, expr: str) -> bool:
    """Evaluate a jq predicate against value; True only if result is truthy.
    A jq error or falsy result -> False (silently skipped per DESIGN §3.2)."""

def jq_value(value, expr: str):
    """Evaluate a jq path/value expression (e.g. '.status'); raises AgctlError on bad expr? 
    Decision: return None on jq error (paths that miss yield None). Use for --path."""

def json_subset(needle, haystack) -> bool:
    """--contains: True if every key/element in needle is present-and-equal in haystack
    (recursive for nested dict/list). Subset, not equality."""

def parse_equals(text: str):
    """D8 step 1-2: json.loads(text) if valid JSON else the raw string."""

def coerce_db_value(value):
    """D8 step 3: numbers->int/float, booleans->bool, None->None,
    datetime/date/time->ISO 8601 string, everything else unchanged."""

def type_aware_equal(expected, actual) -> bool:
    """D8 step 4: strict, type-aware equality. 0 != '0'. """
```
Implementation notes:
- `jq`: use `import jq`; `jq.compile(expr).input(value).all()`. For `jq_bool`, compile, run, take `all()`, return `True` iff any output value is truthy; wrap `jq` exceptions → `False`. For `jq_value`, run `.all()`, return first output or `None`; exceptions → `None`.
- `coerce_db_value`: psycopg returns `datetime.datetime`/`datetime.date`/`datetime.time`/`Decimal`/`UUID` etc. Handle: `Decimal`→int if integral else float; `datetime`→`.isoformat()`; `UUID`→`str`; `bool` stays bool; `int/float/str/None` unchanged. (Import `datetime`, `decimal`, `uuid` at module top — stdlib, always available.)

- [ ] **Step 1:** `test_assertions.py` — `jq_bool({"a":1},".a==1")`→True; `jq_bool({"a":2},".a==1")`→False; `jq_bool({"a":1},".b==1")`→False (no error); bad expr `jq_bool({},")(")`→False. `json_subset({"x":1},{"x":1,"y":2})`→True; `json_subset({"x":2},{"x":1})`→False; nested `json_subset({"o":{"a":1}},{"o":{"a":1,"b":2}})`→True. `parse_equals("0")`→int 0; `parse_equals("true")`→True; `parse_equals("[1,2]")`→[1,2]; `parse_equals("null")`→None; `parse_equals("CONFIRMED")`→"CONFIRMED". `coerce_db_value(Decimal("5"))`→5; `coerce_db_value(datetime(2026,6,29,14,22))`→"2026-06-29T14:22:00"; `coerce_db_value(None)`→None. `type_aware_equal(0,"0")`→False; `type_aware_equal(0,0)`→True; `type_aware_equal("CONFIRMED","CONFIRMED")`→True.
- [ ] **Step 2:** Implement.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: assertion primitives — jq predicates, json subset, D8 equals coercion`

---

### Task A4: Config validator (dangling refs + description warnings)

**Files:**
- Create: `agctl/config/validator.py`
- Modify: `agctl/cli.py` (`config validate` now reports structural errors + warnings)
- Test: `tests/unit/test_validator.py`

`validate_config(cfg: Config) -> tuple[list[dict], list[dict]]` returns `(errors, warnings)` where each entry is `{"path": str, "message": str}`.

Checks:
- **Template→service dangling ref:** for each HTTP template, `template.service` must be a key in `cfg.services`. Error path `templates.<name>.service`.
- **DB template→connection dangling ref:** for each `database.templates[<name>]` with a `.connection` set, that connection must exist in `cfg.database.connections`. Path `database.templates.<name>.connection`.
- **Default connection dangling:** if `cfg.defaults.database_connection` is set, it must exist in `cfg.database.connections`.
- **Kafka pattern.topic:** patterns are referenced by name at call time; no static ref to validate beyond presence. (Skip.)
- **Description warnings (§3.6):** every HTTP template, DB template, and Kafka pattern **missing** a `description` → a warning `{"path": ..., "message": "missing description (discovery degrades without it)"}`. NOT an error.

`config validate` wiring in `cli.py`:
- On success: `load_config` already validated schema/version/interpolation. Then run `validate_config(cfg)`.
- If `errors` non-empty → emit `ok:false`, `result={"valid": False, "errors": errors, "warnings": warnings}`, `error={"type":"ConfigError","message":"<summary>","detail":{"errors":...}}`, exit 2.
- If `errors` empty → `ok:true`, `result={"valid": True, "warnings": warnings}`, exit 0. (Warnings do not fail validation.)

- [ ] **Step 1:** `test_validator.py` — a good config (the existing fixture) → `errors==[]`. A config with an HTTP template pointing at a non-existent service → one error at `templates.<name>.service`. A DB template with a dangling connection → error. `defaults.database_connection` missing → error. A template with no description → a warning, no error. Compose: errors present → `config validate` CLI emits `ok:false` exit 2 with both errors and warnings in result.
- [ ] **Step 2:** Implement `validator.py`; wire into `config validate`.
- [ ] **Step 3:** Tests green (including the existing `test_cli.py` validate tests — update them if the result shape gained a `warnings` key; keep `valid`/`errors` behavior).
- [ ] **Step 4:** Commit: `feat: config validator — dangling refs (§3.5) + missing-description warnings (§3.6)`

---

## Slice B — HTTP

### Task B1: HTTP client (httpx, MockTransport-injectable)

**Files:**
- Create: `agctl/clients/__init__.py`
- Create: `agctl/clients/http_client.py`
- Test: `tests/unit/test_http_client.py`

```python
class HttpClient:
    def __init__(self, base_url, timeout_seconds, *, transport=None, headers=None): ...
    def request(self, method, path, *, headers=None, body=None, params=None) -> dict:
        """Send one request. Returns the §4.2 http.call result:
        {status_code, response_time_ms, headers, body, url, method}.
        - body: dict/list/None; JSON-encoded.
        - headers: merged (per-call wins over client default; case-insensitive keys preserved as-is in output but matched case-insensitively when merging).
        - params: dict for query string (used by 'http request'? DESIGN doesn't list query params; keep optional, unused for now).
        Maps httpx.ConnectError -> ConnectionFailure; httpx.TimeoutException -> OperationTimeout."""
```
- Lazy `import httpx` inside `__init__` (or top of module guarded). Construct `httpx.Client(base_url=..., timeout=..., transport=transport or None)`.
- `response_time_ms` measured around the `.request()` call.
- `body` of the envelope is the parsed JSON if response content-type is JSON, else the decoded text. Non-JSON body → leave as string under `body`.
- `headers` in result: lowercased keys (per §4.2 example shows `content-type`, `x-request-id`). Use response headers lowercased.

- [ ] **Step 1:** `test_http_client.py` using `httpx.MockTransport`:
  - 200 JSON → result has `status_code=200`, `body=={...}`, `response_time_ms` is int ≥0, `headers` lowercased, `url`/`method` set.
  - 404 non-JSON text → `body` is the text string, `status_code=404`.
  - Connection refused (mock raises `httpx.ConnectError`) → `ConnectionFailure` raised.
  - Timeout (mock raises `httpx.ReadTimeout`) → `OperationTimeout` raised.
- [ ] **Step 2:** Implement.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: httpx HttpClient with transport injection (DESIGN §7 clients)`

---

### Task B2: `http call` + `http request` commands (D5 body-merge)

**Files:**
- Create: `agctl/commands/__init__.py`
- Create: `agctl/commands/http_commands.py`
- Modify: `agctl/cli.py` (register `http` group; transport-injection seam for tests)
- Test: `tests/unit/test_http_commands.py`

**`http call <template-name>`** flags: `--param` (multiple), `--body` (JSON string), `--header` (multiple `k=v`), `--timeout`, plus the global `--config`. Behavior:
- Load config. Look up `cfg.templates[name]`; missing → `TemplateMissing` (path `templates.<name>`).
- Resolve service via `template.service` → `cfg.services[...]`; missing service → `ConfigError` (caught by validator normally, but guard here too).
- Fill `{placeholder}` in `template.path` and `template.body` using `parse_params`. If `--body` given, `json.loads` it and `deep_merge(template_body, body_override)` (D5).
- Merge headers: template headers (also `{}`-filled) as base, `--header` overrides win.
- Resolve timeout: `--timeout` > `service.timeout_seconds` > `cfg.defaults.timeout_seconds` > 10.
- Build `HttpClient`, call `.request(method, path, headers=, body=)`, return its result (the §4.2 shape). **`ok:true` regardless of HTTP status** — a 4xx/5xx is a successful *request*, not an assertion failure. (DESIGN: `http call` has no assertion mode.)

**`http request`** (free-form): flags `--service`, `--method`, `--path`, `--body`, `--header`, `--timeout`. Resolve service, build client, send. Same result shape.

**Test seam:** the command builder function (`build_http_call`/`build_http_request`) must accept an injectable `transport` so tests don't hit the network. Pattern: a module-level `_default_transport = None` plus a `CliRunner`-based test that monkeypatches `http_commands.new_client` (a thin factory `new_client(base_url, timeout, transport=None)`) — or pass transport through `ctx.obj["_transport"]`. **Decision:** add `http_commands.set_default_transport(transport)` for tests, used by `new_client` when set. Keep it simple.

- [ ] **Step 1:** `test_http_commands.py` (via `click.testing.CliRunner` + MockTransport):
  - `http call get-order --param order_id=o9` against a mock returning 200 JSON → stdout JSON has `command:"http.call"`, `ok:true`, `result.status_code==200`, `result.url` ends with `/api/v1/orders/o9`.
  - D5 merge: template body `{"customer_id":"{customer_id}","items":[...]}`, `--body '{"priority":"high"}'` → sent body has both `customer_id` filled and `priority:"high"`; verify via the request captured by MockTransport.
  - `http call missing-tpl` → `ok:false`, `error.type=="TemplateNotFound"`, exit 2.
  - `http request --service order-service --method GET --path /x` → `command:"http.request"`.
  - Header merge: `--header X-Foo=1` overrides template header.
- [ ] **Step 2:** Implement commands + register `http` group in `cli.py`.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: http call + http request with D5 body-merge and header merge (DESIGN §3.1)`

---

### Task B3: `http ping` — streaming NDJSON (M5 exception)

**Files:**
- Modify: `agctl/commands/http_commands.py` (add `ping`)
- Test: `tests/unit/test_http_ping.py`

`http ping <template> | --service --path`, flags: `--interval`, `--method`, `--body`, `--header`, `--duration`, `--until-stopped`, `--timeout`. **Does NOT use `@envelope`** — it streams.

Behavior:
- Emits one JSON line per ping: `{"ping": n, "ok": bool, "status_code": int|null, "duration_ms": int, "timestamp": "ISO8601Z"}`. On non-2xx, add `"error": "Unexpected status <code>"` and `ok:false`.
- `ok` per ping = status code is 2xx.
- Loops until `--duration` seconds elapse OR signal received (when `--until-stopped`, default). Use `signal.SIGTERM`/`SIGINT` handlers that set a stop flag and, after the current ping, emit the summary and exit.
- On stop: emit summary `{"summary": true, "total_pings": N, "failed_pings": F, "duration_ms": D}` then exit 0 if `F==0` else 1.
- `--duration` and `--until-stopped` are mutually exclusive; neither given → `--until-stopped` implied.

**Time handling in tests:** `time.monotonic()` and `time.time()` are available (this is real code, not a workflow script). Use small `--interval 0.01 --duration 0.05` in tests, or monkeypatch the sleep/loop count. Provide a test seam: a `ping_loop(..., max_pings=N, sleep_fn=...)` core so tests run a bounded number of pings against a MockTransport without real waiting.

- [ ] **Step 1:** `test_http_ping.py` — bounded loop (e.g. `max_pings=3`) against a MockTransport returning 200: output has 3 ping lines + 1 summary line; summary `total_pings==3, failed_pings==0`; exit 0. One ping returns 401 → that line `ok:false` has `error`; summary `failed_pings==1`; exit 1. Verify stdout is newline-delimited JSON (parse each line). `--duration` + `--until-stopped` together → error exit 2.
- [ ] **Step 2:** Implement `ping` with a bounded, injectable core loop + signal handling.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: http ping streaming NDJSON + summary (DESIGN §3.1, M5)`

---

## Slice C — Database

### Task C1: DBDriver protocol + PostgreSQL driver

**Files:**
- Create: `agctl/clients/db_driver_protocol.py`
- Create: `agctl/clients/db_drivers/__init__.py`
- Create: `agctl/clients/db_drivers/postgresql.py`
- Test: `tests/unit/test_postgresql_driver.py`

```python
# db_driver_protocol.py
from typing import Protocol, Any
class DBDriver(Protocol):
    def connect(self, config: dict) -> None: ...
    def execute(self, sql: str, params: dict) -> list[dict[str, Any]]: ...
    def close(self) -> None: ...
```

```python
# db_drivers/postgresql.py
class PostgreSQLDriver:
    def connect(self, config: dict) -> None:
        # lazy import psycopg; build dsn from host/port/dbname/user/password;
        # store a psycopg connection. Map connect errors -> ConnectionFailure.
    def execute(self, sql: str, params: dict) -> list[dict[str, Any]]:
        # convert_sql_params(sql) -> %(name)s; cursor.execute(rewritten, params);
        # rows -> list of dict (dict(cursor, row)); coerce_db_value each cell.
    def close(self) -> None: ...
```
- Lazy `import psycopg` inside `connect`. `psycopg.connect(...)` using keyword args (`host=, port=, dbname=, user=, password=`). Use `psycopg.errors.OperationalError` → `ConnectionFailure`.
- `execute`: translate `:name`→`%(name)s`, run, return `list[dict]` (column name → coerced value). Query timeout → `OperationTimeout`.
- **Unit-testable without a DB:** `convert_sql_params` is already covered in A2; here test the driver's `execute` SQL translation by injecting a fake connection object (the driver should accept `connect` being given a pre-built connection for tests, OR test `execute` against a stub cursor). **Decision:** `PostgreSQLDriver(connectable=None)` — if a `connectable` (connection) is passed, use it instead of connecting. Tests pass a `FakeConn` with a `cursor()` returning a `FakeCursor` whose `.execute(sql,params)` records the rewritten SQL + params and `.fetchall()` returns canned rows; `.description` gives column names.

- [ ] **Step 1:** `test_postgresql_driver.py` — with a `FakeConn`: `execute("WHERE id=:orderId", {"orderId":"o9"})` records rewritten SQL `%(orderId)s` + params; returns rows coerced (e.g. a `Decimal` cell → int). `close()` calls the conn's `close()`.
- [ ] **Step 2:** Implement protocol + driver.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: DBDriver protocol + PostgreSQL driver with lazy psycopg (DESIGN §9.1)`

---

### Task C2: DbClient — driver dispatch via entry points

**Files:**
- Create: `agctl/clients/db_client.py`
- Test: `tests/unit/test_db_client.py`

```python
class DbClient:
    def __init__(self, connection: DatabaseConnection, *, driver: DBDriver | None = None,
                 drivers: dict[str, type] | None = None): ...
    @classmethod
    def load_drivers(cls) -> dict[str, type]:
        """Entry-point group 'agctl.db_drivers' -> {type_name: driver_class}. Lazy importlib.metadata.
        Always includes built-in 'postgresql'. Missing lib -> the driver still loads (lazy import);
        only connect() fails."""
    def connect(self): ...
    def execute(self, sql, params) -> list[dict]: ...
    def close(self): ...
```
- `load_drivers`: `importlib.metadata.entry_points(group="agctl.db_drivers")`; load each → `{ep.name: ep.load()}`. Merge with built-in `{"postgresql": PostgreSQLDriver}`.
- If `driver` passed directly (DI), use it. Else pick `drivers[connection.type]`; unknown `type` → `ConfigError(f"Unknown database type: {type}")`.

- [ ] **Step 1:** `test_db_client.py` — inject a `FakeDriver`; `DbClient(conn, driver=FakeDriver()).execute(...)` delegates and returns rows. `load_drivers()` includes `"postgresql"`. Unknown `type` (no driver, no DI) → `ConfigError`. (Do NOT require a real DB.)
- [ ] **Step 2:** Implement.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: DbClient entry-point driver dispatch (DESIGN §9.1)`

---

### Task C3: `db query` + `db assert` commands (D8)

**Files:**
- Create: `agctl/commands/db_commands.py`
- Modify: `agctl/cli.py` (register `db` group)
- Test: `tests/unit/test_db_commands.py`

Shared resolution for all `db` commands:
- `--template` XOR `--sql` (neither or both → `ConfigError`). `--template <name>` → `cfg.database.templates[name]` (missing → `TemplateMissing`); `sql = template.sql`. `--sql` → free-form.
- Connection: `--connection` > `template.connection` > `cfg.defaults.database_connection`; missing connection name in `cfg.database.connections` → `ConfigError`. Connection resolved → build `DbClient`.
- `--param` → `parse_params`. `execute(convert + sql, params)`.

**`db query`**: returns §4.2 `db.query` shape: `{"rows":[...], "row_count":N, "connection": <name>}`.

**`db assert --expect-rows N`**: count rows; if `!= N` → `AssertionFailure(detail={expected, actual, sql, connection})`. On pass returns `db.assert` shape `{"assertion_type":"expect_rows","expected":N,"actual":N,"passed":true,"sql":...,"connection":...}`.

**`db assert --expect-value --path P --equals V`** (D8): take first row (0 rows → `AssertionFailure` "no rows"); evaluate `jq_value(row, P)`; `expected = parse_equals(V)`; `actual = coerce_db_value(jq_value(...))`; if `not type_aware_equal(expected, actual)` → `AssertionFailure(detail={path, expected, actual})`. Pass → `db.assert` shape `{"assertion_type":"expect_value","path":P,"expected":...,"actual":...,"passed":true,"connection":...}`.

`--expect-rows` and `--expect-value` are mutually exclusive assertion modes; exactly one required → else `ConfigError`.

**Test seam:** `db_commands` uses a factory `new_db_client(connection)` that tests monkeypatch to return a `DbClient` backed by a `FakeDriver` (DI). 

- [ ] **Step 1:** `test_db_commands.py` with a FakeDriver (returns canned rows):
  - `db query --template find-order --param orderId=o9` → `command:"db.query"`, `result.row_count==1`, `result.connection=="main-db"`.
  - `db assert --sql "SELECT 1 ..." --param order_id=o9 --expect-rows 1` with 1 row → `ok:true`, `result.assertion_type=="expect_rows"`. With 0 rows → `ok:false`, `error.type=="AssertionError"`, exit 1, detail has expected/actual.
  - `db assert --expect-value --path .status --equals CONFIRMED` with row `{status:"CONFIRMED"}` → pass. With `{status:"PENDING"}` → AssertionError exit 1.
  - D8: `--equals "0"` vs numeric 0 → pass; `--equals "0"` vs string "0" → fail (type-aware).
  - `--template` + `--sql` together → exit 2.
  - Missing template → TemplateNotFound exit 2.
- [ ] **Step 2:** Implement + register `db` group.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: db query + db assert (expect-rows/expect-value, D8 coercion) (DESIGN §3.3)`

---

## Slice D — Kafka

### Task D1: Kafka client — produce + consume/lookback mechanics

**Files:**
- Create: `agctl/clients/kafka_client.py`
- Test: `tests/unit/test_kafka_client.py`

```python
class KafkaClient:
    def __init__(self, brokers, group_id=None, *, consumer_factory=None, producer_factory=None): ...
    def produce(self, topic, value: dict, *, key=None, headers=None) -> dict:
        """Returns §4.2 kafka.produce shape {topic, partition, offset, key, timestamp}.
        Lazy confluent_kafka.Producer. flush; map delivery/conn errors -> ConnectionFailure."""
    def consume_window(self, topic, *, lookback_seconds, timeout_seconds,
                       from_beginning=False) -> list[dict]:
        """Seek each partition to (now - lookback) via offsets_for_times (or earliest with
        from_beginning), poll forward until timeout. Return list of message dicts:
        {key, value(parsed JSON or str), partition, offset, timestamp, headers}.
        Lazy confluent_kafka.Consumer. Connection errors -> ConnectionFailure."""
```
- Lazy `import confluent_kafka` inside produce/consume.
- **Test seam:** `consumer_factory`/`producer_factory` let tests inject fakes (no broker). A `FakeConsumer` implements `subscribe`/`assignment`/`offsets_for_times`/`poll`/`close` returning canned messages; a `FakeProducer` implements `produce`/`flush` recording calls and returning partition/offset.
- Pure matching (`jq_bool`, `json_subset`) is NOT in the client — it's applied by the command layer using `agctl/assertions.py`.

- [ ] **Step 1:** `test_kafka_client.py` with FakeProducer/FakeConsumer:
  - `produce("t", {"a":1}, key="k")` → result has topic/key + partition/offset/timestamp from the fake.
  - `consume_window("t", lookback_seconds=10, timeout_seconds=5)` returns the fake's messages; `from_beginning=True` seeks earliest.
- [ ] **Step 2:** Implement client.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: KafkaClient produce + windowed consume with lookback (DESIGN §3.2, D6)`

---

### Task D2: `kafka produce` + `consume` + `assert` commands (D6, D10)

**Files:**
- Create: `agctl/commands/kafka_commands.py`
- Modify: `agctl/cli.py` (register `kafka` group)
- Test: `tests/unit/test_kafka_commands.py`

**`kafka produce`**: `--topic`, `--message` (JSON str), `--key`, `--header` (multiple). Parse `--message` JSON → dict. Build `KafkaClient` from `cfg.kafka.brokers`. Return `kafka.produce` shape.

**`kafka consume`**: `--topic`, `--timeout`, `--lookback`, `--match` (jq), `--filter-key` (deprecated alias of `--match`), `--expect-count`, `--from-beginning`, `--consumer-group`. Resolve timeout/lookback (`--lookback` default = `--timeout`; `--timeout` default = `cfg.kafka.timeout_seconds` or 30). Get messages via `consume_window`. Apply `--match` (`jq_bool`) filter if given. If `--expect-count` set and matched count `< n` → `AssertionFailure` (D10: consume-count-miss is AssertionError, exit 1). Return `kafka.consume` shape `{"topic","messages":[...],"count","timed_out":bool}`.

**`kafka assert`**: three modes combinable — `--contains` (json_subset), `--match` (jq_bool), `--pattern` (named `cfg.kafka.patterns[name]`; fills `{placeholder}` in its `match` via `--param`; topic inferred from pattern). `--topic` required unless `--pattern`. `--path` narrows `--contains` to a sub-path via `jq_value`. `--lookback`/`--timeout`/`--from-beginning`/`--consumer-group` as consume. **A match = a message satisfying ALL given modes.** Loop the window: if a matching message found → return `kafka.assert` shape `{"topic","matched":true,"matching_message":{...},"messages_scanned":N,"elapsed_ms":M}`. If window elapses with no match → `AssertionFailure` (D10: assert-timeout is AssertionError, exit 1).

**Test seam:** `new_kafka_client(brokers, group)` factory monkeypatched to return a client built with FakeConsumer/FakeProducer holding canned messages.

- [ ] **Step 1:** `test_kafka_commands.py`:
  - `produce` → `command:"kafka.produce"`, result has topic/partition/offset.
  - `consume --topic t --timeout 5 --expect-count 1` with a fake returning one matching msg → `ok:true`. Returning zero → `ok:false`, `error.type=="AssertionError"`, exit 1.
  - `--match '.x==1'` filters out non-matching messages.
  - `assert --topic t --contains '{"a":1}' --timeout 5` with a matching msg → `ok:true`, `result.matched==true`. No match in window → AssertionError exit 1.
  - `assert --pattern order-created --param orderId=o9 --timeout 10` → pattern's match filled + applied; topic inferred.
  - D6: a message older than the lookback window is NOT matched (fake's `offsets_for_times` enforces the seek); `--from-beginning` includes it.
- [ ] **Step 2:** Implement + register `kafka` group.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: kafka produce/consume/assert with D6 lookback + D10 error typing (DESIGN §3.2)`

---

## Slice E — check + discover

### Task E1: `check ready` (D7)

**Files:**
- Create: `agctl/commands/check_commands.py`
- Modify: `agctl/cli.py` (register `check` group)
- Test: `tests/unit/test_check_commands.py`

`check ready` flags: `--service`, `--all`, `--timeout`. D7: neither flag → check all configured services. For each service: GET `<base_url><health_path>` (or `GET /` if no `health_path`); ready = 2xx. Return §4.2 `check.ready` shape `{"services": {name: {ready, status_code, url, response_time_ms, error?}}, "all_ready": bool}`. Per-service connection failure → that service `{ready:false, status_code:null, error:"..."}`, NOT a command-level error (the command still `ok:true`). Build `HttpClient` per service (injectable transport for tests).

- [ ] **Step 1:** `test_check_commands.py` via MockTransport: `--service order-service` → checks one; no flags → checks all (D7); a service down (ConnectError) → `ready:false` with error, `all_ready:false`, command `ok:true`; all up → `all_ready:true`.
- [ ] **Step 2:** Implement + register `check` group.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: check ready with D7 default-to-all (DESIGN §3.4)`

---

### Task E2: `discover` — summary / category / item / search (D9)

**Files:**
- Create: `agctl/commands/discover_commands.py`
- Modify: `agctl/cli.py` (register `discover`)
- Test: `tests/unit/test_discover_command.py`

`agctl discover` (Level 0) → `discover.summary`: counts `{services, http_templates, kafka_patterns, db_templates}` + `hint`. `--category <name>` (Level 1) → `discover.category`: `{category, count, items:[{name,description}], hint}`. `--category <name> --name <item>` (Level 2) → `discover.item`: full schema. `--search <term>` → `discover.search`: flat matches across categories by name/description substring.

Category detail (Level 2) shapes per §4.2:
- `services`: `{name, description?, base_url, health_path?}` (services have no description field — omit; include base_url/health_path).
- `http-templates`: `{category, name, description, method, service, path, params:[...], example}`.
- `kafka-patterns`: `{category, name, description, topic, match?, params:[...], example}`.
- `db-templates`: `{category, name, description, connection, sql, params:[...], example}` — **D9: `sql` included.**
- `params`: extract placeholders from the relevant string — HTTP `{name}` (from path + body), Kafka `{name}` (from match), DB `:name` (from sql). Use regex extraction. Unknown category / item → `ConfigError`/`TemplateNotFound` (exit 2). `--category` + `--search` mutually exclusive.

`example` strings: match §3.6 examples, e.g. `agctl http call <name> --param ...`, `agctl db query --template <name> --param ...`, `agctl kafka assert --pattern <name> --param ... --timeout 10`.

- [ ] **Step 1:** `test_discover_command.py` against the existing fixture config:
  - `discover` → counts match fixture; hint present.
  - `discover --category http-templates` → items are `{name, description}` only (no params/sql).
  - `discover --category db-templates --name find-order` → result has `sql` field (D9), `params:["orderId"]`, example.
  - `discover --search payment` → matches across categories by name/description.
  - unknown category → exit 2; unknown item → exit 2.
- [ ] **Step 2:** Implement + register `discover`.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: discover summary/category/item/search with D9 SQL (DESIGN §3.6)`

---

## Slice F — Plugin / entry-point system (§9)

### Task F1: Register entry points + wire plugin loading (§9.1, §9.2)

**Files:**
- Modify: `pyproject.toml` (add `[project.entry-points."agctl.db_drivers"]` postgresql + empty `agctl.plugins` group)
- Modify: `agctl/cli.py` (load `agctl.plugins` groups and register them)
- Modify: `agctl/clients/db_client.py` (now driven by entry points already in C2 — confirm `postgresql` resolves via entry point after reinstall)
- Test: `tests/unit/test_plugins.py`

`pyproject.toml` additions:
```toml
[project.entry-points."agctl.db_drivers"]
postgresql = "agctl.clients.db_drivers.postgresql:PostgreSQLDriver"

[project.entry-points."agctl.plugins"]
# third-party plugins register here
```
`cli.py`: after registering built-in groups, iterate `importlib.metadata.entry_points(group="agctl.plugins")`; for each, `ep.load()` to get an object exposing `.command_group` (a `click.Group`) and `.validate_config(config)`; `cli.add_command(group)` using its `.name`. Wrap each load in try/except → on failure, emit nothing fatal (log to stderr) so a broken plugin doesn't brick the CLI. **Guard the whole loop** so a missing/empty group is a no-op.

After editing pyproject, **reinstall** (`pip install -e .`) so the entry point is registered, then test discovery.

- [ ] **Step 1:** `test_plugins.py` — `DbClient.load_drivers()["postgresql"] is PostgreSQLDriver` (real entry-point resolution, not the built-in fallback). `cli` invocation with a synthetic entry point (use `importlib.metadata` monkeypatch or a real tiny test plugin) registers an extra command group. Broken plugin entry point does not crash `cli --help`.
- [ ] **Step 2:** Edit pyproject, reinstall, wire plugin loading in `cli.py`.
- [ ] **Step 3:** Tests green.
- [ ] **Step 4:** Commit: `feat: plugin/entry-point registration for db_drivers + plugins (DESIGN §9.1, §9.2)`

---

### Task F2: Custom assertion registry (§9.3) + final wiring & integration scaffolding

**Files:**
- Create: `agctl/assertion_registry.py`
- Modify: `agctl/commands/db_commands.py` / `kafka_commands.py` (built-in assertion modes registered)
- Create: `tests/integration/__init__.py`, `tests/integration/conftest.py` (skip hooks)
- Create: `tests/integration/test_http_commands.py`, `test_db_commands.py`, `test_kafka_commands.py` (self-skipping)
- Test: `tests/unit/test_assertion_registry.py`

**`agctl/assertion_registry.py`:** a small registry `AssertionRegistry` keyed by mode name → assertion callable. Built-ins (`expect_rows`, `expect_value` for db; `contains`, `match`, `pattern` for kafka) register at import. Third-party modes load via entry-point group `agctl.assertions`. `get(name)` → `TemplateMissing` for unknown mode. The db/kafka commands consult the registry so the assertion modes are pluggable (§9.3) while built-ins remain primary.

**Integration tests** self-skip: `conftest.py` provides `require_http_service`, `require_postgres`, `require_kafka` fixtures that `pytest.skip()` when the service/extra is unavailable (probe a TCP connect / `import` the lib; do not assume Docker). Each integration test file documents the env vars / service it needs (matching `tests/fixtures/agctl.yaml`). These exist to document the live path and to run in CI later; they must not fail in this dev environment.

- [ ] **Step 1:** `test_assertion_registry.py` — built-in modes present; `get("expect_rows")` returns a callable; unknown → raises. A registered custom mode resolves. Entry-point loading is wrapped (missing group = no-op).
- [ ] **Step 2:** `test_integration_*` — each test marked to skip cleanly when no service (`pytest.skip("requires live <X>")`).
- [ ] **Step 3:** Implement registry + wire built-ins; add integration scaffolding.
- [ ] **Step 4:** `python -m pytest -q` — all unit green, integration skipped.
- [ ] **Step 5:** Commit: `feat: assertion registry (§9.3) + integration-test scaffolding (self-skipping)`

---

## Final verification (controller, after all tasks)

- [ ] `python -m pytest -q` — all unit tests green; integration skipped (no broker/DB).
- [ ] `agctl --help`, `agctl http --help`, `agctl kafka --help`, `agctl db --help`, `agctl check --help`, `agctl discover --help`, `agctl config --help` all render.
- [ ] End-to-end smoke (using `tests/fixtures/agctl.yaml` with env vars set): `agctl config validate`, `agctl discover`, `agctl discover --category http-templates`, `agctl discover --category db-templates --name find-order` (shows `sql`).
- [ ] Dispatch a **final holistic reviewer** subagent over the full diff (correctness vs DESIGN §3/§4, error-type/exit-code consistency, stdout discipline, lazy-import discipline, no dead code).
- [ ] Use superpowers:finishing-a-development-branch (merge to `main` locally, per established preference).

---

## Self-review notes (spec coverage)

- §2 schema, §2.2 interp, §2.3 param syntax, §5 resolution — **already merged** (foundation).
- §3.1 http call/request/ping + D5 → B1/B2/B3. §3.2 kafka consume/produce/assert + D6 → D1/D2. §3.3 db query/assert + D8 → C1–C3. §3.4 check ready + D7 → E1. §3.5 config validate (validator) → A4; config show already merged. §3.6 discover + D9 → E2.
- §4.1 envelope + error types (D10) → A1 (errors) applied across all command tasks. §4.2 result shapes → enforced in each command task's tests.
- §7 `commands/` + `clients/` layout → realized across B–F. §7 output.py interface → unchanged.
- §8 principles (one envelope, windowed assertions, fail-fast, `__` overrides) → honored by construction (envelope wrapper, D6 lookback, ConfigError propagation).
- §9.1 db_drivers → C1/C2/F1. §9.2 plugins → F1. §9.3 assertions → F2.
- §10 Open Questions → intentionally NOT implemented (deferred per spec).
- DESIGN §5 verification scenarios: D5 (A2/B2), D6 (D1/D2), D8 (A3/C3), D10 (A1 + per-command), D9 (E2), D1-regression (already merged) — all covered by a task.
