# agctl Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the agctl foundation — installable package, JSON output envelope, full config-loading pipeline (discovery → `${VAR}` interpolation → `AGCTL_*` overrides → pydantic validation), and a minimal Click CLI with `config validate` / `config show` — with TDD tests including the D1 regression (a full `agctl.yaml` keeps both `database.connections` and `database.templates`).

**Architecture:** Pure-Python foundation (no network/protocol code yet). Config flows: resolve path (§5 order) → parse YAML → interpolate `${...}` → apply `AGCTL_*` `__` overrides → pydantic-validate → `Config` object. Every CLI command emits exactly one JSON envelope via `output.emit()` (§4.1) and sets a deterministic exit code. `ConfigError` (exit 2) is the failure type for all config problems.

**Tech Stack:** Python ≥3.11, Click 8, Pydantic v2, PyYAML, pytest. (Protocol deps — httpx, confluent-kafka, psycopg, jq — are deliberately excluded from this slice; they arrive with their command slices.)

**Spec:** [`docs/DESIGN.md`](../../DESIGN.md) §2, §2.2, §3.5, §4.1, §5, §7; decision rationale in [`docs/superpowers/specs/2026-07-02-agctl-design-hardening.md`](../specs/2026-07-02-agctl-design-hardening.md) (D1, D3, D4, M4).

---

## Out of scope (later plans)

HTTP client/commands, Kafka client/commands, DB client/commands, `check`, `discover`, plugin/entry-point registration, retry/polling, schema registry. Do not implement any of these here.

## File Structure

```
agctl/                                 # package
├── __init__.py
├── output.py                          # emit() — the single stdout write path (§4.1)
├── cli.py                             # Click root group + `config` subgroup
└── config/
    ├── __init__.py                    # re-exports load_config, ConfigError
    ├── models.py                      # pydantic v2 schema models (§2)
    ├── resolver.py                    # AGCTL_* __ override layer (§5, D4)
    └── loader.py                      # discovery, interpolation, load_config, ConfigError
tests/
├── __init__.py
├── unit/
│   ├── __init__.py
│   ├── test_output.py
│   ├── test_models.py
│   ├── test_interpolation.py
│   ├── test_resolver.py
│   ├── test_discovery.py
│   └── test_loader.py
└── fixtures/
    └── agctl.yaml                     # full valid config (the D1 regression fixture)
pyproject.toml
```

Responsibilities: `output.py` owns all stdout; `models.py` is pure data shape; `resolver.py` only applies env overrides to a dict; `loader.py` orchestrates path resolution + interpolation + validation and owns `ConfigError`; `cli.py` wires Click and translates exceptions into envelopes + exit codes.

---

## Task 1: Project scaffolding

**Files:**
- Create: `pyproject.toml`, `agctl/__init__.py`, `agctl/config/__init__.py`, `tests/__init__.py`, `tests/unit/__init__.py`, `tests/unit/test_smoke.py`

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "agctl"
version = "0.1.0"
description = "Agent-facing CLI harness for testing distributed systems"
requires-python = ">=3.11"
dependencies = [
    "click>=8.1",
    "pyyaml>=6.0",
    "pydantic>=2.0",
]

[project.optional-dependencies]
http = ["httpx>=0.27"]
kafka = ["confluent-kafka>=2.4"]
db = ["psycopg[binary]>=3.1", "jq>=1.6"]
dev = ["pytest>=8.0"]

[project.scripts]
agctl = "agctl.cli:cli"
agt = "agctl.cli:cli"

[tool.hatch.build.targets.wheel]
packages = ["agctl"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 2: Write the package marker**

`agctl/__init__.py`:
```python
"""agctl — agent-facing CLI harness for testing distributed systems."""
```

`agctl/config/__init__.py` (empty for now):
```python
"""Configuration loading pipeline."""
```

`tests/__init__.py` and `tests/unit/__init__.py`: empty files.

- [ ] **Step 3: Write the failing smoke test**

`tests/unit/test_smoke.py`:
```python
def test_package_imports():
    import agctl

    assert agctl.__name__ == "agctl"
```

- [ ] **Step 4: Install the package and run the test to verify it passes**

Run: `pip install -e ".[dev]"`
Run: `pytest tests/unit/test_smoke.py -v`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml agctl/ tests/
git commit -m "feat: scaffold agctl package (pyproject, package dirs, pytest)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 2: Output envelope

**Files:**
- Create: `agctl/output.py`
- Test: `tests/unit/test_output.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_output.py`:
```python
import json

from agctl.output import emit


def test_emit_writes_envelope(capsys):
    emit(ok=True, command="http.call", result={"status_code": 200}, duration_ms=12)
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload == {
        "ok": True,
        "command": "http.call",
        "result": {"status_code": 200},
        "error": None,
        "duration_ms": 12,
    }
    assert out.endswith("\n")


def test_emit_defaults(capsys):
    emit(ok=False, command="db.assert", error={"type": "AssertionError", "message": "x"})
    payload = json.loads(capsys.readouterr().out)
    assert payload["result"] is None
    assert payload["duration_ms"] == 0
    assert payload["error"] == {"type": "AssertionError", "message": "x"}


def test_emit_serializes_non_json_via_default_str(capsys):
    class Thing:
        def __str__(self):
            return "THING"

    emit(ok=True, command="x", result=Thing())
    payload = json.loads(capsys.readouterr().out)
    assert payload["result"] == "THING"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_output.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'agctl.output'`).

- [ ] **Step 3: Write minimal implementation**

`agctl/output.py`:
```python
"""JSON output envelope — the only permitted stdout write path (DESIGN §4.1)."""

import json
import sys
from typing import Any


def emit(
    ok: bool,
    command: str,
    result: Any = None,
    error: dict | None = None,
    duration_ms: int = 0,
) -> None:
    """Write exactly one JSON envelope to stdout and flush. Call once per invocation."""
    payload = {
        "ok": ok,
        "command": command,
        "result": result,
        "error": error,
        "duration_ms": duration_ms,
    }
    sys.stdout.write(json.dumps(payload, default=str))
    sys.stdout.write("\n")
    sys.stdout.flush()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_output.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add agctl/output.py tests/unit/test_output.py
git commit -m "feat: add JSON output envelope emit() (DESIGN §4.1)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 3: Config schema models (pydantic v2)

**Files:**
- Create: `agctl/config/models.py`
- Test: `tests/unit/test_models.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_models.py`:
```python
import pytest
from pydantic import ValidationError

from agctl.config.models import Config


def _full_config_dict():
    return {
        "version": "1",
        "services": {
            "order-service": {"base_url": "http://localhost:8081", "health_path": "/health"}
        },
        "kafka": {
            "brokers": ["localhost:9092"],
            "default_consumer_group": "agctl-consumer",
            "patterns": {
                "order-created": {
                    "topic": "orders.created",
                    "match": '.eventType == "ORDER_CREATED"',
                }
            },
        },
        "database": {
            "connections": {
                "main-db": {"type": "postgresql", "host": "h", "default": True},
            },
            "templates": {
                "find-order": {"connection": "main-db", "sql": "SELECT 1 FROM orders WHERE id = :orderId"},
            },
        },
        "templates": {
            "create-order": {"method": "POST", "service": "order-service", "path": "/orders"},
        },
        "defaults": {"timeout_seconds": 10, "database_connection": "main-db"},
    }


def test_full_config_validates_with_connections_and_templates():
    """D1 regression: both database.connections and database.templates survive."""
    cfg = Config.model_validate(_full_config_dict())
    assert cfg.database.connections["main-db"].type == "postgresql"
    assert cfg.database.templates["find-order"].connection == "main-db"
    assert cfg.templates["create-order"].method == "POST"
    assert cfg.kafka.patterns["order-created"].topic == "orders.created"


def test_empty_sections_default():
    cfg = Config.model_validate({"version": "1"})
    assert cfg.services == {}
    assert cfg.database.connections == {}
    assert cfg.database.templates == {}
    assert cfg.kafka.brokers == []


def test_http_template_requires_method_service_path():
    with pytest.raises(ValidationError):
        Config.model_validate({"version": "1", "templates": {"x": {"method": "GET"}}})


def test_db_template_requires_sql():
    with pytest.raises(ValidationError):
        Config.model_validate(
            {"version": "1", "database": {"templates": {"x": {"connection": "c"}}}}
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_models.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'agctl.config.models'`).

- [ ] **Step 3: Write minimal implementation**

`agctl/config/models.py`:
```python
"""Pydantic v2 schema models for agctl.yaml (DESIGN §2)."""

from typing import Any

from pydantic import BaseModel, Field


class Defaults(BaseModel):
    timeout_seconds: int | None = None
    database_connection: str | None = None


class ServiceConfig(BaseModel):
    base_url: str
    health_path: str | None = None
    timeout_seconds: int | None = None


class KafkaPattern(BaseModel):
    description: str | None = None
    topic: str
    match: str | None = None


class KafkaConfig(BaseModel):
    brokers: list[str] = Field(default_factory=list)
    default_consumer_group: str | None = None
    schema_registry_url: str | None = None
    timeout_seconds: int | None = None
    patterns: dict[str, KafkaPattern] = Field(default_factory=dict)


class DatabaseConnection(BaseModel):
    type: str
    host: str | None = None
    port: int | None = None
    dbname: str | None = None
    user: str | None = None
    password: str | None = None
    default: bool = False


class DatabaseTemplate(BaseModel):
    description: str | None = None
    connection: str | None = None
    sql: str


class DatabaseConfig(BaseModel):
    connections: dict[str, DatabaseConnection] = Field(default_factory=dict)
    templates: dict[str, DatabaseTemplate] = Field(default_factory=dict)


class HttpTemplate(BaseModel):
    description: str | None = None
    method: str
    service: str
    path: str
    headers: dict[str, str] = Field(default_factory=dict)
    body: Any = None


class Config(BaseModel):
    version: str
    services: dict[str, ServiceConfig] = Field(default_factory=dict)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    templates: dict[str, HttpTemplate] = Field(default_factory=dict)
    defaults: Defaults = Field(default_factory=Defaults)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_models.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add agctl/config/models.py tests/unit/test_models.py
git commit -m "feat: pydantic config schema models (DESIGN §2, D1 structure)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 4: Environment-variable interpolation

**Files:**
- Create: `agctl/config/loader.py`
- Test: `tests/unit/test_interpolation.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_interpolation.py`:
```python
import pytest

from agctl.config.loader import ConfigError, interpolate


def test_required_var_resolved():
    assert interpolate("${VAR}", {"VAR": "hello"}) == "hello"


def test_partial_substitution():
    assert interpolate("http://${HOST}:8080", {"HOST": "db"}) == "http://db:8080"


def test_required_var_missing_raises():
    with pytest.raises(ConfigError) as exc:
        interpolate("${MISSING}", {})
    assert "MISSING" in exc.value.detail["variables"]


def test_lists_all_unresolved():
    with pytest.raises(ConfigError) as exc:
        interpolate({"a": "${A}", "b": "${B}"}, {})
    assert set(exc.value.detail["variables"]) == {"A", "B"}


def test_optional_with_default():
    assert interpolate("${VAR:-fallback}", {}) == "fallback"


def test_optional_empty():
    assert interpolate("${VAR:-}", {}) == ""


def test_optional_ignored_when_set():
    assert interpolate("${VAR:-fallback}", {"VAR": "real"}) == "real"


def test_interpolates_nested_structures():
    data = {"kafka": {"brokers": ["${BROKER}:9092"]}, "n": 5}
    out = interpolate(data, {"BROKER": "kafka"})
    assert out == {"kafka": {"brokers": ["kafka:9092"]}, "n": 5}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_interpolation.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'agctl.config.loader'`).

- [ ] **Step 3: Write minimal implementation**

`agctl/config/loader.py`:
```python
"""Config loading pipeline: discovery, interpolation, validation (DESIGN §2.2, §5)."""

import re
from typing import Any

_VAR_RE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(:-([^}]*))?\}")


class ConfigError(Exception):
    """Raised for any config problem. Maps to exit code 2 (DESIGN §4.1)."""

    def __init__(self, message: str, detail: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or {}


def interpolate(obj: Any, env: dict[str, str]) -> Any:
    """Resolve ${VAR}, ${VAR:-default}, ${VAR:-} in all string scalars.

    Bare ${VAR} missing -> ConfigError listing all unresolved vars.
    ${VAR:-default} missing -> the literal default. ${VAR:-} missing -> empty.
    """
    unresolved: list[str] = []
    resolved = _interpolate(obj, env, unresolved)
    if unresolved:
        raise ConfigError(
            "Unresolved environment variables",
            {"variables": sorted(set(unresolved))},
        )
    return resolved


def _interpolate(obj: Any, env: dict[str, str], unresolved: list[str]) -> Any:
    if isinstance(obj, str):
        return _interpolate_str(obj, env, unresolved)
    if isinstance(obj, dict):
        return {k: _interpolate(v, env, unresolved) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate(v, env, unresolved) for v in obj]
    return obj


def _interpolate_str(s: str, env: dict[str, str], unresolved: list[str]) -> str:
    def repl(match: re.Match[str]) -> str:
        var, has_default, default = match.group(1), match.group(2), match.group(3)
        if var in env:
            return env[var]
        if has_default is not None:
            return default  # "" for ${VAR:-}, the literal for ${VAR:-x}
        unresolved.append(var)
        return match.group(0)

    return _VAR_RE.sub(repl, s)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_interpolation.py -v`
Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
git add agctl/config/loader.py tests/unit/test_interpolation.py
git commit -m "feat: \${VAR}/\${VAR:-default} interpolation + ConfigError (D3)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 5: AGCTL_* env overrides

**Files:**
- Create: `agctl/config/resolver.py`
- Test: `tests/unit/test_resolver.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_resolver.py`:
```python
from agctl.config.resolver import apply_env_overrides


def test_sets_leaf_value():
    out = apply_env_overrides({"defaults": {"timeout_seconds": 10}}, {"AGCTL_DEFAULTS__TIMEOUT_SECONDS": "30"})
    assert out["defaults"]["timeout_seconds"] == "30"


def test_sets_nested_creating_intermediate_dicts():
    out = apply_env_overrides({}, {"AGCTL_KAFKA__DEFAULT_CONSUMER_GROUP": "ci"})
    assert out["kafka"]["default_consumer_group"] == "ci" == "ci"


def test_ignores_non_agctl_env():
    out = apply_env_overrides({"x": 1}, {"PATH": "/bin", "AGCTL_CONFIG": "/tmp/x"})
    assert out == {"x": 1}


def test_value_always_string():
    out = apply_env_overrides({}, {"AGCTL_DEFAULTS__TIMEOUT_SECONDS": "30"})
    assert out["defaults"]["timeout_seconds"] == "30"
    assert isinstance(out["defaults"]["timeout_seconds"], str)


def test_does_not_mutate_input():
    src = {"defaults": {"timeout_seconds": 10}}
    apply_env_overrides(src, {"AGCTL_DEFAULTS__TIMEOUT_SECONDS": "30"})
    assert src["defaults"]["timeout_seconds"] == 10
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_resolver.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'agctl.config.resolver'`).

- [ ] **Step 3: Write minimal implementation**

`agctl/config/resolver.py`:
```python
"""AGCTL_* environment-override layer (DESIGN §5, §8 — D4: __ nesting delimiter).

Convention: AGCTL_<SECTION>__<KEY> — double-underscore separates path segments;
a single underscore stays within a key segment. Overrides are write-oriented:
hyphenated YAML keys (e.g. order-service) are not reconstructed from the
underscored env name, so prefer overrides on hyphen-free keys.
"""

import copy
from typing import Any

_PREFIX = "AGCTL_"


def apply_env_overrides(data: dict[str, Any], env: dict[str, str]) -> dict[str, Any]:
    """Deep-merge AGCTL_* overrides into a copy of data. Highest precedence."""
    data = copy.deepcopy(data)
    for key, value in env.items():
        if not key.startswith(_PREFIX):
            continue
        path = _parse_path(key[len(_PREFIX):])
        if path is None:
            continue
        _deep_set(data, path, value)
    return data


def _parse_path(suffix: str) -> list[str] | None:
    segments = suffix.split("__")
    if not segments or any(seg == "" for seg in segments):
        return None  # malformed (e.g. trailing __); skip
    return [seg.lower() for seg in segments]


def _deep_set(data: dict[str, Any], path: list[str], value: str) -> None:
    cur = data
    for part in path[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[path[-1]] = value
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_resolver.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add agctl/config/resolver.py tests/unit/test_resolver.py
git commit -m "feat: AGCTL_* __ env-override resolver (D4)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 6: Config path discovery (resolution order)

**Files:**
- Modify: `agctl/config/loader.py` (add `discover_config_path`)
- Test: `tests/unit/test_discovery.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_discovery.py`:
```python
from pathlib import Path

import pytest

from agctl.config.loader import ConfigError, discover_config_path


def test_explicit_flag_wins(tmp_path):
    f = tmp_path / "agctl.yaml"
    f.write_text("version: '1'\n")
    assert discover_config_path(explicit=str(f)) == f


def test_explicit_missing_file_raises(tmp_path):
    with pytest.raises(ConfigError):
        discover_config_path(explicit=str(tmp_path / "nope.yaml"))


def test_agctl_config_env_used(tmp_path, monkeypatch):
    f = tmp_path / "cfg.yaml"
    f.write_text("version: '1'\n")
    monkeypatch.chdir(tmp_path)
    assert discover_config_path(env={"AGCTL_CONFIG": str(f)}) == f


def test_agctl_config_ignored_when_explicit(tmp_path, monkeypatch):
    explicit = tmp_path / "a.yaml"
    explicit.write_text("version: '1'\n")
    other = tmp_path / "b.yaml"
    other.write_text("version: '1'\n")
    assert discover_config_path(explicit=str(explicit), env={"AGCTL_CONFIG": str(other)}) == explicit


def test_walk_up_finds_agctl_yaml(tmp_path, monkeypatch):
    root = tmp_path / "proj"
    sub = root / "src" / "deep"
    sub.mkdir(parents=True)
    cfg = root / "agctl.yaml"
    cfg.write_text("version: '1'\n")
    monkeypatch.chdir(sub)
    assert discover_config_path(env={}) == cfg


def test_no_config_found_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ConfigError):
        discover_config_path(env={})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_discovery.py -v`
Expected: FAIL (`AttributeError: module ... has no attribute 'discover_config_path'`).

- [ ] **Step 3: Write minimal implementation**

Add to `agctl/config/loader.py` (append these imports at the top with the others, then the function):

```python
import os
import pathlib
```

Append the function:
```python
def discover_config_path(explicit: str | None = None, env: dict[str, str] | None = None) -> pathlib.Path:
    """Resolve the config path per DESIGN §5: --config > AGCTL_CONFIG > walk up."""
    env = env if env is not None else os.environ

    if explicit:
        path = pathlib.Path(explicit)
        if not path.is_file():
            raise ConfigError(f"Config file not found: {explicit}", {"path": explicit})
        return path

    if "AGCTL_CONFIG" in env:
        path = pathlib.Path(env["AGCTL_CONFIG"])
        if not path.is_file():
            raise ConfigError(f"Config file not found: {env['AGCTL_CONFIG']}", {"path": str(path)})
        return path

    cwd = pathlib.Path.cwd()
    for directory in [cwd, *cwd.parents]:
        candidate = directory / "agctl.yaml"
        if candidate.is_file():
            return candidate
        if (directory / ".git").exists():
            break

    raise ConfigError("No agctl.yaml found (use --config or AGCTL_CONFIG, or add agctl.yaml)", {})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_discovery.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add agctl/config/loader.py tests/unit/test_discovery.py
git commit -m "feat: config path discovery with §5 resolution order

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 7: load_config pipeline + version check (+ D1 regression fixture)

**Files:**
- Modify: `agctl/config/loader.py` (add `TOOL_MAJOR_VERSION`, `_check_version`, `load_config`)
- Modify: `agctl/config/__init__.py` (re-exports)
- Create: `tests/fixtures/agctl.yaml`, `tests/unit/test_loader.py`

- [ ] **Step 1: Write the fixture**

`tests/fixtures/agctl.yaml` (a full valid config — the D1 regression fixture):
```yaml
version: "1"
services:
  order-service:
    base_url: "${ORDER_SERVICE_URL}"
    health_path: "/actuator/health"
kafka:
  brokers:
    - "${KAFKA_BROKER}:9092"
  default_consumer_group: "agctl-consumer"
  schema_registry_url: "${SCHEMA_REGISTRY_URL:-}"
  patterns:
    order-created:
      topic: orders.created
      match: '.eventType == "ORDER_CREATED"'
database:
  connections:
    main-db:
      type: postgresql
      host: "${DB_HOST}"
      port: 5432
      dbname: "${DB_NAME}"
      user: "${DB_USER}"
      password: "${DB_PASSWORD}"
      default: true
  templates:
    find-order:
      connection: main-db
      sql: "SELECT id, status FROM orders WHERE id = :orderId"
templates:
  create-order:
    method: POST
    service: order-service
    path: "/api/v1/orders"
    body:
      customer_id: "{customer_id}"
defaults:
  timeout_seconds: 10
  database_connection: main-db
```

- [ ] **Step 2: Write the failing test**

`tests/unit/test_loader.py`:
```python
import os
from pathlib import Path

import pytest

from agctl.config import ConfigError, load_config

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"


def _env(**extra):
    env = {
        "ORDER_SERVICE_URL": "http://localhost:8081",
        "KAFKA_BROKER": "localhost",
        "DB_HOST": "h",
        "DB_NAME": "n",
        "DB_USER": "u",
        "DB_PASSWORD": "p",
    }
    env.update(extra)
    return env


def test_load_full_config_keeps_connections_and_templates():
    """§5 verification test / D1 regression: both sections survive loading."""
    cfg = load_config(str(FIXTURE), env=_env())
    assert cfg.database.connections["main-db"].host == "h"
    assert cfg.database.templates["find-order"].connection == "main-db"
    assert cfg.services["order-service"].base_url == "http://localhost:8081"
    assert cfg.kafka.schema_registry_url == ""  # ${VAR:-} resolved to empty


def test_missing_required_env_raises(tmp_path):
    with pytest.raises(ConfigError) as exc:
        load_config(str(FIXTURE), env={})  # ORDER_SERVICE_URL etc. missing
    assert "DB_HOST" in exc.value.detail["variables"]


def test_agctl_override_applied(tmp_path):
    cfg = load_config(str(FIXTURE), env=_env(AGCTL_DEFAULTS__TIMEOUT_SECONDS="99"))
    assert cfg.defaults.timeout_seconds == 99


def test_version_mismatch_raises(tmp_path):
    bad = tmp_path / "agctl.yaml"
    bad.write_text('version: "2"\n')
    with pytest.raises(ConfigError) as exc:
        load_config(str(bad), env={})
    assert exc.value.detail["tool_major"] == "1"


def test_invalid_schema_raises(tmp_path):
    bad = tmp_path / "agctl.yaml"
    bad.write_text('version: "1"\ntemplates:\n  x:\n    method: GET\n')  # missing service/path
    with pytest.raises(ConfigError):
        load_config(str(bad), env={})
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/unit/test_loader.py -v`
Expected: FAIL (`ImportError: cannot import name 'load_config'`).

- [ ] **Step 4: Write minimal implementation**

Add to `agctl/config/loader.py` (top imports):
```python
import yaml
from pydantic import ValidationError

from .models import Config
from .resolver import apply_env_overrides
```

Append:
```python
TOOL_MAJOR_VERSION = "1"


def load_config(path: str | None = None, env: dict[str, str] | None = None):
    """Full pipeline: discover -> parse -> interpolate -> override -> validate."""
    env = env if env is not None else os.environ
    config_path = discover_config_path(explicit=path, env=env)
    raw = yaml.safe_load(config_path.read_text()) or {}
    interpolated = interpolate(raw, env)
    with_overrides = apply_env_overrides(interpolated, env)
    _check_version(with_overrides)
    try:
        return Config.model_validate(with_overrides)
    except ValidationError as exc:
        raise ConfigError("Invalid configuration", {"validation_errors": exc.errors()}) from exc


def _check_version(data: dict) -> None:
    version = str(data.get("version", "")).strip()
    major = version.split(".")[0] if version else ""
    if major != TOOL_MAJOR_VERSION:
        raise ConfigError(
            f"Version mismatch: config major '{major}' != tool major '{TOOL_MAJOR_VERSION}'",
            {"config_version": version, "tool_major": TOOL_MAJOR_VERSION},
        )
```

`agctl/config/__init__.py`:
```python
"""Configuration loading pipeline."""

from .loader import ConfigError, load_config

__all__ = ["ConfigError", "load_config"]
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/unit/test_loader.py -v`
Expected: 5 passed.

- [ ] **Step 6: Run the whole suite**

Run: `pytest -v`
Expected: all tests pass (smoke, output, models, interpolation, resolver, discovery, loader).

- [ ] **Step 7: Commit**

```bash
git add agctl/config/loader.py agctl/config/__init__.py tests/fixtures/agctl.yaml tests/unit/test_loader.py
git commit -m "feat: load_config pipeline + version check + D1 regression fixture (M4, §5)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 8: Click CLI skeleton + `config validate` / `config show`

**Files:**
- Create: `agctl/cli.py`
- Test: `tests/unit/test_cli.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_cli.py`:
```python
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from agctl.cli import cli

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"

ENV = {
    "ORDER_SERVICE_URL": "http://localhost:8081",
    "KAFKA_BROKER": "localhost",
    "DB_HOST": "h",
    "DB_NAME": "n",
    "DB_USER": "u",
    "DB_PASSWORD": "secret",
}


def test_validate_ok():
    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(FIXTURE)], env=ENV)
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["ok"] is True
    assert payload["command"] == "config.validate"
    assert payload["result"] == {"valid": True}


def test_validate_fails_on_missing_env():
    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(FIXTURE)], env={})
    payload = json.loads(result.output)
    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


def test_show_masks_password():
    result = CliRunner().invoke(cli, ["config", "show", "--config", str(FIXTURE)], env=ENV)
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["result"]["database"]["connections"]["main-db"]["password"] == "***"


def test_show_unmask_exposes_password():
    args = ["config", "show", "--config", str(FIXTURE), "--unmask"]
    result = CliRunner().invoke(cli, args, env=ENV)
    payload = json.loads(result.output)
    assert payload["result"]["database"]["connections"]["main-db"]["password"] == "secret"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_cli.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'agctl.cli'`).

- [ ] **Step 3: Write minimal implementation**

`agctl/cli.py`:
```python
"""Click entry point (DESIGN §3, §7). Wires command groups and emits envelopes."""

import time
from typing import Any

import click

from .config import ConfigError, load_config
from .output import emit

_SECRET_FRAGMENTS = ("password", "token", "secret", "key")


def _ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


def _mask(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: ("***" if _is_secret(k) and v else _mask(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_mask(v) for v in obj]
    return obj


def _is_secret(key: str) -> bool:
    lowered = key.lower()
    return any(frag in lowered for frag in _SECRET_FRAGMENTS)


def _emit_config_error(command: str, err: ConfigError, start: float) -> None:
    errors = [{"message": err.message, **(err.detail or {})}]
    emit(
        ok=False,
        command=command,
        result={"valid": False, "errors": errors},
        error={"type": "ConfigError", "message": err.message, "detail": err.detail},
        duration_ms=_ms(start),
    )


@click.group()
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.pass_context
def cli(ctx: click.Context, config_path: str | None) -> None:
    """agctl — agent-facing CLI harness for testing distributed systems."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


@cli.group(name="config")
def config_group() -> None:
    """Config introspection."""


@config_group.command("validate")
@click.option("--config", "config_path", default=None)
def config_validate(config_path: str | None) -> None:
    """Parse and validate agctl.yaml (DESIGN §3.5). Exit 2 on any error."""
    start = time.monotonic()
    try:
        load_config(config_path)
    except ConfigError as err:
        _emit_config_error("config.validate", err, start)
        raise SystemExit(2)
    emit(ok=True, command="config.validate", result={"valid": True}, duration_ms=_ms(start))


@config_group.command("show")
@click.option("--config", "config_path", default=None)
@click.option("--unmask", is_flag=True, default=False)
def config_show(config_path: str | None, unmask: bool) -> None:
    """Dump the resolved config as JSON, secrets masked (DESIGN §3.5)."""
    start = time.monotonic()
    try:
        cfg = load_config(config_path)
    except ConfigError as err:
        _emit_config_error("config.show", err, start)
        raise SystemExit(2)
    data = cfg.model_dump()
    if not unmask:
        data = _mask(data)
    emit(ok=True, command="config.show", result=data, duration_ms=_ms(start))


if __name__ == "__main__":
    cli()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_cli.py -v`
Expected: 4 passed.

- [ ] **Step 5: Run full suite and verify the installed CLI**

Run: `pytest -v`
Expected: all foundation tests pass.

Run: `agctl config validate --config tests/fixtures/agctl.yaml` with the `ENV` variables exported (or skip if env unset — it will report unresolved vars and exit 2, which is correct behavior).
Expected (with env set): `{"ok": true, "command": "config.validate", "result": {"valid": true}, "error": null, "duration_ms": <n>}` and exit 0.

- [ ] **Step 6: Commit**

```bash
git add agctl/cli.py tests/unit/test_cli.py
git commit -m "feat: Click CLI skeleton with config validate/show (DESIGN §3.5)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Self-review (run during authoring)

**1. Spec coverage** (foundation slice only — out-of-scope items intentionally absent):
- §2 schema → Task 3 (models). §2.2 interpolation → Task 4. §3.5 validate/show → Task 8. §4.1 envelope → Task 2. §5 resolution order → Task 6; overrides → Task 5; full pipeline → Task 7. §7 structure → Tasks 1–8. D1 regression → Tasks 3 & 7. D3 → Task 4. D4 → Task 5. M4 version → Task 7.
- §5 "verification: load full agctl.yaml, assert connections+templates present" → Task 7 `test_load_full_config_keeps_connections_and_templates` (fixture in Task 7 Step 1).
- Gaps: none within the foundation slice. (HTTP/Kafka/DB/check/discover/plugins deliberately deferred.)

**2. Placeholder scan:** none. Every step shows exact code; every test has real assertions; commit messages are concrete.

**3. Type/name consistency:** `emit(ok, command, result=None, error=None, duration_ms=0)` (Task 2) matches every call in Task 8. `ConfigError(message, detail)` (Task 4) with `.message`/`.detail` matches usage in Tasks 6/7/8. `load_config(path, env)` (Task 7) matches Task 8 calls and `__init__.py` re-export. `apply_env_overrides(data, env)` (Task 5) matches the Task 7 call. `discover_config_path(explicit, env)` (Task 6) matches the Task 7 call. Model field names (`database.connections`, `database.templates`, `kafka.patterns`, `templates`, `defaults.timeout_seconds`) match between Task 3 models and all tests.
