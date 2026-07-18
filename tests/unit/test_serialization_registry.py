"""Unit tests for the lazy Confluent Schema Registry client wrapper.

The wrapper (``agctl/serialization/registry.py``) has two surfaces:

* ``build_schema_registry_conf`` — a *pure* translation from the typed
  :class:`SchemaRegistryConfig` to the dotted-key dict that
  ``confluent_kafka.schema_registry.SchemaRegistryClient`` expects.
* :class:`SchemaRegistryClient` — a thin cached wrapper around the real
  client. Its constructor takes a ``client_factory`` test seam so these
  tests run WITHOUT ``confluent_kafka`` importable; the one real-path
  test uses ``pytest.importorskip``.

Cases (a)-(g) mirror the task brief verbatim.
"""

import sys

import pytest

from agctl.config.models import BasicAuth, KafkaSSL, SchemaRegistryConfig
from agctl.errors import ConfigError, ConnectionFailure
from agctl.serialization.registry import (
    SchemaRegistryClient,
    build_schema_registry_conf,
)


# --- Fake SR client (the test seam target) ---------------------------------


class _FakeSchema:
    """Stand-in for ``confluent_kafka.schema_registry.Schema``."""

    def __init__(self, schema_type, schema_str):
        self.schema_type = schema_type
        self.schema_str = schema_str


class _FakeSRClient:
    """Records every call so cache-hit tests can assert non-recall.

    Method names/shapes mirror the *modern* confluent_kafka API
    (``get_schema(schema_id)``, ``register_schema(subject, schema)``,
    ``get_subjects()``) which is what the wrapper calls internally.
    """

    def __init__(self, conf):
        self.conf = conf
        self.get_schema_calls = 0
        self.register_schema_calls = 0
        self.subjects_calls = 0
        self.raise_on_subjects = False
        # Return values a test can override after construction.
        self._schema_for_id = {
            7: _FakeSchema("AVRO", '{"type":"record","name":"X"}'),
        }
        self._id_for_subject = 42

    def get_schema(self, schema_id):
        self.get_schema_calls += 1
        if schema_id not in self._schema_for_id:
            raise KeyError(schema_id)
        return self._schema_for_id[schema_id]

    def register_schema(self, subject, schema):
        self.register_schema_calls += 1
        # Capture what the wrapper passed so tests can assert shape.
        self.last_register = (subject, schema)
        return self._id_for_subject

    def get_subjects(self):
        self.subjects_calls += 1
        if self.raise_on_subjects:
            raise RuntimeError("registry unreachable")
        return ["t-value"]


def _factory(fake):
    """Build a ``client_factory`` closure that injects ``fake`` and captures conf."""

    captured = {}

    def factory(conf):
        captured["conf"] = conf
        fake.conf = conf
        return fake

    return factory, captured


# --- (a) plaintext: no auth, no TLS ----------------------------------------


def test_build_conf_plaintext_only_url():
    # (a) ``build_schema_registry_conf("http://sr:8081", None)`` == {"url": ...}
    assert build_schema_registry_conf("http://sr:8081", None) == {
        "url": "http://sr:8081"
    }


def test_build_conf_empty_config_object_also_just_url():
    # An empty SchemaRegistryConfig still yields only the url key.
    conf = build_schema_registry_conf("http://sr:8081", SchemaRegistryConfig())
    assert conf == {"url": "http://sr:8081"}


# --- (b) basic auth ---------------------------------------------------------


def test_build_conf_basic_auth_emits_user_info():
    # (b) auth=basic, basic_auth={username:"u",password:"p"} -> "u:p"
    sr = SchemaRegistryConfig(
        auth="basic",
        basic_auth=BasicAuth(username="u", password="p"),
    )
    conf = build_schema_registry_conf("http://sr:8081", sr)
    assert conf["url"] == "http://sr:8081"
    assert conf["basic.auth.user.info"] == "u:p"


# --- (c) mTLS / SSL ---------------------------------------------------------


def test_build_conf_ssl_populates_present_keys_only():
    # (c) ca_location, certificate_location, key_location set; key_password
    # unset -> the three ssl.* keys appear, ssl.key.password does NOT.
    sr = SchemaRegistryConfig(
        auth="mtls",
        ssl=KafkaSSL(
            ca_location="/etc/ssl/ca.pem",
            certificate_location="/etc/ssl/cert.pem",
            key_location="/etc/ssl/key.pem",
        ),
    )
    conf = build_schema_registry_conf("https://sr:8081", sr)
    assert conf["url"] == "https://sr:8081"
    assert conf["ssl.ca.location"] == "/etc/ssl/ca.pem"
    assert conf["ssl.certificate.location"] == "/etc/ssl/cert.pem"
    assert conf["ssl.key.location"] == "/etc/ssl/key.pem"
    # Unset fields must be omitted, not present-as-None/empty.
    assert "ssl.key.password" not in conf


def test_build_conf_ssl_password_emitted_when_set():
    # key_password set -> the ssl.key.password key appears.
    sr = SchemaRegistryConfig(
        ssl=KafkaSSL(
            ca_location="/etc/ssl/ca.pem",
            key_password="secret",
        ),
    )
    conf = build_schema_registry_conf("https://sr:8081", sr)
    assert conf["ssl.ca.location"] == "/etc/ssl/ca.pem"
    assert conf["ssl.key.password"] == "secret"
    # The two unset ssl.* keys are still omitted.
    assert "ssl.certificate.location" not in conf
    assert "ssl.key.location" not in conf


def test_build_conf_auth_field_is_not_a_conf_key():
    # The ``auth`` field is a validator hint only; it must never leak into
    # the confluent conf dict under any key.
    sr = SchemaRegistryConfig(
        auth="basic", basic_auth=BasicAuth(username="u", password="p")
    )
    conf = build_schema_registry_conf("http://sr:8081", sr)
    assert "auth" not in conf
    assert "basic.auth" not in conf


# --- (d) get_schema cached --------------------------------------------------


def test_get_schema_returns_value_and_caches():
    # (d) get_schema(7) returns the fake's value AND a second get_schema(7)
    # does not re-call the fake.
    fake = _FakeSRClient({"url": "http://sr:8081"})
    factory, captured = _factory(fake)
    client = SchemaRegistryClient(
        "http://sr:8081", client_factory=factory
    )

    first = client.get_schema(7)
    assert first == ("AVRO", '{"type":"record","name":"X"}')
    assert fake.get_schema_calls == 1

    second = client.get_schema(7)
    assert second == first
    # Cache hit: the underlying fake was NOT called again.
    assert fake.get_schema_calls == 1


def test_get_schema_distinct_ids_each_hit_once():
    fake = _FakeSRClient({"url": "http://sr:8081"})
    fake._schema_for_id[1] = _FakeSchema("PROTOBUF", "syntax=\"proto3\";")
    fake._schema_for_id[2] = _FakeSchema("JSON", "{}")
    client = SchemaRegistryClient("http://sr:8081", client_factory=_factory(fake)[0])

    client.get_schema(1)
    client.get_schema(2)
    client.get_schema(1)  # cached
    client.get_schema(2)  # cached

    assert fake.get_schema_calls == 2


# --- (e) register_schema cached --------------------------------------------


def test_register_schema_returns_id_and_caches():
    # (e) register_schema("t-value", "...", "AVRO") returns the fake's id and
    # is cached: a second identical call does not re-call the fake.
    fake = _FakeSRClient({"url": "http://sr:8081"})
    client = SchemaRegistryClient("http://sr:8081", client_factory=_factory(fake)[0])

    schema_str = '{"type":"record","name":"X"}'
    first = client.register_schema("t-value", schema_str, "AVRO")
    assert first == 42
    assert fake.register_schema_calls == 1

    second = client.register_schema("t-value", schema_str, "AVRO")
    assert second == 42
    # Cache hit on (subject, schema_str).
    assert fake.register_schema_calls == 1


def test_register_schema_cache_keyed_on_subject_and_schema_str():
    # Same schema_str under a different subject, or different schema_str
    # under the same subject, must each miss the cache and hit the fake.
    fake = _FakeSRClient({"url": "http://sr:8081"})
    client = SchemaRegistryClient("http://sr:8081", client_factory=_factory(fake)[0])

    client.register_schema("t-value", "{}", "AVRO")
    client.register_schema("t-key", "{}", "AVRO")  # different subject
    client.register_schema("t-value", '{"x":1}', "AVRO")  # different schema_str
    client.register_schema("t-value", "{}", "AVRO")  # cached

    assert fake.register_schema_calls == 3


# --- (f) check_reachable ----------------------------------------------------


def test_check_reachable_returns_none_on_success():
    # (f) success path: returns None when the fake's get_subjects() works.
    fake = _FakeSRClient({"url": "http://sr:8081"})
    client = SchemaRegistryClient("http://sr:8081", client_factory=_factory(fake)[0])

    assert client.check_reachable() is None
    assert fake.subjects_calls == 1


def test_check_reachable_raises_connection_failure_on_error():
    # (f) failure path: any error from the underlying client surfaces as
    # ConnectionFailure naming the URL.
    fake = _FakeSRClient({"url": "http://sr:8081"})
    fake.raise_on_subjects = True
    client = SchemaRegistryClient("http://sr:8081", client_factory=_factory(fake)[0])

    with pytest.raises(ConnectionFailure) as exc_info:
        client.check_reachable()
    # The URL must appear in the message so operators can locate the registry.
    assert "http://sr:8081" in exc_info.value.message


def test_get_schema_maps_underlying_error_to_connection_failure():
    # Non-check_reachable methods must also translate SR errors.
    fake = _FakeSRClient({"url": "http://sr:8081"})
    fake._schema_for_id.clear()  # any get_schema() will raise KeyError
    client = SchemaRegistryClient("http://sr:8081", client_factory=_factory(fake)[0])

    with pytest.raises(ConnectionFailure) as exc_info:
        client.get_schema(99)
    assert "http://sr:8081" in exc_info.value.message


# --- missing kafka extra -> ConfigError -------------------------------------


def test_missing_kafka_extra_raises_config_error(monkeypatch):
    """A missing ``kafka`` extra must surface as ConfigError, never bare ImportError.

    ``sys.modules['confluent_kafka.schema_registry'] = None`` makes Python
    raise ImportError on the lazy ``from confluent_kafka.schema_registry import ...``
    inside ``__init__`` (standard "halted; None in sys.modules" path). The
    wrapper must translate that into a ConfigError pointing at ``agctl[kafka]``.
    """
    monkeypatch.setitem(sys.modules, "confluent_kafka.schema_registry", None)

    with pytest.raises(ConfigError) as exc_info:
        SchemaRegistryClient("http://sr:8081")  # no client_factory -> real path
    assert "agctl[kafka]" in str(exc_info.value)


def test_missing_transitive_dep_surfaces_in_config_error_message(monkeypatch):
    """A missing *transitive* dependency (e.g. ``authlib``, pulled by
    ``confluent_kafka.schema_registry`` itself) must surface in the ConfigError
    message — not just the generic ``agctl[kafka]`` install hint.

    The ``kafka`` extra installs ``confluent-kafka`` but does NOT pin
    ``authlib``; when the SR submodule's own ``import authlib`` fails, the
    resulting ``ModuleNotFoundError`` is an ``ImportError`` subclass, so the
    wrapper's ``except ImportError`` catches it. Without echoing the underlying
    error text, an operator sees only "install agctl[kafka]" — wrong remedy,
    since the kafka extra IS already installed. This test locks the cause into
    the surfaced message so a future refactor that swallows it again fails here.
    """
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "confluent_kafka.schema_registry":
            raise ModuleNotFoundError("No module named 'authlib'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    # Ensure the lazy import actually re-enters our patched __import__ rather
    # than short-circuiting on a previously-cached module entry.
    monkeypatch.delitem(sys.modules, "confluent_kafka.schema_registry", raising=False)

    with pytest.raises(ConfigError) as exc_info:
        SchemaRegistryClient("http://sr:8081")  # no client_factory -> real path

    message = str(exc_info.value)
    # The missing transitive dependency MUST be named in the surfaced message
    # — this is the assertion that locks the behavior and would fail if a
    # future refactor swallowed the cause again.
    assert "authlib" in message
    # The spec-mandated install pointer is retained as a recovery hint.
    assert "agctl[kafka]" in message


# --- (g) real-path construction --------------------------------------------


def test_real_schema_registry_client_constructs_from_conf():
    # (g) Real path: when confluent_kafka is importable, constructing a
    # SchemaRegistryClient from a conf dict must not raise. No network call
    # is made by construction; this only exercises the lazy import + the
    # real constructor's conf handling.
    #
    # Two skips: the brief names ``confluent_kafka``; we ALSO skip on the
    # ``schema_registry`` submodule because that is what the wrapper actually
    # lazy-imports (and in some envs the top-level wheel imports but its SR
    # submodule pulls an uninstalled ``authlib``).
    pytest.importorskip("confluent_kafka")
    pytest.importorskip("confluent_kafka.schema_registry")

    # Construction must not raise and must not perform any I/O.
    client = SchemaRegistryClient("http://localhost:8081")
    assert client is not None


# --- module purity: lazy import discipline ----------------------------------


def test_registry_module_top_level_has_no_confluent_import():
    """The wrapper module must stay import-light at module top.

    ``confluent_kafka`` is lazy-imported INSIDE ``__init__`` so this module
    imports cleanly even without the ``kafka`` extra. A structural guard:
    importing the module must not pull ``confluent_kafka`` into its globals.
    """
    import agctl.serialization.registry as mod  # noqa: F401

    assert "confluent_kafka" not in dir(mod)
    assert "SchemaRegistryClient" in dir(mod)  # our class, not theirs
    assert callable(build_schema_registry_conf)
