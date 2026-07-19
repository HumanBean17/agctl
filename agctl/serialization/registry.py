"""Lazy Confluent Schema Registry client wrapper (DESIGN §6.1).

The real client (``confluent_kafka.schema_registry.SchemaRegistryClient``) is
shipped with the optional ``kafka`` extra and is lazy-imported INSIDE
:meth:`SchemaRegistryClient.__init__`. Module top imports only stdlib plus
the local config models and the error hierarchy, so this module imports
cleanly even when the ``kafka`` extra is absent — only *constructing* a
client requires the extra. A missing extra (or a missing transitive dep the
SR submodule itself imports, e.g. ``authlib``) surfaces as a
:class:`ConfigError` whose message echoes the underlying import error text
AND points at ``pip install 'agctl[kafka]'``, never a bare ``ImportError``.

This wrapper adds two things the raw client does not:

* a typed ``client_factory`` test seam, so unit tests inject a fake SR
  client and run without ``confluent_kafka`` installed; and
* per-instance caches keyed by schema id and by ``(subject, schema_str)``
  so repeated decode/encode of the same id or subject does not round-trip
  the registry.

It is the I/O boundary for the Avro / Protobuf codecs (Tasks 6/13) and for
the startup reachability probe (Task 9).
"""

from __future__ import annotations

from typing import Callable

from ..config.models import SchemaRegistryConfig
from ..errors import ConfigError, ConnectionFailure


def build_schema_registry_conf(
    url: str, sr: SchemaRegistryConfig | None
) -> dict:
    """Translate :class:`SchemaRegistryConfig` into the confluent SR conf dict.

    Pure: no imports, no I/O. Always includes ``"url"``; adds auth/TLS keys
    only when the corresponding sub-fields are populated. The ``auth`` field
    itself is a validator hint and is intentionally NOT mirrored here.

    Args:
        url: The Schema Registry base URL (e.g. ``"http://sr:8081"``).
        sr: The typed SR auth/TLS config, or ``None`` for plaintext.

    Returns:
        A dict of dotted-key -> value understood by
        ``confluent_kafka.schema_registry.SchemaRegistryClient``.
    """
    conf: dict = {"url": url}
    if sr is None:
        return conf

    if sr.basic_auth is not None:
        creds = sr.basic_auth
        # Emit only when there is something to format; a half-populated
        # block is a validator concern (Task 2), not a conf-translation
        # concern, but we still avoid emitting a literal "None:None" entry.
        if creds.username or creds.password:
            conf["basic.auth.user.info"] = f"{creds.username}:{creds.password}"

    if sr.ssl is not None:
        ssl = sr.ssl
        # Mirror only the non-empty librdkafka ``ssl.*`` keys. ``auth``'s
        # ``mtls`` mode maps to these same keys (no separate mTLS knob).
        for conf_key, value in (
            ("ssl.ca.location", ssl.ca_location),
            ("ssl.certificate.location", ssl.certificate_location),
            ("ssl.key.location", ssl.key_location),
            ("ssl.key.password", ssl.key_password),
        ):
            if value:  # non-empty string
                conf[conf_key] = value

    return conf


class SchemaRegistryClient:
    """Cached, error-mapped wrapper around the confluent SR client.

    Construction builds the conf via :func:`build_schema_registry_conf` and
    obtains the underlying client either from the ``client_factory`` seam or
    by lazy-importing ``confluent_kafka.schema_registry``. The three I/O
    methods (:meth:`get_schema`, :meth:`register_schema`,
    :meth:`check_reachable`) cache by id / ``(subject, schema_str)`` and map
    any underlying exception to :class:`ConnectionFailure` naming the URL.

    The ``client_factory`` parameter is the test seam: when provided, it is
    called with the built conf and its return value is used as the
    underlying client, so unit tests run WITHOUT ``confluent_kafka``
    installed.
    """

    def __init__(
        self,
        url: str,
        sr_config: SchemaRegistryConfig | None = None,
        *,
        client_factory: Callable[[dict], object] | None = None,
    ) -> None:
        self._url = url
        self._conf = build_schema_registry_conf(url, sr_config)
        # Per-instance caches (DESIGN §6.1): decode hot path hits the
        # by-id cache; encode hot path hits the by-subject cache; the
        # latest-version cache backs encode_payload (Task 7).
        self._by_id: dict[int, tuple[str, str]] = {}
        self._by_subject: dict[tuple[str, str], int] = {}
        self._by_latest: dict[str, tuple[str, str, int]] = {}

        if client_factory is not None:
            self._client = client_factory(self._conf)
            return

        # Lazy import the heavy module INSIDE __init__: a missing ``kafka``
        # extra must surface as ConfigError, never a bare ImportError. We
        # also surface the underlying error text in the message because the
        # SR submodule pulls transitive deps (e.g. ``authlib``) that the
        # ``kafka`` extra does not pin; in that case the generic install
        # hint is misleading (the extra IS installed) and the operator
        # needs the missing-module name to act.
        try:
            from confluent_kafka.schema_registry import (
                Schema as _Schema,
                SchemaRegistryClient as _ConfluentSRClient,
            )
        except ImportError as exc:  # pragma: no cover - env-dependent
            raise ConfigError(
                "Schema Registry support could not be loaded: "
                f"{exc}. Install the 'kafka' extra (pip install 'agctl[kafka]'); "
                "if it is already installed, the error above names the missing "
                "dependency."
            ) from exc

        self._Schema = _Schema
        self._client = _ConfluentSRClient(self._conf)

    # -- decode path ---------------------------------------------------------

    def get_schema(self, schema_id: int) -> tuple[str, str]:
        """Return ``(schema_type, schema_str)`` for ``schema_id``, cached.

        ``schema_type`` is one of ``"AVRO"`` / ``"PROTOBUF"`` / ``"JSON"`` and
        ``schema_str`` is the schema source. Cache misses call the underlying
        client; hits return the cached tuple without a round-trip. Any
        error from the registry (HTTP 4xx/5xx, auth, connectivity) surfaces
        as :class:`ConnectionFailure` naming the URL.
        """
        cached = self._by_id.get(schema_id)
        if cached is not None:
            return cached

        try:
            raw = self._client.get_schema(schema_id)
        except Exception as exc:  # noqa: BLE001 - I/O boundary: map all to ConnectionFailure
            raise ConnectionFailure(
                message=(
                    f"Schema Registry get_schema({schema_id}) failed at "
                    f"{self._url}: {exc}"
                )
            ) from exc

        result = (raw.schema_type, raw.schema_str)
        self._by_id[schema_id] = result
        return result

    # -- encode path ---------------------------------------------------------

    def register_schema(
        self, subject: str, schema_str: str, schema_type: str
    ) -> int:
        """Register ``schema_str`` under ``subject`` and return its id, cached.

        Cached by ``(subject, schema_str)`` so re-registering an identical
        schema is a no-op. Any error from the registry surfaces as
        :class:`ConnectionFailure` naming the URL.
        """
        key = (subject, schema_str)
        cached = self._by_subject.get(key)
        if cached is not None:
            return cached

        schema = self._build_schema(schema_str, schema_type)
        try:
            schema_id = self._client.register_schema(subject, schema)
        except Exception as exc:  # noqa: BLE001 - I/O boundary
            raise ConnectionFailure(
                message=(
                    f"Schema Registry register_schema(subject={subject!r}) "
                    f"failed at {self._url}: {exc}"
                )
            ) from exc

        self._by_subject[key] = schema_id
        return schema_id

    def get_latest_schema(self, subject: str) -> tuple[str, str, int]:
        """Return ``(schema_type, schema_str, schema_id)`` for ``subject``'s latest version.

        The encode hot path: ``encode_payload`` calls this to resolve the
        schema to encode against (the value alone carries no Avro schema).
        Cached by subject so repeated encode of the same subject's latest
        version does not round-trip the registry. v1 contract: no
        auto-registration — if the subject does not exist, the underlying
        call raises (``SchemaRegistryError`` 40404) and this surfaces as
        :class:`ConfigError` with a clear "register it before producing"
        message; any other failure (HTTP 5xx, auth, connectivity) surfaces
        as :class:`ConnectionFailure` naming the URL, matching the other
        I/O methods.

        The underlying ``get_latest_version(subject)`` returns a
        ``RegisteredSchema`` (NOT a tuple) exposing ``.schema`` (a
        ``Schema`` with ``.schema_type`` / ``.schema_str``) and
        ``.schema_id``. We unpack via attributes — the historical
        4-tuple unpack broke against the real non-iterable return.
        """
        cached = self._by_latest.get(subject)
        if cached is not None:
            return cached

        try:
            registered = self._client.get_latest_version(subject)
            schema = registered.schema
            schema_id = registered.schema_id
        except Exception as exc:  # noqa: BLE001 - I/O boundary
            # 40404 SUBJECT_NOT_FOUND is a config/contract bug (the subject
            # has no schema yet), NOT a connectivity problem. Detect by
            # duck-typing on ``error_code`` to avoid importing the heavy
            # ``confluent_kafka.schema_registry.error`` module here.
            if getattr(exc, "error_code", None) == 40404:
                raise ConfigError(
                    f"subject '{subject}' has no registered schema; "
                    "register it before producing",
                    {"subject": subject},
                ) from exc
            raise ConnectionFailure(
                message=(
                    f"Schema Registry get_latest_version(subject={subject!r}) "
                    f"failed at {self._url}: {exc}"
                )
            ) from exc

        result = (schema.schema_type, schema.schema_str, schema_id)
        self._by_latest[subject] = result
        return result

    # -- startup probe (Task 9) ---------------------------------------------

    def check_reachable(self) -> None:
        """Lightweight reachability probe; returns ``None`` on success.

        Issues a cheap call (``get_subjects``) against the registry; any
        HTTP/connectivity/auth error surfaces as :class:`ConnectionFailure`
        naming the URL. Used by the startup probe so a misconfigured SR is
        caught before the first message rather than mid-flow.
        """
        try:
            self._client.get_subjects()
        except Exception as exc:  # noqa: BLE001 - I/O boundary
            raise ConnectionFailure(
                message=(
                    f"Schema Registry unreachable at {self._url}: {exc}"
                )
            ) from exc

    # -- internal ------------------------------------------------------------

    def _build_schema(self, schema_str: str, schema_type: str):
        """Construct the underlying ``Schema`` regardless of import path.

        When the real client was lazy-imported, ``self._Schema`` is the
        confluent ``Schema`` class. Under the test seam the attribute is
        absent and ``schema_type``-tagged tuples/objects are typically not
        needed because tests assert on what the wrapper passed to the fake;
        we still build a minimal stand-in so the production code path is
        exercised by the seam too.
        """
        schema_cls = getattr(self, "_Schema", None)
        if schema_cls is not None:
            return schema_cls(schema_str, schema_type)
        # Test-seam fallback: a plain tuple the fake can inspect.
        return (schema_str, schema_type)
