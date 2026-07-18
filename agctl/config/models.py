"""Pydantic v2 schema models for agctl.yaml (DESIGN §2)."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..assertions import parse_grpc_status
from ..errors import ConfigError


def parse_listen(listen: str) -> tuple[str, int]:
    """Parse a listen address string into (host, port).

    Args:
        listen: Address string in format "host:port" or "[host]:port" for IPv6.

    Returns:
        Tuple of (host, port) where host is the string without brackets and port is an int.

    Raises:
        ValueError: If listen is empty, missing port, port is not a valid
            integer, port is out of range (0-65535), or an IPv6 address is not
            bracketed.
    """
    if not listen:
        raise ValueError("listen address cannot be empty")

    # Handle IPv6 bracketed addresses
    if listen.startswith("["):
        # IPv6 format: [::1]:18080
        if "]:" not in listen:
            raise ValueError(f"invalid listen address format: {listen!r}")
        host_part, port_part = listen.rsplit(":", 1)
        host = host_part[1:-1]  # Remove brackets
        if not host:
            raise ValueError(f"invalid listen address format: {listen!r}")
    else:
        # IPv4 or hostname: 0.0.0.0:18080
        if ":" not in listen:
            raise ValueError(f"missing port in listen address: {listen!r}")
        host, port_part = listen.rsplit(":", 1)
        if ":" in host:
            # An unbracketed host containing ':' is an IPv6 address — spec §7.2
            # requires bracketing (e.g. [::1]:18080). rsplit would otherwise
            # mis-split it ("::1:8080" → ("::1", 8080); "::1" → ("::", 1)).
            raise ValueError(
                f"IPv6 listen addresses must be bracketed, e.g. '[::1]:18080'; "
                f"got {listen!r}"
            )

    try:
        port = int(port_part)
    except ValueError:
        raise ValueError(f"port must be an integer, got {port_part!r}")

    # 0 is allowed (ephemeral bind — the engine reports the OS-assigned port in
    # the started line); reject out-of-range ports so a typo yields a clean
    # ConfigError(2) at parse time rather than an opaque OS bind error.
    if not (0 <= port <= 65535):
        raise ValueError(f"port out of range 0-65535: {port}")

    return host, port


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
    # Named cluster this pattern binds to (DESIGN §6, consumed in Tasks 2-3).
    # None -> resolved via default_cluster / single-cluster auto-default.
    cluster: str | None = None


class KafkaTopicConfig(BaseModel):
    """Per-topic serialization contract under ``kafka.topics.<name>`` (DESIGN §6.2).

    Any field left ``None`` falls back to the resolved cluster's default
    (:attr:`KafkaCluster.value_format` / :attr:`KafkaCluster.key_format`) and
    ultimately to today's ``json`` / ``string`` defaults. ``subject_strategy``
    governs only the *encode* subject; decode reads the embedded schema id and is
    strategy-independent. Cross-field semantics (cluster must exist,
    format-requires-SR, strategy-vs-format warnings) are enforced in
    ``config/validator.py`` (Task 2), not here.
    """

    cluster: str | None = None
    value_format: Literal["json", "avro", "protobuf"] | None = None
    key_format: Literal["string", "avro", "protobuf"] | None = None
    subject_strategy: Literal["topic", "record", "topic_record"] | None = None


class KafkaSSL(BaseModel):
    """TLS/mTLS settings for a Kafka connection (DESIGN §2.1).

    Fields map to librdkafka ``ssl.*`` / ``security.protocol`` keys. When any
    knob is set, ``security.protocol`` defaults to ``"SSL"`` unless overridden
    via :attr:`security_protocol`. Hostname verification stays on (librdkafka's
    secure default) unless :attr:`endpoint_identification_algorithm` is set to
    ``"none"`` (e.g. for self-signed/dev brokers).
    """

    ca_location: str | None = None
    certificate_location: str | None = None
    key_location: str | None = None
    key_password: str | None = None
    endpoint_identification_algorithm: str | None = None
    security_protocol: str | None = None

    @field_validator("security_protocol")
    @classmethod
    def _check_security_protocol(cls, v: str | None) -> str | None:
        # Fail fast at config load (DESIGN §3.5): an invalid protocol would
        # otherwise surface as an opaque broker-connect error. Kafka's
        # security.protocol is a fixed enum; normalize to the librdkafka form.
        if v is None:
            return v
        allowed = {"PLAINTEXT", "SSL", "SASL_SSL", "SASL_PLAINTEXT"}
        upper = v.upper()
        if upper not in allowed:
            raise ValueError(
                f"security_protocol must be one of {sorted(allowed)} (got {v!r})"
            )
        return upper


class BasicAuth(BaseModel):
    """Username/password credentials for HTTP Basic auth.

    For Confluent Cloud these are the API key / API secret pair.
    """

    username: str | None = None
    password: str | None = None


class SchemaRegistryConfig(BaseModel):
    """Schema Registry auth/TLS settings (DESIGN §6.1).

    The URL itself lives on the existing bare :attr:`KafkaCluster.schema_registry_url`
    field — this block holds only auth/TLS, so there is no nested ``url`` to alias
    or conflict with the bare field. ``auth`` is auto-inferred by the resolver
    when omitted (``basic_auth`` present -> ``basic``; ``ssl`` present ->
    ``mtls``; else ``plaintext``); the inference rule and auth-shape checks
    (basic-requires-basic_auth, mtls-requires-ssl) are enforced in
    ``config/validator.py`` (Task 2), not here — Pydantic ``Literal`` types
    reject only out-of-enum ``auth`` values at parse time.
    """

    auth: Literal["plaintext", "basic", "mtls"] | None = None
    basic_auth: BasicAuth | None = None
    ssl: KafkaSSL | None = None


class KafkaCluster(BaseModel):
    """A named Kafka cluster's broker configuration (DESIGN §6, v3 schema).

    Holds the per-cluster knobs formerly on ``KafkaConfig`` (brokers / TLS /
    timeout / consumer group / schema registry). Mirrors
    :class:`DatabaseConnection`: a named entry in ``kafka.clusters.<name>``,
    selected by name via ``resolve_cluster_name``. The
    :attr:`schema_registry` sub-block carries auth/TLS for the registry;
    :attr:`value_format` / :attr:`key_format` are cluster-level format defaults
    (overridable per topic via :attr:`KafkaTopicConfig`).
    """

    brokers: list[str] = Field(default_factory=list)
    ssl: KafkaSSL | None = None
    timeout_seconds: int | None = None
    default_consumer_group: str | None = None
    schema_registry_url: str | None = None
    schema_registry: SchemaRegistryConfig | None = None
    value_format: Literal["json", "avro", "protobuf"] = "json"
    key_format: Literal["string", "avro", "protobuf"] = "string"


class KafkaConfig(BaseModel):
    """Top-level Kafka config (v3 schema).

    ``clusters`` is a named map (mirroring ``database.connections``);
    ``default_cluster`` names the cluster used when no flag/binding selects one
    (required only when >1 cluster is defined, per single-cluster auto-default);
    ``patterns`` is a global cluster-aware map; ``topics`` is a per-topic
    serialization-contract map (DESIGN §6.2, consumed by the format resolver).
    """

    clusters: dict[str, KafkaCluster] = Field(default_factory=dict)
    default_cluster: str | None = None
    patterns: dict[str, KafkaPattern] = Field(default_factory=dict)
    topics: dict[str, KafkaTopicConfig] = Field(default_factory=dict)


class DatabaseConnection(BaseModel):
    type: str
    # Optional connection URI (e.g. "postgresql://user:pass@host:port/dbname").
    # When set, the driver passes it to psycopg as the conninfo string and still
    # forwards any discrete host/port/dbname/user/password fields — discrete
    # fields override URI params (DESIGN §3.3). Supports ${ENV} interpolation.
    # An empty/missing url falls back to discrete fields, so "${DB_URL:-}" lets
    # you default to discrete fields when the env var is unset.
    url: str | None = None
    host: str | None = None
    port: int | None = None
    dbname: str | None = None
    user: str | None = None
    password: str | None = None
    default: bool = False
    writable: bool = False


class DatabaseTemplate(BaseModel):
    description: str | None = None
    connection: str | None = None
    sql: str
    mode: Literal["read", "write"] = "read"


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


class HttpResponse(BaseModel):
    """HTTP response definition for mock stubs."""

    status: int = Field(default=200, ge=100, le=599)
    headers: dict[str, str] | None = None
    body: Any = None


class HttpMatch(BaseModel):
    """HTTP request matching criteria for mock stubs.

    Supports optional body subset matching (via `body`) and optional jq predicate
    matching (via `jq`). Both fields can coexist; a stub matches if all provided
    criteria pass.
    """

    body: dict | None = None
    jq: str | None = None


class CaptureSpec(BaseModel):
    """A single capture declaration for a mock stub or Kafka reactor.

    Reads a value off the *incoming* envelope (HTTP request or Kafka consumed
    message) at the jq path ``from`` and stores it under the capture key. ``type``
    controls how the resolver renders the captured value into a reaction
    (``"scalar"`` -> JSON scalar, ``"object"`` -> merged object, ``"json"`` ->
    raw JSON). This is the first aliased field in agctl's config schema: the YAML
    key is the Python keyword ``from``, so the attribute is ``from_`` and
    ``populate_by_name`` lets callers also construct via ``from_=...``.
    """

    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    type: Literal["scalar", "object", "json"] = "scalar"


class HttpStub(BaseModel):
    """HTTP mock stub definition."""

    description: str | None = None
    method: str
    path: str
    match: HttpMatch | None = None
    capture: dict[str, CaptureSpec] | None = None
    response: HttpResponse
    delay_ms: int = 0

    @field_validator("method")
    @classmethod
    def _normalize_method(cls, v: str) -> str:
        """Normalize HTTP method to uppercase."""
        return v.upper()


class HttpMockConfig(BaseModel):
    """HTTP mock server configuration."""

    listen: str = "0.0.0.0:18080"
    stubs: dict[str, HttpStub] = Field(default_factory=dict)

    @field_validator("listen")
    @classmethod
    def _validate_listen(cls, v: str) -> str:
        """Validate listen address format."""
        try:
            parse_listen(v)
        except ValueError as e:
            raise ValueError(f"invalid listen address: {e}") from e
        return v


class KafkaReaction(BaseModel):
    """Kafka reaction definition (produced message)."""

    topic: str
    key: str | None = None
    value: Any
    headers: dict[str, str] | None = None

    @field_validator("headers")
    @classmethod
    def _check_headers(cls, v: dict[str, str] | None) -> dict[str, str] | None:
        """Ensure all header values are strings."""
        if v is None:
            return v
        for key, val in v.items():
            if not isinstance(val, str):
                raise ValueError(
                    f"header value for {key!r} must be a string, got {type(val).__name__}"
                )
        return v


class KafkaReactor(BaseModel):
    """Kafka reactor definition (consumes and reacts)."""

    description: str | None = None
    topic: str
    consumer_group: str | None = None
    match: str | None = None
    capture: dict[str, CaptureSpec] | None = None
    reaction: KafkaReaction
    # Named cluster this reactor binds to (DESIGN §7, consumed in Task 3).
    # None -> resolved via default_cluster / single-cluster auto-default.
    cluster: str | None = None


class KafkaMockConfig(BaseModel):
    """Kafka mock reactor configuration."""

    reactors: dict[str, KafkaReactor] = Field(default_factory=dict)


class MocksConfig(BaseModel):
    """Mock server configuration (HTTP, Kafka, gRPC).

    ``grpc`` is defined after the gRPC mock models below (which depend on
    :class:`GrpcDescriptorSource`); see the gRPC mock section near the bottom of
    this module.
    """

    http: HttpMockConfig | None = None
    kafka: KafkaMockConfig | None = None
    grpc: "GrpcMockConfig | None" = None


class LogSource(BaseModel):
    """Log source configuration (file or journald)."""

    type: str = "file"
    path: str | None = None
    format: str = "logstash"
    service: str | None = None


class LogsDefaults(BaseModel):
    """Default parameters for logs commands."""

    tail_lines: int = 200
    limit: int = 50
    timeout_seconds: int = 10
    poll_interval_ms: int = 100


class LogsConfig(BaseModel):
    """Logs configuration: sources and defaults."""

    sources: dict[str, LogSource] = Field(default_factory=dict)
    defaults: LogsDefaults = Field(default_factory=LogsDefaults)


class GrpcTls(BaseModel):
    """TLS/mTLS settings for a gRPC connection."""

    ca_location: str | None = None
    certificate_location: str | None = None
    key_location: str | None = None
    override_authority: str | None = None


class GrpcTarget(BaseModel):
    """gRPC target configuration: address, TLS, and server reflection settings."""

    address: str
    use_tls: bool = False
    tls: GrpcTls | None = None
    reflection: Literal["auto", "on", "off"] = "auto"


class GrpcDescriptorSource(BaseModel):
    """Protobuf descriptor source: either proto file or descriptor set."""

    proto: str | None = None
    include_paths: list[str] = Field(default_factory=list)
    descriptor_set: str | None = None


class GrpcTemplate(BaseModel):
    """gRPC call template: target service, method, metadata, and message."""

    description: str | None = None
    target: str
    service: str
    method: str
    metadata: dict[str, str] = Field(default_factory=dict)
    message: dict | None = None


class GrpcConfig(BaseModel):
    """gRPC configuration: targets, descriptors, and templates."""

    targets: dict[str, GrpcTarget] = Field(default_factory=dict)
    descriptors: list[GrpcDescriptorSource] = Field(default_factory=list)
    templates: dict[str, GrpcTemplate] = Field(default_factory=dict)


class GrpcMatch(BaseModel):
    """gRPC request matching criteria for mock stubs (mirrors :class:`HttpMatch`).

    ``body`` is a subset match against the incoming request message; ``jq`` is a
    predicate evaluated against the incoming envelope. Both optional and may
    coexist; a stub matches only if all provided criteria pass.
    """

    body: dict | None = None
    jq: str | None = None


class GrpcResponseMessage(BaseModel):
    """A single streaming-response message (one element of ``GrpcResponse.messages``)."""

    message: Any
    delay_ms: int = Field(default=0, ge=0)


class GrpcResponse(BaseModel):
    """gRPC response definition for a mock stub.

    Exactly one of ``message`` (unary / client_stream / bidi single authored
    payload) or ``messages`` (server_stream sequence, one entry per streamed
    response message) must be set. ``status`` is
    validated here against the gRPC status enum via :func:`parse_grpc_status`,
    but stored verbatim (name or int as authored) — ``(code, name)`` resolution
    happens at render time (Task 5). Response-shape-vs-call-type (e.g.
    ``messages`` on a unary method) needs the descriptor pool and is therefore
    deferred to the server (Task 6); this model enforces only the structural
    exactly-one-of and status validity.
    """

    status: str | int = "OK"
    message: Any = None
    messages: list[GrpcResponseMessage] | None = None
    metadata: dict[str, str] | None = None

    @field_validator("status")
    @classmethod
    def _validate_status(cls, v: str | int) -> str | int:
        """Reject invalid gRPC status at model parse time.

        Delegates name/code resolution to :func:`parse_grpc_status` (single
        source of truth; case-sensitive name lookup, digit-string→int coercion,
        0-16 range). The helper raises :class:`ConfigError`; re-raise as
        ``ValueError`` so Pydantic surfaces it as a ``ValidationError`` (the
        config loader turns that into exit 2). The original ``v`` is returned
        unchanged — resolution to ``(code, name)`` is deferred to render time.
        """
        try:
            parse_grpc_status(v)
        except ConfigError as e:
            raise ValueError(str(e)) from e
        return v

    @model_validator(mode="after")
    def _exactly_one_of_message_or_messages(self) -> "GrpcResponse":
        """Enforce exactly-one-of ``message`` / ``messages`` (structural check).

        Both set, or neither set, -> ``ValidationError``. ``message is None`` is
        treated as "unset" (the field's default): an explicitly-authored
        ``message: None`` is indistinguishable from omission. Authoring intent
        for an empty unary response is expressed via ``message: {}`` (an
        empty-but-present payload); omitting both keys is rejected.
        """
        if (self.message is not None) == (self.messages is not None):
            raise ValueError(
                "grpc response must set exactly one of 'message' or 'messages'"
            )
        return self


class GrpcStub(BaseModel):
    """gRPC mock stub definition: match an incoming call and author its response."""

    description: str | None = None
    service: str
    method: str
    match: GrpcMatch | None = None
    capture: dict[str, CaptureSpec] | None = None
    response: GrpcResponse
    delay_ms: int = Field(default=0, ge=0)


class GrpcMockConfig(BaseModel):
    """gRPC mock server configuration (mirrors :class:`HttpMockConfig`).

    ``descriptors`` supplies the proto/descriptor-set sources used to resolve
    service/method names and encode response messages at render time (Task 5/6).
    """

    listen: str = "0.0.0.0:50051"
    descriptors: list[GrpcDescriptorSource] | None = None
    reflection: bool = True
    health: bool = True
    concurrency_cap: int = Field(default=64, ge=1)
    stubs: dict[str, GrpcStub] = Field(default_factory=dict)

    @field_validator("listen")
    @classmethod
    def _validate_listen(cls, v: str) -> str:
        """Validate listen address format (mirrors ``HttpMockConfig.listen``)."""
        try:
            parse_listen(v)
        except ValueError as e:
            raise ValueError(f"invalid listen address: {e}") from e
        return v


# ``MocksConfig`` (defined above, before the gRPC mock section) references
# ``GrpcMockConfig`` via a forward reference; rebuild now that the gRPC mock
# models are in scope so the ``grpc`` field's core schema resolves.
MocksConfig.model_rebuild()


class Config(BaseModel):
    version: str
    services: dict[str, ServiceConfig] = Field(default_factory=dict)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    templates: dict[str, HttpTemplate] = Field(default_factory=dict)
    defaults: Defaults = Field(default_factory=Defaults)
    mocks: MocksConfig | None = None
    logs: LogsConfig = Field(default_factory=LogsConfig)
    grpc: GrpcConfig = Field(default_factory=GrpcConfig)


class PartialConfig(Config):
    """Overlay fragment — Config with version optional; version is inherited from the base at merge time (spec D5)."""

    version: str | None = None
