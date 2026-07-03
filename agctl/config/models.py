"""Pydantic v2 schema models for agctl.yaml (DESIGN §2)."""

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


def parse_listen(listen: str) -> tuple[str, int]:
    """Parse a listen address string into (host, port).

    Args:
        listen: Address string in format "host:port" or "[host]:port" for IPv6.

    Returns:
        Tuple of (host, port) where host is the string without brackets and port is an int.

    Raises:
        ValueError: If listen is empty, missing port, or port is not a valid integer.
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

    try:
        port = int(port_part)
    except ValueError:
        raise ValueError(f"port must be an integer, got {port_part!r}")

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


class KafkaConfig(BaseModel):
    brokers: list[str] = Field(default_factory=list)
    default_consumer_group: str | None = None
    schema_registry_url: str | None = None
    timeout_seconds: int | None = None
    patterns: dict[str, KafkaPattern] = Field(default_factory=dict)
    ssl: KafkaSSL | None = None


class DatabaseConnection(BaseModel):
    type: str
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
    """HTTP request matching criteria for mock stubs."""

    body: dict | None = None


class HttpStub(BaseModel):
    """HTTP mock stub definition."""

    description: str | None = None
    method: str
    path: str
    match: HttpMatch | None = None
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
    reaction: KafkaReaction


class KafkaMockConfig(BaseModel):
    """Kafka mock reactor configuration."""

    reactors: dict[str, KafkaReactor] = Field(default_factory=dict)


class MocksConfig(BaseModel):
    """Mock server configuration (HTTP and Kafka)."""

    http: HttpMockConfig | None = None
    kafka: KafkaMockConfig | None = None


class Config(BaseModel):
    version: str
    services: dict[str, ServiceConfig] = Field(default_factory=dict)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    templates: dict[str, HttpTemplate] = Field(default_factory=dict)
    defaults: Defaults = Field(default_factory=Defaults)
    mocks: MocksConfig | None = None
