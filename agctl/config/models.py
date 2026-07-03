"""Pydantic v2 schema models for agctl.yaml (DESIGN §2)."""

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


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


class Config(BaseModel):
    version: str
    services: dict[str, ServiceConfig] = Field(default_factory=dict)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    templates: dict[str, HttpTemplate] = Field(default_factory=dict)
    defaults: Defaults = Field(default_factory=Defaults)
