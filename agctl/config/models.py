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
