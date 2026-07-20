"""Typed error hierarchy for agctl (DESIGN §4.1).

Every command error has a ``type_name`` (machine-readable) and an ``exit_code``
that the envelope wrapper maps to a process exit. ``to_dict()`` produces the
``error`` field of the output envelope.
"""


class AgctlError(Exception):
    """Base for all agctl errors. Defaults to InternalError / exit 2."""

    type_name = "InternalError"
    exit_code = 2

    def __init__(self, message: str, detail: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or {}

    def to_dict(self) -> dict:
        return {"type": self.type_name, "message": self.message, "detail": self.detail}


class AssertionFailure(AgctlError):
    type_name = "AssertionError"
    exit_code = 1


class ConfigError(AgctlError):
    type_name = "ConfigError"
    exit_code = 2


class ConnectionFailure(AgctlError):
    type_name = "ConnectionError"
    exit_code = 2


class OperationTimeout(AgctlError):
    type_name = "TimeoutError"
    exit_code = 1


class TemplateNotFound(AgctlError):
    type_name = "TemplateNotFound"
    exit_code = 2


class SerializationError(AgctlError):
    type_name = "SerializationError"
    exit_code = 2
