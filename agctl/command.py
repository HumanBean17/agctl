"""Envelope command wrapper (DESIGN §4.1).

``@envelope(command)`` wraps a Click-style callback so that every code path
emits exactly one JSON envelope via :func:`agctl.output.emit` and exits with
the appropriate code. Importing this module never depends on a command being
run — ``load_config`` is imported lazily inside :func:`load_config_or_raise`.
"""

import time
from typing import Any, Callable

from .errors import AgctlError
from .output import emit


def _elapsed_ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


def envelope(command: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator: wrap a command callback in the success/error envelope."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.monotonic()
            try:
                result = fn(*args, **kwargs)
            except SystemExit:
                # A nested SystemExit (e.g. from a sub-call) propagates as-is.
                raise
            except AgctlError as err:
                emit(
                    ok=False,
                    command=command,
                    error=err.to_dict(),
                    duration_ms=_elapsed_ms(start),
                )
                raise SystemExit(err.exit_code)
            except AssertionError as err:
                # Builtin ``assert`` behaves as an assertion failure.
                emit(
                    ok=False,
                    command=command,
                    error={"type": "AssertionError", "message": str(err), "detail": {}},
                    duration_ms=_elapsed_ms(start),
                )
                raise SystemExit(1)
            except Exception as exc:  # last resort -> InternalError
                emit(
                    ok=False,
                    command=command,
                    error={"type": "InternalError", "message": str(exc), "detail": {}},
                    duration_ms=_elapsed_ms(start),
                )
                raise SystemExit(2)
            else:
                emit(
                    ok=True,
                    command=command,
                    result=result,
                    duration_ms=_elapsed_ms(start),
                )
                return result

        return wrapper

    return decorator


def load_config_or_raise(
    config_path: str | None = None, overlay_paths: list[str] | None = None
):
    """Load config, letting ConfigError propagate to the envelope wrapper (exit 2)."""
    from .config import load_config

    return load_config(config_path, overlays=overlay_paths)
