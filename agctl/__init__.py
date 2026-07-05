"""agctl — agent-facing CLI harness for testing distributed systems."""

import importlib.metadata

#: Package version, read from installed distribution metadata. Resolves to a
#: best-effort sentinel when the package is imported without being installed
#: (e.g. directly from a source checkout with no metadata) so ``--version``
#: never crashes the CLI.
try:
    __version__ = importlib.metadata.version("agctl")
except importlib.metadata.PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0+unknown"
