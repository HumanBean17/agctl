"""TEMP diagnostic (revert after): isolate the Windows-only post-summary exit 1.

On Windows CI, `pytest tests/unit` exits 1 despite "1109 passed, 112 skipped"
(0 failures, no stderr) — not reproducible on Linux/macOS. Skipping every new
listen test file on Windows tells us whether the exit-1 originates in the listen
suite (→ green here) or is pre-existing/exposed-by-completion (→ still red).
"""
import os

import pytest


def pytest_collection_modifyitems(config, items):
    if os.name == "nt":
        skip = pytest.mark.skip(reason="diagnostic: isolating Windows-only exit 1")
        for item in items:
            p = str(item.fspath).replace("\\", "/").lower()
            if "test_listen" in p or "test_kafka_listen" in p:
                item.add_marker(skip)
