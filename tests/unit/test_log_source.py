"""Unit tests for the ``LogSource`` config model.

Covers the remote-backend fields (``url``/``query``/``options``) added for the
Loki log backend, backward compatibility with minimal ``file`` sources, and the
strict-model guarantee that unknown top-level keys still error (``options`` is
the only intentional escape hatch for backend-specific extras).
"""

import pytest
from pydantic import ValidationError

from agctl.config.models import LogSource


def test_loki_source_parses_url_query_and_options_with_types_preserved():
    """A ``loki`` source populates url/query/options and preserves option types."""
    source = LogSource(
        type="loki",
        url="http://loki:3100",
        query='{app="x"}',
        service="loki-svc",
        options={
            "username": "u",
            "password": "p",
            "token": "t",
            "org_id": "o",
            "verify_tls": False,
            "fetch_limit": 10,
            "direction": "backward",
        },
    )
    assert source.type == "loki"
    assert source.url == "http://loki:3100"
    assert source.query == '{app="x"}'
    assert source.service == "loki-svc"
    # options preserves every key and its native Python type (no coercion).
    assert source.options["username"] == "u"
    assert source.options["password"] == "p"
    assert source.options["token"] == "t"
    assert source.options["org_id"] == "o"
    assert source.options["verify_tls"] is False
    assert source.options["fetch_limit"] == 10
    assert source.options["direction"] == "backward"


def test_minimal_file_source_is_backward_compatible():
    """A minimal ``file`` source still parses with url/query None, options empty."""
    source = LogSource(type="file", path="/var/log/app.log")
    assert source.type == "file"
    assert source.path == "/var/log/app.log"
    assert source.format == "logstash"
    assert source.url is None
    assert source.query is None
    assert source.options == {}


def test_unknown_top_level_key_raises_validation_error():
    """An unknown top-level key is rejected — the model stays strict.

    ``options`` is the only place arbitrary backend-specific keys are accepted;
    a stray key at the source level must still raise so typos surface early.
    """
    with pytest.raises(ValidationError):
        LogSource(type="file", path="/x", bogus=1)
