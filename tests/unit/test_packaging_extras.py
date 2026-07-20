"""Packaging tests for the Kafka Schema Registry / Avro / Protobuf extras.

These extras (Task 15) split the heavy codec libs out of the core install so
a lean `pip install agctl` stays codec-free, while
``pip install 'agctl[schema-registry]'`` pulls everything needed to decode
Avro and Protobuf Kafka payloads out of the box.

The test parses ``pyproject.toml`` via ``tomllib`` (no build-backend import)
so it runs in any environment that has the source tree checked out, with no
need to actually install the package.
"""

from __future__ import annotations

from pathlib import Path

import tomllib


def _load_optional_dependencies() -> dict[str, list[str]]:
    pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)
    return pyproject["project"]["optional-dependencies"]


def test_avro_extra_exists():
    """The ``avro`` extra exists and pins fastavro (>=1.8)."""
    optional_deps = _load_optional_dependencies()
    assert "avro" in optional_deps, "avro extra should exist"
    assert any(
        spec.startswith("fastavro") for spec in optional_deps["avro"]
    ), "avro extra should pin fastavro"


def test_protobuf_extra_exists_and_is_standalone():
    """The ``protobuf`` extra exists and does NOT drag in grpcio.

    A Kafka-Protobuf user must be able to install just the codec without being
    forced into the (much heavier) grpc extra — so we assert no spec in the
    ``protobuf`` extra references ``grpcio``.
    """
    optional_deps = _load_optional_dependencies()
    assert "protobuf" in optional_deps, "protobuf extra should exist"
    assert any(
        spec.startswith("protobuf") for spec in optional_deps["protobuf"]
    ), "protobuf extra should pin protobuf"
    for spec in optional_deps["protobuf"]:
        assert not spec.startswith("grpcio"), (
            f"protobuf extra must not pull grpcio (found {spec!r}); "
            "a Kafka-Protobuf user should not be forced into the grpc extra"
        )


def test_schema_registry_meta_extra_bundles_avro_and_protobuf():
    """``schema-registry`` is a convenience meta-extra that bundles both codecs.

    It must reference both the avro and protobuf extras (in either the combined
    ``agctl[avro,protobuf]`` form or as separate ``agctl[avro]`` +
    ``agctl[protobuf]`` entries) so a single
    ``pip install 'agctl[schema-registry]'`` makes every codec available.
    """
    optional_deps = _load_optional_dependencies()
    assert "schema-registry" in optional_deps, "schema-registry extra should exist"
    specs = optional_deps["schema-registry"]
    # Accept either the combined form (agctl[avro,protobuf]) or separate
    # entries (agctl[avro] + agctl[protobuf]).
    joined = " ".join(specs)
    assert "avro" in joined, "schema-registry extra must reference agctl[avro]"
    assert "protobuf" in joined, "schema-registry extra must reference agctl[protobuf]"
    # Every entry must be an agctl self-reference (no raw third-party specs in
    # the meta-extra — those live in the codec extras themselves).
    for spec in specs:
        assert spec.startswith("agctl["), (
            f"schema-registry meta-extra should only contain agctl[...] "
            f"self-references (found {spec!r})"
        )


def test_kafka_extra_pulls_authlib():
    """The ``kafka`` extra must make ``authlib`` importable.

    ``confluent_kafka.schema_registry`` (which ships with ``confluent-kafka``)
    imports ``authlib`` at module import time, but ``confluent-kafka`` only
    declares ``authlib`` under its own ``[schemaregistry]`` extra — NOT as a
    core transitive dep. So a plain ``confluent-kafka>=2.4`` pin leaves SR
    broken: ``pip install 'agctl[kafka]'`` pulls confluent-kafka but NOT
    authlib, and constructing the SR client fails with ``ModuleNotFoundError:
    No module named 'authlib'`` (then cachetools, etc.).

    The fix is to depend on ``confluent-kafka[schemaregistry]>=2.4``, which
    pulls authlib (and cachetools, attrs, certifi, httpx) the way upstream
    intends. This test accepts EITHER form: an explicit ``authlib`` spec, OR a
    ``confluent-kafka[schemaregistry]`` reference. Both make authlib available
    to ``pip install 'agctl[kafka]'``.
    """
    optional_deps = _load_optional_dependencies()
    assert "kafka" in optional_deps, "kafka extra should exist"
    specs = optional_deps["kafka"]
    has_explicit_authlib = any(spec.startswith("authlib") for spec in specs)
    has_schemaregistry_extra = any(
        "confluent-kafka" in spec and "schemaregistry" in spec for spec in specs
    )
    assert has_explicit_authlib or has_schemaregistry_extra, (
        "kafka extra must make authlib importable, either via an explicit "
        "'authlib' spec or via 'confluent-kafka[schemaregistry]' "
        f"(found {specs!r})"
    )


def test_integration_extra_pulls_codecs():
    """The ``integration`` extra must pull the codecs for live integration tests.

    Live SR integration tests (Task 16) need to decode Avro and Protobuf
    payloads, so the ``integration`` extra must reference both codecs in
    addition to the existing ``agctl[db,kafka,http,grpc]`` bundle.
    """
    optional_deps = _load_optional_dependencies()
    assert "integration" in optional_deps, "integration extra should exist"
    specs = optional_deps["integration"]
    # The codecs are bundled via the meta-extra, which expands to both
    # agctl[avro] and agctl[protobuf].
    assert any(
        "schema-registry" in spec or ("avro" in spec and "protobuf" in spec)
        for spec in specs
    ), (
        "integration extra must pull the Avro/Protobuf codecs "
        "(via agctl[schema-registry] or agctl[avro,protobuf])"
    )
