"""Tests for config validator (DESIGN §3.5 dangling refs, §3.6 warnings)."""

import pytest

from agctl.config.models import Config
from agctl.config.validator import validate_config


def test_grpc_template_unknown_target_is_error():
    """A gRPC template referencing a missing target produces an error."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "targets": {"existing-target": {"address": "localhost:50051"}},
                "templates": {
                    "t1": {
                        "target": "missing-target",
                        "service": "s.Svc",
                        "method": "M",
                        "description": "A template",
                    }
                },
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == [
        {
            "path": "grpc.templates.t1.target",
            "message": "gRPC template references unknown target 'missing-target'",
        }
    ]
    # No template-level warning for this config (description not involved)
    assert warnings == []


def test_grpc_template_known_target_no_error():
    """A gRPC template referencing an existing target produces no error."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "targets": {"existing-target": {"address": "localhost:50051"}},
                "templates": {
                    "t1": {
                        "target": "existing-target",
                        "service": "s.Svc",
                        "method": "M",
                        "description": "A template",
                    }
                },
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == []


def test_grpc_descriptor_both_set_is_error():
    """A grpc.descriptors entry with both proto and descriptor_set set is an error."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "descriptors": [
                    {"proto": "a.proto", "descriptor_set": "b.pb"}
                ],
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == [
        {
            "path": "grpc.descriptors[0]",
            "message": "each grpc.descriptors entry must set exactly one of 'proto' or 'descriptor_set'",
        }
    ]
    assert warnings == []


def test_grpc_descriptor_neither_set_is_error():
    """A grpc.descriptors entry with neither proto nor descriptor_set set is an error."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "descriptors": [{}],
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == [
        {
            "path": "grpc.descriptors[0]",
            "message": "each grpc.descriptors entry must set exactly one of 'proto' or 'descriptor_set'",
        }
    ]
    assert warnings == []


def test_grpc_descriptor_exactly_one_ok():
    """A grpc.descriptors entry with exactly one of proto or descriptor_set is valid."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "descriptors": [
                    {"proto": "a.proto", "include_paths": ["."]},
                    {"descriptor_set": "b.pb"},
                ],
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == []


def test_grpc_template_missing_description_warns():
    """A gRPC template with no description produces a warning."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "targets": {"t1": {"address": "localhost:50051"}},
                "templates": {
                    "tmpl1": {
                        "target": "t1",
                        "service": "s.Svc",
                        "method": "M",
                        "description": None,
                    }
                },
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == [
        {
            "path": "grpc.templates.tmpl1",
            "message": "missing description (discovery degrades without it)",
        }
    ]


def test_grpc_template_with_description_no_warning():
    """A gRPC template with a description produces no warning."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "targets": {"t1": {"address": "localhost:50051"}},
                "templates": {
                    "tmpl1": {
                        "target": "t1",
                        "service": "s.Svc",
                        "method": "M",
                        "description": "Call the method",
                    }
                },
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == []
