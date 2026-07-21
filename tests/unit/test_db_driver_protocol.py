"""Unit tests for DBDriver protocol DTOs (DESIGN §9.1).

Covers construction, defaults, and ``dataclasses.asdict`` round-trip for the
six DTOs defined in :mod:`agctl.clients.db_driver_protocol`:

- :class:`WriteResult`
- :class:`ColumnInfo`
- :class:`ForeignKey`
- :class:`UniqueConstraint`
- :class:`SchemaItem`
- :class:`SchemaMatch`
"""

from __future__ import annotations

import dataclasses

from agctl.clients.db_driver_protocol import (
    ColumnInfo,
    ForeignKey,
    SchemaItem,
    SchemaMatch,
    UniqueConstraint,
    WriteResult,
)


class TestWriteResult:
    """Construction and round-trip for WriteResult."""

    def test_default_returning_is_empty_list(self):
        """WriteResult(rows_affected=5) has returning == [] (default factory)."""
        wr = WriteResult(rows_affected=5)
        assert wr.rows_affected == 5
        assert wr.returning == []

    def test_default_returning_is_per_instance(self):
        """Each instance gets its own list (not a shared class attribute)."""
        a = WriteResult(rows_affected=1)
        b = WriteResult(rows_affected=2)
        a.returning.append({"id": 1})
        assert b.returning == []

    def test_asdict_round_trip_with_returning(self):
        """WriteResult round-trips via asdict including None and list values."""
        wr = WriteResult(rows_affected=None, returning=[{"id": 1}])
        assert dataclasses.asdict(wr) == {
            "rows_affected": None,
            "returning": [{"id": 1}],
        }

    def test_asdict_round_trip_default(self):
        """WriteResult with default returning round-trips to empty list."""
        wr = WriteResult(rows_affected=3)
        assert dataclasses.asdict(wr) == {
            "rows_affected": 3,
            "returning": [],
        }


class TestColumnInfo:
    """Construction and defaults for ColumnInfo."""

    def test_required_fields_only_defaults(self):
        """ColumnInfo(name=..., data_type=..., nullable=False) has the rest None."""
        col = ColumnInfo(name="id", data_type="integer", nullable=False)
        assert col.name == "id"
        assert col.data_type == "integer"
        assert col.nullable is False
        assert col.default is None
        assert col.generated is None
        assert col.enum_values is None
        assert col.comment is None

    def test_full_construction(self):
        """ColumnInfo accepts all seven fields."""
        col = ColumnInfo(
            name="status",
            data_type="text",
            nullable=False,
            default="'open'",
            generated="stored",
            enum_values=["open", "closed"],
            comment="order status",
        )
        assert col.name == "status"
        assert col.data_type == "text"
        assert col.nullable is False
        assert col.default == "'open'"
        assert col.generated == "stored"
        assert col.enum_values == ["open", "closed"]
        assert col.comment == "order status"

    def test_asdict_has_seven_fields(self):
        """asdict(ColumnInfo(...)) has exactly the seven ColumnInfo fields."""
        col = ColumnInfo(name="id", data_type="integer", nullable=False)
        d = dataclasses.asdict(col)
        assert set(d.keys()) == {
            "name",
            "data_type",
            "nullable",
            "default",
            "generated",
            "enum_values",
            "comment",
        }
        assert d == {
            "name": "id",
            "data_type": "integer",
            "nullable": False,
            "default": None,
            "generated": None,
            "enum_values": None,
            "comment": None,
        }


class TestForeignKey:
    """Construction and round-trip for ForeignKey."""

    def test_construction(self):
        fk = ForeignKey(
            name="orders_user_fk",
            columns=["user_id"],
            references_schema="public",
            references_table="users",
            references_columns=["id"],
        )
        assert fk.name == "orders_user_fk"
        assert fk.columns == ["user_id"]
        assert fk.references_schema == "public"
        assert fk.references_table == "users"
        assert fk.references_columns == ["id"]

    def test_references_schema_optional(self):
        fk = ForeignKey(
            name="fk1",
            columns=["a"],
            references_schema=None,
            references_table="t",
            references_columns=["b"],
        )
        assert fk.references_schema is None

    def test_asdict_round_trip(self):
        fk = ForeignKey(
            name="orders_user_fk",
            columns=["user_id"],
            references_schema="public",
            references_table="users",
            references_columns=["id"],
        )
        assert dataclasses.asdict(fk) == {
            "name": "orders_user_fk",
            "columns": ["user_id"],
            "references_schema": "public",
            "references_table": "users",
            "references_columns": ["id"],
        }


class TestUniqueConstraint:
    """Construction and round-trip for UniqueConstraint."""

    def test_construction(self):
        uc = UniqueConstraint(name="uq_orders_number", columns=["number"])
        assert uc.name == "uq_orders_number"
        assert uc.columns == ["number"]

    def test_asdict_round_trip(self):
        uc = UniqueConstraint(name="uq", columns=["a", "b"])
        assert dataclasses.asdict(uc) == {"name": "uq", "columns": ["a", "b"]}


class TestSchemaItem:
    """Construction and round-trip for SchemaItem."""

    def test_construction(self):
        si = SchemaItem(schema="public", name="orders", kind="table", column_count=4)
        assert si.schema == "public"
        assert si.name == "orders"
        assert si.kind == "table"
        assert si.column_count == 4

    def test_kind_view(self):
        si = SchemaItem(schema="public", name="v_orders", kind="view", column_count=2)
        assert si.kind == "view"

    def test_asdict_round_trip(self):
        si = SchemaItem(schema="public", name="orders", kind="table", column_count=4)
        assert dataclasses.asdict(si) == {
            "schema": "public",
            "name": "orders",
            "kind": "table",
            "column_count": 4,
        }


class TestSchemaMatch:
    """Construction, nesting, and round-trip for SchemaMatch."""

    def _sample_columns(self):
        return [
            ColumnInfo(name="id", data_type="integer", nullable=False),
            ColumnInfo(
                name="status",
                data_type="text",
                nullable=False,
                default="'open'",
                enum_values=["open", "closed"],
            ),
        ]

    def test_construction_flat(self):
        sm = SchemaMatch(
            schema="public",
            table="orders",
            kind="table",
            comment=None,
            columns=[],
            primary_key=["id"],
            foreign_keys=[],
            unique_constraints=[],
        )
        assert sm.schema == "public"
        assert sm.table == "orders"
        assert sm.kind == "table"
        assert sm.comment is None
        assert sm.columns == []
        assert sm.primary_key == ["id"]
        assert sm.foreign_keys == []
        assert sm.unique_constraints == []

    def test_asdict_nested_round_trip(self):
        """SchemaMatch round-trips via asdict to a nested dict.

        ``columns`` is a list of dicts (each with all seven ColumnInfo fields).
        ``foreign_keys`` and ``unique_constraints`` likewise recurse.
        """
        cols = self._sample_columns()
        fks = [
            ForeignKey(
                name="orders_user_fk",
                columns=["user_id"],
                references_schema="public",
                references_table="users",
                references_columns=["id"],
            )
        ]
        uqs = [UniqueConstraint(name="uq_number", columns=["number"])]

        sm = SchemaMatch(
            schema="public",
            table="orders",
            kind="table",
            comment="order fact table",
            columns=cols,
            primary_key=["id"],
            foreign_keys=fks,
            unique_constraints=uqs,
        )

        expected = {
            "schema": "public",
            "table": "orders",
            "kind": "table",
            "comment": "order fact table",
            "columns": [
                {
                    "name": "id",
                    "data_type": "integer",
                    "nullable": False,
                    "default": None,
                    "generated": None,
                    "enum_values": None,
                    "comment": None,
                },
                {
                    "name": "status",
                    "data_type": "text",
                    "nullable": False,
                    "default": "'open'",
                    "generated": None,
                    "enum_values": ["open", "closed"],
                    "comment": None,
                },
            ],
            "primary_key": ["id"],
            "foreign_keys": [
                {
                    "name": "orders_user_fk",
                    "columns": ["user_id"],
                    "references_schema": "public",
                    "references_table": "users",
                    "references_columns": ["id"],
                }
            ],
            "unique_constraints": [
                {"name": "uq_number", "columns": ["number"]}
            ],
        }
        assert dataclasses.asdict(sm) == expected

    def test_asdict_columns_list_of_dicts_with_seven_keys(self):
        """asdict(SchemaMatch).columns is a list of dicts each with 7 keys."""
        sm = SchemaMatch(
            schema="public",
            table="orders",
            kind="table",
            comment=None,
            columns=self._sample_columns(),
            primary_key=["id"],
            foreign_keys=[],
            unique_constraints=[],
        )
        d = dataclasses.asdict(sm)
        assert isinstance(d["columns"], list)
        assert len(d["columns"]) == 2
        for col_dict in d["columns"]:
            assert isinstance(col_dict, dict)
            assert set(col_dict.keys()) == {
                "name",
                "data_type",
                "nullable",
                "default",
                "generated",
                "enum_values",
                "comment",
            }

    def test_json_serializable(self):
        """The asdict output is JSON-serializable (no bare dataclasses)."""
        import json

        sm = SchemaMatch(
            schema="public",
            table="orders",
            kind="table",
            comment=None,
            columns=self._sample_columns(),
            primary_key=["id"],
            foreign_keys=[
                ForeignKey(
                    name="fk",
                    columns=["user_id"],
                    references_schema=None,
                    references_table="users",
                    references_columns=["id"],
                )
            ],
            unique_constraints=[],
        )
        s = json.dumps(dataclasses.asdict(sm))
        assert '"table": "orders"' in s
