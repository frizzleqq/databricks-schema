from __future__ import annotations

import pytest
from databricks.sdk.service.catalog import TableType

from databricks_schema.dbml_io import (
    DbmlDependencyError,
    DbmlParseError,
    catalog_from_dbml,
    catalog_to_dbml,
    schema_from_dbml,
    schema_to_dbml,
    schemas_from_dbml,
)
from databricks_schema.models import Catalog, Column, ForeignKey, PrimaryKey, Schema, Table
from databricks_schema.yaml_io import load_schema_dir


def _full_catalog() -> Catalog:
    return Catalog(
        name="main",
        comment="it's the\nmain catalog",
        tags={"env": "prod"},
        schemas=[
            Schema(
                name="sales",
                comment="Sales data",
                owner="bob",
                tags={"domain": "sales"},
                grants={"analysts": ["USE_SCHEMA"]},
                tables=[
                    Table(
                        name="orders",
                        table_type=TableType.MANAGED,
                        comment="Orders",
                        owner="alice",
                        storage_location="s3://bucket/orders",
                        tags={"pii": "false"},
                        grants={"analysts": ["MODIFY", "SELECT"], "etl": ["ALL_PRIVILEGES"]},
                        columns=[
                            Column(
                                name="id",
                                data_type="bigint",
                                nullable=False,
                                comment="Primary key",
                                tags={"role": "id"},
                            ),
                            Column(name="customer id", data_type="bigint"),
                            Column(name="meta", data_type="struct<a:int,b:array<string>>"),
                            Column(name="amount", data_type="decimal(10,2)", comment=""),
                            Column(name="attrs", data_type="map<string, string>"),
                        ],
                        primary_key=PrimaryKey(name="pk_orders", columns=["id"]),
                        foreign_keys=[
                            ForeignKey(
                                name="fk_customer",
                                columns=["customer id"],
                                ref_schema="crm",
                                ref_table="customers",
                                ref_columns=["id"],
                            ),
                        ],
                    )
                ],
            ),
            Schema(name="empty"),
            Schema(
                name="crm",
                tables=[
                    Table(
                        name="customers",
                        table_type=TableType.EXTERNAL,
                        columns=[
                            Column(name="id", data_type="bigint", nullable=False),
                            Column(name="region", data_type="string", nullable=False),
                        ],
                        primary_key=PrimaryKey(columns=["id", "region"]),
                    )
                ],
            ),
        ],
    )


class TestRoundTrip:
    def test_catalog_round_trip_is_lossless(self):
        catalog = _full_catalog()
        assert catalog_from_dbml(catalog_to_dbml(catalog)) == catalog

    def test_schema_round_trip(self):
        schema = _full_catalog().schemas[0]
        text = schema_to_dbml(schema)
        assert "Project" not in text
        assert schema_from_dbml(text) == schema

    def test_composite_fk_round_trip(self):
        schema = Schema(
            name="s",
            tables=[
                Table(
                    name="child",
                    columns=[Column(name="a", data_type="int"), Column(name="b", data_type="int")],
                    foreign_keys=[
                        ForeignKey(
                            columns=["a", "b"],
                            ref_schema="other",
                            ref_table="parent",
                            ref_columns=["x", "y"],
                        )
                    ],
                )
            ],
        )
        assert schema_from_dbml(schema_to_dbml(schema)) == schema

    def test_note_escaping(self):
        tricky = "back\\slash, 'quotes', \"double\"\n  indented\n\n@not metadata here"
        schema = Schema(
            name="s",
            comment=tricky,
            tables=[Table(name="t", columns=[Column(name="c", data_type="int", comment=tricky)])],
        )
        assert schema_from_dbml(schema_to_dbml(schema)) == schema


class TestWriterOutput:
    def test_complex_types_are_quoted_plain_types_are_not(self):
        text = catalog_to_dbml(_full_catalog())
        assert '"meta" "struct<a:int,b:array<string>>"' in text
        assert '"attrs" "map<string, string>"' in text
        assert '"amount" decimal(10,2)' in text
        assert '"id" bigint [not null' in text

    def test_metadata_lines_in_table_note(self):
        text = catalog_to_dbml(_full_catalog())
        assert "@table_type: MANAGED" in text
        assert "@owner: alice" in text
        assert "@storage_location: s3://bucket/orders" in text
        assert '@tags: {"pii": "false"}' in text
        assert '@grants: {"analysts": ["MODIFY", "SELECT"], "etl": ["ALL_PRIVILEGES"]}' in text
        assert '@grants: {"analysts": ["USE_SCHEMA"]}' in text

    def test_primary_key_and_ref(self):
        text = catalog_to_dbml(_full_catalog())
        assert "(\"id\") [pk, name: 'pk_orders']" in text
        assert 'Ref "fk_customer": "sales"."orders"."customer id" > "crm"."customers"."id"' in text

    def test_every_schema_gets_a_table_group(self):
        text = catalog_to_dbml(_full_catalog())
        assert 'TableGroup "empty" {' in text
        assert 'TableGroup "crm" {\n    "crm"."customers"\n}' in text

    def test_no_note_without_comment_or_metadata(self):
        schema = Schema(
            name="s", tables=[Table(name="t", columns=[Column(name="c", data_type="int")])]
        )
        assert "Note" not in schema_to_dbml(schema)
        assert "note" not in schema_to_dbml(schema)

    def test_identifier_with_double_quote_rejected(self):
        with pytest.raises(ValueError, match="double quote"):
            schema_to_dbml(Schema(name='bad"name'))


class TestHandWrittenDbml:
    def test_dbdiagram_style_input(self):
        text = """
// generated in dbdiagram
Table users as U {
  id integer [pk, increment]
  username varchar [unique, not null, note: 'login name']
  created_at timestamp [default: `now()`]
  Note: 'All users'
}

Table posts {
  id integer [primary key]
  user_id integer [ref: > U.id]
  status post_status
}

Enum post_status {
  draft
  published
}

TableGroup blog {
  users
  posts
}

Note sticky {
  'free-floating note'
}
"""
        (schema,) = schemas_from_dbml(text)
        assert schema.name == "public"
        users, posts = schema.tables
        assert users.comment == "All users"
        assert users.primary_key == PrimaryKey(columns=["id"])
        assert [(c.name, c.data_type, c.nullable) for c in users.columns] == [
            ("id", "integer", False),
            ("username", "varchar", False),
            ("created_at", "timestamp", True),
        ]
        assert users.columns[1].comment == "login name"
        assert posts.foreign_keys == [
            ForeignKey(
                columns=["user_id"], ref_schema="public", ref_table="users", ref_columns=["id"]
            )
        ]
        assert posts.columns[2].data_type == "post_status"

    def test_reverse_ref_direction(self):
        text = """
Table s.parent {
  id int
}
Table s.child {
  pid int
}
Ref: s.parent.id < s.child.pid
"""
        (schema,) = schemas_from_dbml(text)
        child = schema.tables[1]
        assert child.foreign_keys == [
            ForeignKey(columns=["pid"], ref_schema="s", ref_table="parent", ref_columns=["id"])
        ]

    def test_many_to_many_ref_skipped(self):
        text = "Table a {\n x int\n}\nTable b {\n y int\n}\nRef: a.x <> b.y\n"
        assert all(not t.foreign_keys for t in schemas_from_dbml(text)[0].tables)

    def test_multiple_pk_column_flags_form_composite_key(self):
        text = "Table s.t {\n a int [pk]\n b int [pk]\n}\n"
        (schema,) = schemas_from_dbml(text)
        assert schema.tables[0].primary_key == PrimaryKey(columns=["a", "b"])

    def test_non_schema_table_group_ignored(self):
        text = "Table s.t {\n a int\n}\nTableGroup \"g\" [note: 'x'] {\n s.t\n}\n"
        schemas = schemas_from_dbml(text)
        assert [s.name for s in schemas] == ["s"]
        assert schemas[0].comment is None


class TestErrors:
    def test_syntax_error(self):
        with pytest.raises(DbmlParseError, match="line"):
            schemas_from_dbml("Table t {\n a int [bogus_setting]\n}\n")

    def test_catalog_requires_project(self):
        with pytest.raises(DbmlParseError, match="Project"):
            catalog_from_dbml("Table t {\n a int\n}\n")

    def test_schema_from_dbml_requires_single_schema(self):
        with pytest.raises(DbmlParseError, match="exactly one"):
            schema_from_dbml(catalog_to_dbml(_full_catalog()))

    def test_invalid_tags_json(self):
        text = "Table s.t {\n a int [note: '@tags: not-json']\n}\n"
        with pytest.raises(DbmlParseError, match="@tags"):
            schemas_from_dbml(text)

    @pytest.mark.parametrize("comment", [None, "", "text"])
    def test_comment_round_trips_alongside_metadata(self, comment):
        schema = Schema(
            name="s",
            comment=comment,
            owner="bob",
            grants={"analysts": ["USE_SCHEMA"]},
            tables=[
                Table(
                    name="t",
                    comment=comment,
                    owner="bob",
                    columns=[Column(name="a", data_type="int")],
                )
            ],
        )
        assert schema_from_dbml(schema_to_dbml(schema)) == schema

    @pytest.mark.parametrize("grants", ["not-json", '["SELECT"]', '{"bob": "SELECT"}'])
    def test_invalid_grants(self, grants):
        text = f"Table s.t {{\n a int\n Note: '@grants: {grants}'\n}}\n"
        with pytest.raises(DbmlParseError, match="@grants"):
            schemas_from_dbml(text)


class TestLoadSchemaDir:
    def test_loads_all_schemas_from_dbml_files(self, tmp_path):
        (tmp_path / "main.dbml").write_text(catalog_to_dbml(_full_catalog()), encoding="utf-8")
        schemas = load_schema_dir(tmp_path, "dbml")
        assert list(schemas) == ["sales", "empty", "crm"]

    def test_filters_on_schema_name(self, tmp_path):
        (tmp_path / "main.dbml").write_text(catalog_to_dbml(_full_catalog()), encoding="utf-8")
        schemas = load_schema_dir(tmp_path, "dbml", frozenset({"crm"}))
        assert list(schemas) == ["crm"]

    def test_duplicate_schema_across_files_rejected(self, tmp_path):
        text = catalog_to_dbml(_full_catalog())
        (tmp_path / "a.dbml").write_text(text, encoding="utf-8")
        (tmp_path / "b.dbml").write_text(text, encoding="utf-8")
        with pytest.raises(DbmlParseError, match="defined in both"):
            load_schema_dir(tmp_path, "dbml")


class TestWithoutPydbml:
    def test_writing_needs_no_pydbml(self, monkeypatch):
        monkeypatch.setattr("databricks_schema.dbml_io.PYDBML_AVAILABLE", False)
        assert catalog_to_dbml(_full_catalog()).startswith('Project "main"')

    def test_reading_raises_helpful_error(self, monkeypatch):
        monkeypatch.setattr("databricks_schema.dbml_io.PYDBML_AVAILABLE", False)
        with pytest.raises(DbmlDependencyError, match=r"databricks-schema\[dbml\]"):
            schemas_from_dbml("Table t {\n a int\n}\n")
