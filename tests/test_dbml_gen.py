from __future__ import annotations

from databricks_schema.dbml_gen import schema_to_dbml, schemas_to_dbml
from databricks_schema.models import Column, ForeignKey, PrimaryKey, Schema, Table


def _schema(name="main", tables=None):
    return Schema(name=name, tables=tables or [])


def _table(
    name,
    comment=None,
    columns=None,
    tags=None,
    primary_key=None,
    foreign_keys=None,
):
    return Table(
        name=name,
        comment=comment,
        columns=columns or [],
        tags=tags or {},
        primary_key=primary_key,
        foreign_keys=foreign_keys or [],
    )


def _col(name, data_type="STRING", comment=None, nullable=True, tags=None):
    return Column(
        name=name, data_type=data_type, comment=comment, nullable=nullable, tags=tags or {}
    )


class TestTableBlock:
    def test_basic_table(self):
        schema = _schema("myschema", tables=[_table("users")])
        dbml = schema_to_dbml(schema)
        assert 'Table "myschema"."users" {' in dbml
        assert "}" in dbml

    def test_column_present(self):
        schema = _schema("s", tables=[_table("t", columns=[_col("id", "BIGINT")])])
        dbml = schema_to_dbml(schema)
        assert '"id" BIGINT' in dbml

    def test_multiple_columns(self):
        schema = _schema("s", tables=[_table("t", columns=[_col("id", "BIGINT"), _col("name")])])
        dbml = schema_to_dbml(schema)
        assert '"id" BIGINT' in dbml
        assert '"name" STRING' in dbml

    def test_not_null_column(self):
        schema = _schema("s", tables=[_table("t", columns=[_col("id", nullable=False)])])
        dbml = schema_to_dbml(schema)
        assert "[not null]" in dbml

    def test_nullable_column_has_no_not_null(self):
        schema = _schema("s", tables=[_table("t", columns=[_col("id", nullable=True)])])
        dbml = schema_to_dbml(schema)
        assert "not null" not in dbml

    def test_column_comment(self):
        schema = _schema("s", tables=[_table("t", columns=[_col("id", comment="the id")])])
        dbml = schema_to_dbml(schema)
        assert "note: 'the id'" in dbml

    def test_column_comment_escaped(self):
        schema = _schema("s", tables=[_table("t", columns=[_col("id", comment="it's here")])])
        dbml = schema_to_dbml(schema)
        assert "note: 'it\\'s here'" in dbml

    def test_not_null_and_comment_combined(self):
        schema = _schema(
            "s", tables=[_table("t", columns=[_col("id", nullable=False, comment="pk col")])]
        )
        dbml = schema_to_dbml(schema)
        assert "[not null, note: 'pk col']" in dbml


class TestPrimaryKey:
    def test_single_col_pk_inline(self):
        schema = _schema(
            "s",
            tables=[_table("t", columns=[_col("id")], primary_key=PrimaryKey(columns=["id"]))],
        )
        dbml = schema_to_dbml(schema)
        assert "[pk]" in dbml

    def test_single_col_pk_no_indexes_block(self):
        schema = _schema(
            "s",
            tables=[_table("t", columns=[_col("id")], primary_key=PrimaryKey(columns=["id"]))],
        )
        dbml = schema_to_dbml(schema)
        assert "indexes" not in dbml

    def test_composite_pk_indexes_block(self):
        schema = _schema(
            "s",
            tables=[
                _table(
                    "t",
                    columns=[_col("a"), _col("b")],
                    primary_key=PrimaryKey(columns=["a", "b"]),
                )
            ],
        )
        dbml = schema_to_dbml(schema)
        assert "indexes {" in dbml
        assert '("a", "b") [pk]' in dbml

    def test_composite_pk_with_name(self):
        schema = _schema(
            "s",
            tables=[
                _table(
                    "orders",
                    columns=[_col("a"), _col("b")],
                    primary_key=PrimaryKey(name="pk_orders", columns=["a", "b"]),
                )
            ],
        )
        dbml = schema_to_dbml(schema)
        assert "name: 'pk_orders'" in dbml

    def test_single_col_pk_no_inline_on_other_col(self):
        schema = _schema(
            "s",
            tables=[
                _table(
                    "t",
                    columns=[_col("id"), _col("name")],
                    primary_key=PrimaryKey(columns=["id"]),
                )
            ],
        )
        dbml = schema_to_dbml(schema)
        assert '"name" STRING' in dbml
        # name column must not have [pk]
        lines = dbml.splitlines()
        name_line = next(line for line in lines if '"name"' in line)
        assert "pk" not in name_line


class TestForeignKey:
    def test_single_col_fk(self):
        fk = ForeignKey(
            name="fk_user",
            columns=["user_id"],
            ref_schema="auth",
            ref_table="users",
            ref_columns=["id"],
        )
        schema = _schema("app", tables=[_table("orders", foreign_keys=[fk])])
        dbml = schema_to_dbml(schema)
        assert 'Ref "fk_user": "app"."orders"."user_id" > "auth"."users"."id"' in dbml

    def test_multi_col_fk(self):
        fk = ForeignKey(
            name="fk_multi",
            columns=["a", "b"],
            ref_schema="other",
            ref_table="t2",
            ref_columns=["x", "y"],
        )
        schema = _schema("s", tables=[_table("t1", foreign_keys=[fk])])
        dbml = schema_to_dbml(schema)
        assert 'Ref "fk_multi": "s"."t1".("a", "b") > "other"."t2".("x", "y")' in dbml

    def test_unnamed_fk(self):
        fk = ForeignKey(
            name=None,
            columns=["org_id"],
            ref_schema="orgs",
            ref_table="orgs",
            ref_columns=["id"],
        )
        schema = _schema("s", tables=[_table("t", foreign_keys=[fk])])
        dbml = schema_to_dbml(schema)
        assert "Ref: " in dbml
        assert '"s"."t"."org_id" > "orgs"."orgs"."id"' in dbml

    def test_fk_appears_after_table_block(self):
        fk = ForeignKey(
            name="fk_x",
            columns=["c"],
            ref_schema="other",
            ref_table="t2",
            ref_columns=["id"],
        )
        schema = _schema("s", tables=[_table("t1", foreign_keys=[fk])])
        dbml = schema_to_dbml(schema)
        table_pos = dbml.index('Table "s"."t1"')
        ref_pos = dbml.index("Ref")
        assert ref_pos > table_pos


class TestNotes:
    def test_table_comment_in_note(self):
        schema = _schema("s", tables=[_table("t", comment="all orders")])
        dbml = schema_to_dbml(schema)
        assert "Note: 'all orders'" in dbml

    def test_table_comment_escaped(self):
        schema = _schema("s", tables=[_table("t", comment="it's here")])
        dbml = schema_to_dbml(schema)
        assert "Note: 'it\\'s here'" in dbml

    def test_tags_in_note_by_default(self):
        schema = _schema("s", tables=[_table("t", tags={"env": "prod"})])
        dbml = schema_to_dbml(schema)
        assert "tags: env=prod" in dbml

    def test_no_tags_suppresses_tags(self):
        schema = _schema("s", tables=[_table("t", tags={"env": "prod"})])
        dbml = schema_to_dbml(schema, include_tags=False)
        assert "tags:" not in dbml

    def test_comment_and_tags_combined_in_note(self):
        schema = _schema("s", tables=[_table("t", comment="my table", tags={"tier": "gold"})])
        dbml = schema_to_dbml(schema)
        assert "my table" in dbml
        assert "tags: tier=gold" in dbml

    def test_no_note_when_no_comment_no_tags(self):
        schema = _schema("s", tables=[_table("t")])
        dbml = schema_to_dbml(schema)
        assert "Note:" not in dbml

    def test_no_note_when_no_tags_suppressed(self):
        schema = _schema("s", tables=[_table("t", tags={"k": "v"})])
        dbml = schema_to_dbml(schema, include_tags=False)
        assert "Note:" not in dbml


class TestSchemaToDbml:
    def test_empty_schema_returns_empty_string(self):
        schema = _schema("s", tables=[])
        assert schema_to_dbml(schema) == ""

    def test_output_ends_with_newline(self):
        schema = _schema("s", tables=[_table("t")])
        assert schema_to_dbml(schema).endswith("\n")

    def test_multiple_tables_all_present(self):
        schema = _schema("s", tables=[_table("t1"), _table("t2")])
        dbml = schema_to_dbml(schema)
        assert '"s"."t1"' in dbml
        assert '"s"."t2"' in dbml


class TestSchemasToDbml:
    def test_single_schema_no_table_group(self):
        schema = _schema("s", tables=[_table("t")])
        dbml = schemas_to_dbml([schema])
        assert "TableGroup" not in dbml

    def test_multiple_schemas_have_table_groups(self):
        s1 = _schema("s1", tables=[_table("t1")])
        s2 = _schema("s2", tables=[_table("t2")])
        dbml = schemas_to_dbml([s1, s2])
        assert 'TableGroup "s1"' in dbml
        assert 'TableGroup "s2"' in dbml

    def test_table_group_contains_table_ref(self):
        s1 = _schema("s1", tables=[_table("orders")])
        s2 = _schema("s2", tables=[_table("users")])
        dbml = schemas_to_dbml([s1, s2])
        assert '"s1"."orders"' in dbml
        assert '"s2"."users"' in dbml

    def test_empty_schemas_skipped(self):
        s1 = _schema("s1", tables=[])
        s2 = _schema("s2", tables=[_table("t")])
        dbml = schemas_to_dbml([s1, s2])
        # Only one non-empty schema → no TableGroup
        assert "TableGroup" not in dbml
        assert '"s2"."t"' in dbml

    def test_all_empty_returns_empty_string(self):
        assert schemas_to_dbml([_schema("s1"), _schema("s2")]) == ""

    def test_refs_present_in_combined_output(self):
        fk = ForeignKey(
            name="fk_x", columns=["c"], ref_schema="s2", ref_table="t2", ref_columns=["id"]
        )
        s1 = _schema("s1", tables=[_table("t1", foreign_keys=[fk])])
        s2 = _schema("s2", tables=[_table("t2")])
        dbml = schemas_to_dbml([s1, s2])
        assert "Ref" in dbml
        assert '"s1"."t1"."c" > "s2"."t2"."id"' in dbml
