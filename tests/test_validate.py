from __future__ import annotations

from databricks_schema.models import Column, ForeignKey, PrimaryKey, Schema, Table
from databricks_schema.validate import validate_schemas


def _col(name: str, data_type: str = "STRING") -> Column:
    return Column(name=name, data_type=data_type)


def _table(name: str, columns: list[Column] | None = None, **kwargs) -> Table:
    return Table(name=name, columns=columns or [], **kwargs)


def _schema(name: str, tables: list[Table] | None = None) -> Schema:
    return Schema(name=name, tables=tables or [])


def _schemas(*schemas: Schema) -> dict[str, Schema]:
    return {s.name: s for s in schemas}


class TestValidateClean:
    def test_empty_schemas(self):
        result = validate_schemas({})
        assert not result.has_errors
        assert result.issues == []

    def test_valid_schema_no_constraints(self):
        schema = _schema("main", [_table("users", [_col("id"), _col("email")])])
        result = validate_schemas(_schemas(schema))
        assert not result.has_errors

    def test_valid_pk(self):
        table = _table("users", [_col("id")], primary_key=PrimaryKey(columns=["id"]))
        result = validate_schemas(_schemas(_schema("main", [table])))
        assert not result.has_errors

    def test_valid_fk_cross_schema(self):
        orgs = _schema("orgs", [_table("org", [_col("id")])])
        fk = ForeignKey(columns=["org_id"], ref_schema="orgs", ref_table="org", ref_columns=["id"])
        users = _schema("main", [_table("users", [_col("id"), _col("org_id")], foreign_keys=[fk])])
        result = validate_schemas(_schemas(orgs, users))
        assert not result.has_errors


class TestDuplicateColumns:
    def test_duplicate_column_name(self):
        table = _table("t", [_col("id"), _col("id")])
        result = validate_schemas(_schemas(_schema("main", [table])))
        assert result.has_errors
        assert len(result.issues) == 1
        assert "duplicate column name" in result.issues[0].message
        assert "'id'" in result.issues[0].message

    def test_duplicate_reported_once_per_name(self):
        # three columns named "x" — only one issue (detected on second occurrence)
        table = _table("t", [_col("x"), _col("x"), _col("x")])
        result = validate_schemas(_schemas(_schema("main", [table])))
        assert result.has_errors
        # second and third "x" both trigger, so two issues
        assert len(result.issues) == 2


class TestPrimaryKeyValidation:
    def test_pk_column_missing(self):
        table = _table("t", [_col("id")], primary_key=PrimaryKey(columns=["nonexistent"]))
        result = validate_schemas(_schemas(_schema("main", [table])))
        assert result.has_errors
        assert any(
            "primary key" in i.message and "'nonexistent'" in i.message for i in result.issues
        )

    def test_pk_multiple_columns_one_missing(self):
        table = _table("t", [_col("a"), _col("b")], primary_key=PrimaryKey(columns=["a", "c"]))
        result = validate_schemas(_schemas(_schema("main", [table])))
        assert result.has_errors
        assert len(result.issues) == 1
        assert "'c'" in result.issues[0].message


class TestForeignKeyValidation:
    def test_fk_source_column_missing(self):
        fk = ForeignKey(columns=["bad_col"], ref_schema="other", ref_table="t", ref_columns=["id"])
        other = _schema("other", [_table("t", [_col("id")])])
        main = _schema("main", [_table("users", [_col("id")], foreign_keys=[fk])])
        result = validate_schemas(_schemas(other, main))
        assert result.has_errors
        assert any(
            "unknown source column" in i.message and "'bad_col'" in i.message for i in result.issues
        )

    def test_fk_ref_schema_missing(self):
        fk = ForeignKey(columns=["org_id"], ref_schema="ghost", ref_table="org", ref_columns=["id"])
        main = _schema("main", [_table("users", [_col("id"), _col("org_id")], foreign_keys=[fk])])
        result = validate_schemas(_schemas(main))
        assert result.has_errors
        assert any("unknown schema" in i.message and "'ghost'" in i.message for i in result.issues)

    def test_fk_ref_table_missing(self):
        fk = ForeignKey(
            columns=["org_id"], ref_schema="other", ref_table="ghost", ref_columns=["id"]
        )
        other = _schema("other", [_table("real_table", [_col("id")])])
        main = _schema("main", [_table("users", [_col("id"), _col("org_id")], foreign_keys=[fk])])
        result = validate_schemas(_schemas(other, main))
        assert result.has_errors
        assert any(
            "unknown table" in i.message and "other.ghost" in i.message for i in result.issues
        )

    def test_fk_ref_column_missing(self):
        fk = ForeignKey(
            columns=["org_id"], ref_schema="other", ref_table="org", ref_columns=["bad"]
        )
        other = _schema("other", [_table("org", [_col("id")])])
        main = _schema("main", [_table("users", [_col("id"), _col("org_id")], foreign_keys=[fk])])
        result = validate_schemas(_schemas(other, main))
        assert result.has_errors
        assert any("'bad'" in i.message and "other.org" in i.message for i in result.issues)

    def test_multiple_fk_issues_reported(self):
        fk = ForeignKey(
            columns=["bad_src"],
            ref_schema="ghost",
            ref_table="t",
            ref_columns=["id"],
        )
        main = _schema("main", [_table("t", [_col("id")], foreign_keys=[fk])])
        result = validate_schemas(_schemas(main))
        # bad_src missing + ghost schema missing = 2 issues
        assert len(result.issues) == 2


class TestIssueStr:
    def test_str_with_table(self):
        from databricks_schema.validate import ValidationIssue

        issue = ValidationIssue(schema="main", table="users", message="some problem")
        assert str(issue) == "main.users: some problem"

    def test_str_without_table(self):
        from databricks_schema.validate import ValidationIssue

        issue = ValidationIssue(schema="main", table=None, message="schema-level problem")
        assert str(issue) == "main: schema-level problem"
