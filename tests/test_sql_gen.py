from __future__ import annotations

from databricks.sdk.service.catalog import TableType

from databricks_schema.diff import SchemaDiff, diff_schemas
from databricks_schema.models import Column, ForeignKey, PrimaryKey, Schema, Table
from databricks_schema.sql_gen import schema_diff_to_sql


def _schema(name="main", comment=None, owner=None, tables=None, tags=None):
    return Schema(name=name, comment=comment, owner=owner, tables=tables or [], tags=tags or {})


def _table(
    name, comment=None, columns=None, tags=None, owner=None, primary_key=None, foreign_keys=None
):
    return Table(
        name=name,
        comment=comment,
        columns=columns or [],
        tags=tags or {},
        owner=owner,
        primary_key=primary_key,
        foreign_keys=foreign_keys or [],
    )


def _col(name, data_type="STRING", comment=None, nullable=True, tags=None):
    return Column(
        name=name, data_type=data_type, comment=comment, nullable=nullable, tags=tags or {}
    )


class TestSchemaRemoved:
    def test_creates_schema(self):
        sd = SchemaDiff(name="main", status="removed")
        stored = _schema("main")
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "CREATE SCHEMA IF NOT EXISTS `prod`.`main`;" in sql

    def test_creates_table(self):
        stored = _schema("main", tables=[_table("users", columns=[_col("id", data_type="BIGINT")])])
        sd = SchemaDiff(name="main", status="removed")
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "CREATE TABLE IF NOT EXISTS `prod`.`main`.`users`" in sql
        assert "`id` BIGINT" in sql

    def test_emits_comment(self):
        stored = _schema("main", comment="my schema")
        sd = SchemaDiff(name="main", status="removed")
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "COMMENT ON SCHEMA `prod`.`main` IS 'my schema';" in sql

    def test_emits_owner(self):
        stored = _schema("main", owner="alice@example.com")
        sd = SchemaDiff(name="main", status="removed")
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER SCHEMA `prod`.`main` SET OWNER TO `alice@example.com`;" in sql

    def test_emits_tags(self):
        stored = _schema("main", tags={"env": "prod"})
        sd = SchemaDiff(name="main", status="removed")
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "SET TAGS" in sql
        assert "'env' = 'prod'" in sql

    def test_table_owner_and_tags(self):
        stored = _schema("main", tables=[_table("users", owner="bob", tags={"tier": "gold"})])
        sd = SchemaDiff(name="main", status="removed")
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER TABLE `prod`.`main`.`users` SET OWNER TO `bob`;" in sql
        assert "SET TAGS" in sql

    def test_no_stored_schema(self):
        sd = SchemaDiff(name="main", status="removed")
        sql = schema_diff_to_sql("prod", sd, None)
        assert "CREATE SCHEMA IF NOT EXISTS `prod`.`main`;" in sql


class TestSchemaAdded:
    def test_commented_drop_by_default(self):
        sd = SchemaDiff(name="extra", status="added")
        sql = schema_diff_to_sql("prod", sd, None)
        assert "-- DROP SCHEMA `prod`.`extra` CASCADE;" in sql

    def test_real_drop_with_allow_drop(self):
        sd = SchemaDiff(name="extra", status="added")
        sql = schema_diff_to_sql("prod", sd, None, allow_drop=True)
        assert "DROP SCHEMA `prod`.`extra` CASCADE;" in sql
        assert not sql.strip().startswith("--")


class TestSchemaModified:
    def test_comment_change(self):
        stored = _schema("main", comment="new comment")
        live = _schema("main", comment="old comment")
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "COMMENT ON SCHEMA `prod`.`main` IS 'new comment';" in sql

    def test_comment_removed(self):
        stored = _schema("main", comment=None)
        live = _schema("main", comment="old comment")
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "COMMENT ON SCHEMA `prod`.`main` IS NULL;" in sql

    def test_owner_change(self):
        stored = _schema("main", owner="alice")
        live = _schema("main", owner="bob")
        sd = diff_schemas(live, stored, include_metadata=True)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER SCHEMA `prod`.`main` SET OWNER TO `alice`;" in sql

    def test_tags_set(self):
        stored = _schema("main", tags={"env": "prod"})
        live = _schema("main", tags={})
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "SET TAGS" in sql
        assert "'env' = 'prod'" in sql

    def test_tags_unset(self):
        stored = _schema("main", tags={})
        live = _schema("main", tags={"old_key": "old_val"})
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "UNSET TAGS" in sql
        assert "'old_key'" in sql

    def test_tags_set_and_unset(self):
        stored = _schema("main", tags={"env": "prod", "team": "data"})
        live = _schema("main", tags={"env": "staging", "old_key": "old_val"})
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "SET TAGS" in sql
        assert "UNSET TAGS" in sql
        assert "'old_key'" in sql

    def test_single_quote_escaped(self):
        stored = _schema("main", comment="it's a schema")
        live = _schema("main", comment="old")
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "it\\'s a schema" in sql


class TestTableRemoved:
    def test_creates_table_with_columns(self):
        stored = _schema(
            "main",
            tables=[_table("users", columns=[_col("id", "BIGINT"), _col("email", "STRING")])],
        )
        live = _schema("main", tables=[])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "CREATE TABLE IF NOT EXISTS `prod`.`main`.`users`" in sql
        assert "`id` BIGINT" in sql
        assert "`email` STRING" in sql

    def test_creates_table_with_not_null(self):
        stored = _schema(
            "main", tables=[_table("users", columns=[_col("id", "BIGINT", nullable=False)])]
        )
        live = _schema("main", tables=[])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "NOT NULL" in sql

    def test_creates_table_with_comment(self):
        stored = _schema("main", tables=[_table("orders", comment="all orders")])
        live = _schema("main", tables=[])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "COMMENT 'all orders'" in sql

    def test_table_owner_emitted(self):
        stored = _schema("main", tables=[_table("users", owner="alice")])
        live = _schema("main", tables=[])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER TABLE `prod`.`main`.`users` SET OWNER TO `alice`;" in sql

    def test_table_tags_emitted(self):
        stored = _schema("main", tables=[_table("users", tags={"pii": "true"})])
        live = _schema("main", tables=[])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER TABLE `prod`.`main`.`users` SET TAGS" in sql


class TestTableAdded:
    def test_commented_drop_by_default(self):
        stored = _schema("main", tables=[])
        live = _schema("main", tables=[_table("extra")])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "-- DROP TABLE `prod`.`main`.`extra`;" in sql

    def test_real_drop_with_allow_drop(self):
        stored = _schema("main", tables=[])
        live = _schema("main", tables=[_table("extra")])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored, allow_drop=True)
        assert "DROP TABLE `prod`.`main`.`extra`;" in sql
        assert "-- DROP TABLE" not in sql


class TestTableModified:
    def test_comment_change(self):
        stored = _schema("main", tables=[_table("users", comment="new")])
        live = _schema("main", tables=[_table("users", comment="old")])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "COMMENT ON TABLE `prod`.`main`.`users` IS 'new';" in sql

    def test_comment_removed(self):
        stored = _schema("main", tables=[_table("users", comment=None)])
        live = _schema("main", tables=[_table("users", comment="old")])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "COMMENT ON TABLE `prod`.`main`.`users` IS NULL;" in sql

    def test_owner_change(self):
        stored = _schema("main", tables=[_table("users", owner="alice")])
        live = _schema("main", tables=[_table("users", owner="bob")])
        sd = diff_schemas(live, stored, include_metadata=True)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER TABLE `prod`.`main`.`users` SET OWNER TO `alice`;" in sql

    def test_tags_change(self):
        stored = _schema("main", tables=[_table("users", tags={"env": "prod"})])
        live = _schema("main", tables=[_table("users", tags={"old": "val"})])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "SET TAGS" in sql
        assert "UNSET TAGS" in sql

    def test_primary_key_added(self):
        stored = _schema(
            "main", tables=[Table(name="t", primary_key=PrimaryKey(name="pk_t", columns=["id"]))]
        )
        live = _schema("main", tables=[Table(name="t", primary_key=None)])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ADD CONSTRAINT `pk_t` PRIMARY KEY (`id`)" in sql

    def test_primary_key_dropped(self):
        stored = _schema("main", tables=[Table(name="t", primary_key=None)])
        live = _schema(
            "main", tables=[Table(name="t", primary_key=PrimaryKey(name="pk_t", columns=["id"]))]
        )
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "DROP PRIMARY KEY IF EXISTS" in sql

    def test_primary_key_auto_name(self):
        stored = _schema(
            "main", tables=[Table(name="orders", primary_key=PrimaryKey(name=None, columns=["id"]))]
        )
        live = _schema("main", tables=[Table(name="orders", primary_key=None)])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ADD CONSTRAINT `pk_orders` PRIMARY KEY" in sql

    def test_foreign_key_added(self):
        fk = ForeignKey(
            name="fk_org",
            columns=["org_id"],
            ref_schema="orgs",
            ref_table="orgs",
            ref_columns=["id"],
        )
        stored = _schema("main", tables=[Table(name="t", foreign_keys=[fk])])
        live = _schema("main", tables=[Table(name="t", foreign_keys=[])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ADD CONSTRAINT `fk_org` FOREIGN KEY (`org_id`)" in sql
        assert "REFERENCES `prod`.`orgs`.`orgs` (`id`);" in sql

    def test_foreign_key_dropped(self):
        fk = ForeignKey(
            name="fk_org",
            columns=["org_id"],
            ref_schema="orgs",
            ref_table="orgs",
            ref_columns=["id"],
        )
        stored = _schema("main", tables=[Table(name="t", foreign_keys=[])])
        live = _schema("main", tables=[Table(name="t", foreign_keys=[fk])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "DROP FOREIGN KEY IF EXISTS (`org_id`);" in sql

    def test_foreign_key_auto_constraint_name(self):
        fk = ForeignKey(
            name=None,
            columns=["org_id", "dept_id"],
            ref_schema="orgs",
            ref_table="orgs",
            ref_columns=["id", "dept"],
        )
        stored = _schema("main", tables=[Table(name="t", foreign_keys=[fk])])
        live = _schema("main", tables=[Table(name="t", foreign_keys=[])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ADD CONSTRAINT `fk_t_org_id_dept_id` FOREIGN KEY" in sql

    def test_table_type_unsupported(self):
        stored = _schema("main", tables=[Table(name="t", table_type=TableType.MANAGED)])
        live = _schema("main", tables=[Table(name="t", table_type=TableType.EXTERNAL)])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "-- TODO: unsupported change: table_type" in sql


class TestColumnDiff:
    def test_column_removed_adds_column(self):
        stored = _schema("main", tables=[_table("t", columns=[_col("id"), _col("email")])])
        live = _schema("main", tables=[_table("t", columns=[_col("id")])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER TABLE `prod`.`main`.`t` ADD COLUMN `email` STRING;" in sql

    def test_column_removed_with_not_null(self):
        stored = _schema(
            "main", tables=[_table("t", columns=[_col("id", "BIGINT", nullable=False)])]
        )
        live = _schema("main", tables=[_table("t", columns=[])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ADD COLUMN `id` BIGINT NOT NULL;" in sql

    def test_column_removed_with_comment(self):
        stored = _schema("main", tables=[_table("t", columns=[_col("id", comment="the id")])])
        live = _schema("main", tables=[_table("t", columns=[])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "COMMENT 'the id'" in sql

    def test_column_added_commented_drop(self):
        stored = _schema("main", tables=[_table("t", columns=[_col("id")])])
        live = _schema("main", tables=[_table("t", columns=[_col("id"), _col("extra")])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "-- ALTER TABLE `prod`.`main`.`t` DROP COLUMN `extra`;" in sql

    def test_column_added_real_drop(self):
        stored = _schema("main", tables=[_table("t", columns=[_col("id")])])
        live = _schema("main", tables=[_table("t", columns=[_col("id"), _col("extra")])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored, allow_drop=True)
        assert "ALTER TABLE `prod`.`main`.`t` DROP COLUMN `extra`;" in sql
        assert "-- ALTER TABLE" not in sql

    def test_column_data_type_change(self):
        stored = _schema("main", tables=[_table("t", columns=[_col("x", data_type="STRING")])])
        live = _schema("main", tables=[_table("t", columns=[_col("x", data_type="BIGINT")])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER TABLE `prod`.`main`.`t` ALTER COLUMN `x` TYPE STRING;" in sql

    def test_column_comment_change(self):
        stored = _schema("main", tables=[_table("t", columns=[_col("x", comment="new")])])
        live = _schema("main", tables=[_table("t", columns=[_col("x", comment="old")])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER TABLE `prod`.`main`.`t` ALTER COLUMN `x` COMMENT 'new';" in sql

    def test_column_comment_removed(self):
        stored = _schema("main", tables=[_table("t", columns=[_col("x", comment=None)])])
        live = _schema("main", tables=[_table("t", columns=[_col("x", comment="old")])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER TABLE `prod`.`main`.`t` ALTER COLUMN `x` COMMENT NULL;" in sql

    def test_column_nullable_true_to_false(self):
        # stored wants NOT NULL, live has nullable
        stored = _schema("main", tables=[_table("t", columns=[_col("id", nullable=False)])])
        live = _schema("main", tables=[_table("t", columns=[_col("id", nullable=True)])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER TABLE `prod`.`main`.`t` ALTER COLUMN `id` SET NOT NULL;" in sql

    def test_column_nullable_false_to_true(self):
        # stored wants nullable, live has NOT NULL
        stored = _schema("main", tables=[_table("t", columns=[_col("id", nullable=True)])])
        live = _schema("main", tables=[_table("t", columns=[_col("id", nullable=False)])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "ALTER TABLE `prod`.`main`.`t` ALTER COLUMN `id` DROP NOT NULL;" in sql

    def test_column_tags_set(self):
        stored = _schema("main", tables=[_table("t", columns=[_col("x", tags={"pii": "true"})])])
        live = _schema("main", tables=[_table("t", columns=[_col("x", tags={})])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "SET TAGS" in sql
        assert "'pii' = 'true'" in sql

    def test_column_tags_unset(self):
        stored = _schema("main", tables=[_table("t", columns=[_col("x", tags={})])])
        live = _schema("main", tables=[_table("t", columns=[_col("x", tags={"old": "val"})])])
        sd = diff_schemas(live, stored)
        sql = schema_diff_to_sql("prod", sd, stored)
        assert "UNSET TAGS" in sql
        assert "'old'" in sql
