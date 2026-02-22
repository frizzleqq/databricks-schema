from __future__ import annotations

from pathlib import Path

from databricks_schema.diff import (
    diff_catalog_with_dir,
    diff_schemas,
)
from databricks_schema.models import Catalog, Column, ForeignKey, PrimaryKey, Schema, Table
from databricks_schema.yaml_io import schema_to_yaml


def _schema(name="main", comment=None, owner=None, tables=None, tags=None):
    return Schema(name=name, comment=comment, owner=owner, tables=tables or [], tags=tags or {})


def _table(name, comment=None, columns=None, tags=None, table_type=None, owner=None):
    return Table(
        name=name,
        comment=comment,
        columns=columns or [],
        tags=tags or {},
        table_type=table_type,
        owner=owner,
    )


def _col(name, data_type="STRING", comment=None, nullable=True, tags=None):
    return Column(
        name=name, data_type=data_type, comment=comment, nullable=nullable, tags=tags or {}
    )


class TestDiffSchemas:
    def test_no_changes(self):
        schema = _schema(tables=[_table("users", columns=[_col("id")])])
        result = diff_schemas(live=schema, stored=schema)
        assert result.status == "unchanged"
        assert not result.has_changes
        assert result.changes == []
        assert result.tables == []

    def test_comment_changed(self):
        stored = _schema(comment="old comment")
        live = _schema(comment="new comment")
        result = diff_schemas(live=live, stored=stored)
        assert result.status == "modified"
        assert len(result.changes) == 1
        assert result.changes[0].field == "comment"
        assert result.changes[0].old == "old comment"
        assert result.changes[0].new == "new comment"

    def test_owner_changed(self):
        stored = _schema(owner="alice")
        live = _schema(owner="bob")
        result = diff_schemas(live=live, stored=stored)
        assert result.status == "modified"
        assert result.changes[0].field == "owner"
        assert result.changes[0].old == "alice"
        assert result.changes[0].new == "bob"

    def test_tags_changed(self):
        stored = _schema(tags={"env": "prod"})
        live = _schema(tags={"env": "staging"})
        result = diff_schemas(live=live, stored=stored)
        assert result.status == "modified"
        assert result.changes[0].field == "tags"

    def test_table_added(self):
        stored = _schema(tables=[])
        live = _schema(tables=[_table("users")])
        result = diff_schemas(live=live, stored=stored)
        assert result.status == "modified"
        assert len(result.tables) == 1
        assert result.tables[0].name == "users"
        assert result.tables[0].status == "added"

    def test_table_removed(self):
        stored = _schema(tables=[_table("users")])
        live = _schema(tables=[])
        result = diff_schemas(live=live, stored=stored)
        assert result.status == "modified"
        assert result.tables[0].name == "users"
        assert result.tables[0].status == "removed"

    def test_table_comment_changed(self):
        stored = _schema(tables=[_table("users", comment="old")])
        live = _schema(tables=[_table("users", comment="new")])
        result = diff_schemas(live=live, stored=stored)
        assert result.status == "modified"
        t = result.tables[0]
        assert t.status == "modified"
        assert t.changes[0].field == "comment"
        assert t.changes[0].old == "old"
        assert t.changes[0].new == "new"

    def test_column_added(self):
        stored = _schema(tables=[_table("users", columns=[_col("id")])])
        live = _schema(tables=[_table("users", columns=[_col("id"), _col("email")])])
        result = diff_schemas(live=live, stored=stored)
        t = result.tables[0]
        assert t.status == "modified"
        col_diff = next(c for c in t.columns if c.name == "email")
        assert col_diff.status == "added"

    def test_column_removed(self):
        stored = _schema(tables=[_table("users", columns=[_col("id"), _col("email")])])
        live = _schema(tables=[_table("users", columns=[_col("id")])])
        result = diff_schemas(live=live, stored=stored)
        col_diff = next(c for c in result.tables[0].columns if c.name == "email")
        assert col_diff.status == "removed"

    def test_column_type_changed(self):
        stored = _schema(tables=[_table("t", columns=[_col("x", data_type="STRING")])])
        live = _schema(tables=[_table("t", columns=[_col("x", data_type="BIGINT")])])
        result = diff_schemas(live=live, stored=stored)
        col_diff = result.tables[0].columns[0]
        assert col_diff.status == "modified"
        assert col_diff.changes[0].field == "data_type"
        assert col_diff.changes[0].old == "STRING"
        assert col_diff.changes[0].new == "BIGINT"

    def test_column_nullable_changed(self):
        stored = _schema(tables=[_table("t", columns=[_col("id", nullable=True)])])
        live = _schema(tables=[_table("t", columns=[_col("id", nullable=False)])])
        result = diff_schemas(live=live, stored=stored)
        col_diff = result.tables[0].columns[0]
        assert col_diff.status == "modified"
        assert col_diff.changes[0].field == "nullable"

    def test_pk_changed(self):
        stored = _schema(tables=[Table(name="t", primary_key=None)])
        live = _schema(tables=[Table(name="t", primary_key=PrimaryKey(columns=["id"]))])
        result = diff_schemas(live=live, stored=stored)
        t = result.tables[0]
        assert t.status == "modified"
        assert any(fc.field == "primary_key" for fc in t.changes)

    def test_fk_changed(self):
        fk = ForeignKey(columns=["org_id"], ref_schema="orgs", ref_table="orgs", ref_columns=["id"])
        stored = _schema(tables=[Table(name="t", foreign_keys=[])])
        live = _schema(tables=[Table(name="t", foreign_keys=[fk])])
        result = diff_schemas(live=live, stored=stored)
        assert any(fc.field == "foreign_keys" for fc in result.tables[0].changes)


class TestDiffCatalogWithDir:
    def test_no_changes(self, tmp_path: Path):
        schema = _schema("main", tables=[_table("users", columns=[_col("id")])])
        (tmp_path / "main.yaml").write_text(schema_to_yaml(schema))
        catalog = Catalog(name="prod", schemas=[schema])
        result = diff_catalog_with_dir(catalog, tmp_path)
        assert not result.has_changes
        assert result.schemas[0].status == "unchanged"

    def test_schema_added_in_catalog(self, tmp_path: Path):
        stored = _schema("main")
        (tmp_path / "main.yaml").write_text(schema_to_yaml(stored))
        # catalog has main + extra schema not in YAML dir
        catalog = Catalog(name="prod", schemas=[stored, _schema("extra")])
        result = diff_catalog_with_dir(catalog, tmp_path)
        added = next(s for s in result.schemas if s.name == "extra")
        assert added.status == "added"

    def test_schema_names_filter_prevents_false_removed(self, tmp_path: Path):
        # Both main.yaml and raw.yaml exist, but we only compare main
        main = _schema("main")
        raw = _schema("raw")
        (tmp_path / "main.yaml").write_text(schema_to_yaml(main))
        (tmp_path / "raw.yaml").write_text(schema_to_yaml(raw))
        # catalog only contains main (as if --schema main was passed)
        catalog = Catalog(name="prod", schemas=[main])
        result = diff_catalog_with_dir(catalog, tmp_path, schema_names=frozenset({"main"}))
        # raw should NOT appear as removed
        assert not any(s.name == "raw" for s in result.schemas)
        assert not result.has_changes

    def test_default_schema_not_reported_as_added(self, tmp_path: Path):
        stored = _schema("main")
        (tmp_path / "main.yaml").write_text(schema_to_yaml(stored))
        # catalog has main + default, but no default.yaml
        catalog = Catalog(name="prod", schemas=[stored, _schema("default")])
        result = diff_catalog_with_dir(catalog, tmp_path)
        assert not any(s.name == "default" for s in result.schemas)
        assert not result.has_changes

    def test_default_schema_ignored_but_others_reported(self, tmp_path: Path):
        stored = _schema("main")
        (tmp_path / "main.yaml").write_text(schema_to_yaml(stored))
        # catalog has main + default + new_schema without YAML files
        catalog = Catalog(name="prod", schemas=[stored, _schema("default"), _schema("new_schema")])
        result = diff_catalog_with_dir(catalog, tmp_path)
        assert not any(s.name == "default" for s in result.schemas)
        new_diff = next(s for s in result.schemas if s.name == "new_schema")
        assert new_diff.status == "added"
        assert result.has_changes

    def test_ignore_added_can_be_overridden(self, tmp_path: Path):
        stored = _schema("main")
        (tmp_path / "main.yaml").write_text(schema_to_yaml(stored))
        catalog = Catalog(name="prod", schemas=[stored, _schema("default")])
        # passing empty frozenset means default is NOT ignored
        result = diff_catalog_with_dir(catalog, tmp_path, ignore_added=frozenset())
        assert any(s.name == "default" and s.status == "added" for s in result.schemas)

    def test_schema_removed_from_catalog(self, tmp_path: Path):
        stored = _schema("main")
        (tmp_path / "main.yaml").write_text(schema_to_yaml(stored))
        # catalog is empty — schema exists in YAML but not in live
        catalog = Catalog(name="prod", schemas=[])
        result = diff_catalog_with_dir(catalog, tmp_path)
        assert result.schemas[0].name == "main"
        assert result.schemas[0].status == "removed"

    def test_multiple_yaml_files(self, tmp_path: Path):
        s1 = _schema("main", comment="unchanged")
        s2 = _schema("raw", comment="old")
        (tmp_path / "main.yaml").write_text(schema_to_yaml(s1))
        (tmp_path / "raw.yaml").write_text(schema_to_yaml(s2))
        live_s2 = _schema("raw", comment="new")
        catalog = Catalog(name="prod", schemas=[s1, live_s2])
        result = diff_catalog_with_dir(catalog, tmp_path)
        main_diff = next(s for s in result.schemas if s.name == "main")
        raw_diff = next(s for s in result.schemas if s.name == "raw")
        assert main_diff.status == "unchanged"
        assert raw_diff.status == "modified"
        assert result.has_changes

    def test_empty_dir_raises(self, tmp_path: Path):
        # diff_catalog_with_dir with no yaml files → empty CatalogDiff
        catalog = Catalog(name="prod", schemas=[])
        result = diff_catalog_with_dir(catalog, tmp_path)
        assert result.schemas == []
        assert not result.has_changes
