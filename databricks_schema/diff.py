from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from databricks_schema.models import Catalog, Column, Schema, Table
from databricks_schema.yaml_io import schema_from_json, schema_from_yaml


@dataclass
class FieldChange:
    field: str
    old: Any
    new: Any


@dataclass
class ColumnDiff:
    name: str
    status: str  # "added" | "removed" | "modified"
    changes: list[FieldChange] = field(default_factory=list)


@dataclass
class TableDiff:
    name: str
    status: str  # "added" | "removed" | "modified"
    changes: list[FieldChange] = field(default_factory=list)
    columns: list[ColumnDiff] = field(default_factory=list)


@dataclass
class SchemaDiff:
    name: str
    status: str  # "added" | "removed" | "modified" | "unchanged"
    changes: list[FieldChange] = field(default_factory=list)
    tables: list[TableDiff] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return self.status != "unchanged"


@dataclass
class CatalogDiff:
    schemas: list[SchemaDiff] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return any(s.has_changes for s in self.schemas)


def _compare_fields(stored: Any, live: Any, field_names: list[str]) -> list[FieldChange]:
    changes = []
    for name in field_names:
        old = getattr(stored, name, None)
        new = getattr(live, name, None)
        if old != new:
            changes.append(FieldChange(field=name, old=old, new=new))
    return changes


def _diff_columns(live_cols: list[Column], stored_cols: list[Column]) -> list[ColumnDiff]:
    live_map = {c.name: c for c in live_cols}
    stored_map = {c.name: c for c in stored_cols}
    diffs: list[ColumnDiff] = []

    for name in stored_map:
        if name not in live_map:
            diffs.append(ColumnDiff(name=name, status="removed"))

    for name, live_col in live_map.items():
        if name not in stored_map:
            diffs.append(ColumnDiff(name=name, status="added"))
        else:
            changes = _compare_fields(
                stored_map[name], live_col, ["data_type", "comment", "nullable", "tags"]
            )
            if changes:
                diffs.append(ColumnDiff(name=name, status="modified", changes=changes))

    return diffs


def _diff_tables(
    live_tables: list[Table], stored_tables: list[Table], include_metadata: bool = False
) -> list[TableDiff]:
    live_map = {t.name: t for t in live_tables}
    stored_map = {t.name: t for t in stored_tables}
    diffs: list[TableDiff] = []

    for name in stored_map:
        if name not in live_map:
            diffs.append(TableDiff(name=name, status="removed"))

    table_fields = ["table_type", "comment", "primary_key", "foreign_keys", "tags"]
    if include_metadata:
        table_fields.insert(2, "owner")

    for name, live_table in live_map.items():
        if name not in stored_map:
            diffs.append(TableDiff(name=name, status="added"))
        else:
            stored_table = stored_map[name]
            changes = _compare_fields(stored_table, live_table, table_fields)
            col_diffs = _diff_columns(live_table.columns, stored_table.columns)
            if changes or col_diffs:
                diffs.append(
                    TableDiff(name=name, status="modified", changes=changes, columns=col_diffs)
                )

    return diffs


def diff_schemas(live: Schema, stored: Schema, include_metadata: bool = False) -> SchemaDiff:
    """Compare a live Schema against a stored Schema.

    Returns a SchemaDiff describing what changed between the stored YAML state
    and the live catalog state.
    """
    schema_fields = ["comment", "tags"]
    if include_metadata:
        schema_fields.insert(1, "owner")
    changes = _compare_fields(stored, live, schema_fields)
    table_diffs = _diff_tables(live.tables, stored.tables, include_metadata)
    status = "modified" if (changes or table_diffs) else "unchanged"
    return SchemaDiff(name=live.name, status=status, changes=changes, tables=table_diffs)


def diff_catalog_with_dir(
    catalog: Catalog,
    schema_dir: Path,
    ignore_added: frozenset[str] = frozenset({"default"}),
    schema_names: frozenset[str] | None = None,
    fmt: Literal["yaml", "json"] = "yaml",
    include_metadata: bool = False,
) -> CatalogDiff:
    """Compare a Catalog against schema files in schema_dir.

    For each file in schema_dir (limited to schema_names if provided):
      - If the schema exists in the catalog: compare them.
      - If the schema is missing from the catalog: report as removed.

    For each schema in the catalog without a file: report as added,
    unless its name is in ignore_added (default: {"default"}).

    schema_names: if set, only files whose stem is in this set are loaded.
                  Use this when comparing a subset of schemas to avoid reporting
                  unrelated schemas as removed.
    fmt: file format to read ("yaml" or "json").
    """
    ext = ".json" if fmt == "json" else ".yaml"
    loader = schema_from_json if fmt == "json" else schema_from_yaml
    stored: dict[str, Schema] = {}
    for schema_file in sorted(schema_dir.glob(f"*{ext}")):
        if schema_names is not None and schema_file.stem not in schema_names:
            continue
        schema = loader(schema_file.read_text(encoding="utf-8"))
        stored[schema.name] = schema

    live = {s.name: s for s in catalog.schemas}
    schema_diffs: list[SchemaDiff] = []

    for name, stored_schema in stored.items():
        if name not in live:
            schema_diffs.append(SchemaDiff(name=name, status="removed"))
        else:
            schema_diffs.append(diff_schemas(live[name], stored_schema, include_metadata))

    for name in live:
        if name not in stored and name not in ignore_added:
            schema_diffs.append(SchemaDiff(name=name, status="added"))

    return CatalogDiff(schemas=schema_diffs)
