from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from databricks_schema.models import Catalog, Column, Schema, Table
from databricks_schema.yaml_io import SchemaFormat, load_schema_dir


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
    status: str  # "added" | "removed" | "modified" | "unchanged"
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


def _grants_change(stored: dict[str, list[str]], live: dict[str, list[str]]) -> FieldChange | None:
    """Compare grants per principal, reporting only the principals whose privileges differ.

    Privilege order is ignored. A principal missing from one side is absent from that side's
    dict in the resulting FieldChange (old = stored, new = live).
    """
    old: dict[str, list[str]] = {}
    new: dict[str, list[str]] = {}
    for principal in sorted(set(stored) | set(live)):
        stored_privs = set(stored.get(principal, []))
        live_privs = set(live.get(principal, []))
        if stored_privs == live_privs:
            continue
        if stored_privs:
            old[principal] = sorted(stored_privs)
        if live_privs:
            new[principal] = sorted(live_privs)
    if not old and not new:
        return None
    return FieldChange(field="grants", old=old, new=new)


def _compare_object(
    stored: Schema | Table,
    live: Schema | Table,
    field_names: list[str],
    include_grants: bool,
) -> list[FieldChange]:
    changes = _compare_fields(stored, live, field_names)
    if include_grants:
        grants_change = _grants_change(stored.grants, live.grants)
        if grants_change is not None:
            changes.append(grants_change)
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
    live_tables: list[Table],
    stored_tables: list[Table],
    include_metadata: bool = False,
    include_grants: bool = False,
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
            changes = _compare_object(stored_table, live_table, table_fields, include_grants)
            col_diffs = _diff_columns(live_table.columns, stored_table.columns)
            if changes or col_diffs:
                diffs.append(
                    TableDiff(name=name, status="modified", changes=changes, columns=col_diffs)
                )

    return diffs


def diff_schemas(
    live: Schema, stored: Schema, include_metadata: bool = False, include_grants: bool = False
) -> SchemaDiff:
    """Compare a live Schema against a stored Schema.

    Returns a SchemaDiff describing what changed between the stored YAML state
    and the live catalog state. Grants are only compared when include_grants is True.
    """
    schema_fields = ["comment", "tags"]
    if include_metadata:
        schema_fields.insert(1, "owner")
    changes = _compare_object(stored, live, schema_fields, include_grants)
    table_diffs = _diff_tables(live.tables, stored.tables, include_metadata, include_grants)
    status = "modified" if (changes or table_diffs) else "unchanged"
    return SchemaDiff(name=live.name, status=status, changes=changes, tables=table_diffs)


def diff_table_pair(
    live: Table, stored: Table, include_metadata: bool = False, include_grants: bool = False
) -> TableDiff:
    """Compare two Tables directly, regardless of whether their names match.

    Used for `catalog.schema.table` vs `catalog.schema.table` CLI diffs, where the two
    tables being compared may live under different names/schemas/catalogs.
    """
    table_fields = ["table_type", "comment", "primary_key", "foreign_keys", "tags"]
    if include_metadata:
        table_fields.insert(2, "owner")
    changes = _compare_object(stored, live, table_fields, include_grants)
    columns = _diff_columns(live.columns, stored.columns)
    status = "modified" if (changes or columns) else "unchanged"
    return TableDiff(name=live.name, status=status, changes=changes, columns=columns)


def diff_schema_dirs(
    dir1: Path,
    dir2: Path,
    fmt1: SchemaFormat = "yaml",
    fmt2: SchemaFormat = "yaml",
    schema_names: frozenset[str] | None = None,
    include_metadata: bool = False,
    include_grants: bool = False,
) -> CatalogDiff:
    """Compare two local directories of schema files.

    dir1 is treated as the stored/baseline state (old).
    dir2 is treated as the live/current state (new).

    Schemas present in dir1 but not dir2 are reported as "removed".
    Schemas present in dir2 but not dir1 are reported as "added".
    Schemas present in both are compared with diff_schemas.

    schema_names: if set, only these schemas are loaded from either directory (matched on file
                  stem for YAML/JSON, on schema name for DBML).
    fmt1 / fmt2: file format for dir1 and dir2 respectively ("yaml", "json" or "dbml").
    """
    stored = load_schema_dir(dir1, fmt1, schema_names)
    live = load_schema_dir(dir2, fmt2, schema_names)

    schema_diffs: list[SchemaDiff] = []
    for name, stored_schema in stored.items():
        if name not in live:
            schema_diffs.append(SchemaDiff(name=name, status="removed"))
        else:
            schema_diffs.append(
                diff_schemas(live[name], stored_schema, include_metadata, include_grants)
            )
    for name in live:
        if name not in stored:
            schema_diffs.append(SchemaDiff(name=name, status="added"))

    return CatalogDiff(schemas=schema_diffs)


def diff_catalogs(
    live: Catalog,
    stored: Catalog,
    ignore_added: frozenset[str] = frozenset({"default"}),
    schema_names: frozenset[str] | None = None,
    include_metadata: bool = False,
    include_grants: bool = False,
) -> CatalogDiff:
    """Compare two live Catalogs directly (no local files involved).

    live is treated as the actual/current state (the primary `catalog` CLI argument).
    stored is treated as the baseline/reference state (the second catalog).

    Schemas present in stored but not live are reported as "removed".
    Schemas present in live but not stored are reported as "added", unless their name
    is in ignore_added (default: {"default"}).

    schema_names: if set, only schemas whose name is in this set are compared from either catalog.
    """
    stored_map = {
        s.name: s for s in stored.schemas if schema_names is None or s.name in schema_names
    }
    live_map = {s.name: s for s in live.schemas if schema_names is None or s.name in schema_names}

    schema_diffs: list[SchemaDiff] = []
    for name, stored_schema in stored_map.items():
        if name not in live_map:
            schema_diffs.append(SchemaDiff(name=name, status="removed"))
        else:
            schema_diffs.append(
                diff_schemas(live_map[name], stored_schema, include_metadata, include_grants)
            )

    for name in live_map:
        if name not in stored_map and name not in ignore_added:
            schema_diffs.append(SchemaDiff(name=name, status="added"))

    return CatalogDiff(schemas=schema_diffs)


def diff_catalog_with_dir(
    catalog: Catalog,
    schema_dir: Path,
    ignore_added: frozenset[str] = frozenset({"default"}),
    schema_names: frozenset[str] | None = None,
    fmt: SchemaFormat = "yaml",
    include_metadata: bool = False,
    include_grants: bool = False,
) -> CatalogDiff:
    """Compare a Catalog against schema files in schema_dir.

    For each file in schema_dir (limited to schema_names if provided):
      - If the schema exists in the catalog: compare them.
      - If the schema is missing from the catalog: report as removed.

    For each schema in the catalog without a file: report as added,
    unless its name is in ignore_added (default: {"default"}).

    schema_names: if set, only these schemas are loaded (matched on file stem for YAML/JSON,
                  on schema name for DBML). Use this when comparing a subset of schemas to
                  avoid reporting unrelated schemas as removed.
    fmt: file format to read ("yaml", "json" or "dbml").
    """
    stored = load_schema_dir(schema_dir, fmt, schema_names)

    live = {s.name: s for s in catalog.schemas}
    schema_diffs: list[SchemaDiff] = []

    for name, stored_schema in stored.items():
        if name not in live:
            schema_diffs.append(SchemaDiff(name=name, status="removed"))
        else:
            schema_diffs.append(
                diff_schemas(live[name], stored_schema, include_metadata, include_grants)
            )

    for name in live:
        if name not in stored and name not in ignore_added:
            schema_diffs.append(SchemaDiff(name=name, status="added"))

    return CatalogDiff(schemas=schema_diffs)
