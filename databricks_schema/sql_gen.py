from __future__ import annotations

from databricks_schema.diff import ColumnDiff, SchemaDiff, TableDiff
from databricks_schema.models import Column, ForeignKey, PrimaryKey, Schema, Table


def _esc(s: str) -> str:
    """Escape single quotes in a string for SQL."""
    return s.replace("'", "\\'")


def _q(name: str) -> str:
    """Backtick-quote a SQL identifier."""
    return f"`{name}`"


def _schema_ref(catalog: str, schema: str) -> str:
    return f"{_q(catalog)}.{_q(schema)}"


def _table_ref(catalog: str, schema: str, table: str) -> str:
    return f"{_q(catalog)}.{_q(schema)}.{_q(table)}"


def _column_def(col: Column) -> str:
    """Generate a column definition fragment: `name` TYPE [NOT NULL] [COMMENT '...']"""
    parts = [_q(col.name), col.data_type]
    if not col.nullable:
        parts.append("NOT NULL")
    if col.comment:
        parts.append(f"COMMENT '{_esc(col.comment)}'")
    return " ".join(parts)


def _tags_set(tags: dict[str, str]) -> str:
    items = ", ".join(f"'{_esc(k)}' = '{_esc(v)}'" for k, v in tags.items())
    return f"({items})"


def _tags_unset(keys: list[str]) -> str:
    items = ", ".join(f"'{_esc(k)}'" for k in keys)
    return f"({items})"


def _tag_stmts(prefix: str, old_tags: dict[str, str], new_tags: dict[str, str]) -> list[str]:
    """Generate SET TAGS / UNSET TAGS statements.

    old_tags = stored/target state, new_tags = live/current state.
    Produces SET for keys that need to be added or updated, UNSET for keys to remove.
    """
    to_set = {k: v for k, v in old_tags.items() if k not in new_tags or new_tags[k] != v}
    to_unset = [k for k in new_tags if k not in old_tags]
    stmts: list[str] = []
    if to_set:
        stmts.append(f"{prefix} SET TAGS {_tags_set(to_set)};")
    if to_unset:
        stmts.append(f"{prefix} UNSET TAGS {_tags_unset(to_unset)};")
    return stmts


def _pk_stmts(
    tref: str, table_name: str, old_pk: PrimaryKey | None, new_pk: PrimaryKey | None
) -> list[str]:
    """Generate DROP / ADD PRIMARY KEY statements.

    old_pk = stored/target, new_pk = live/current.
    """
    stmts: list[str] = []
    if new_pk is not None:
        stmts.append(f"ALTER TABLE {tref} DROP PRIMARY KEY IF EXISTS;")
    if old_pk is not None:
        cols = ", ".join(_q(c) for c in old_pk.columns)
        constraint_name = old_pk.name or f"pk_{table_name}"
        stmts.append(
            f"ALTER TABLE {tref} ADD CONSTRAINT {_q(constraint_name)} PRIMARY KEY ({cols});"
        )
    return stmts


def _fk_stmts(
    tref: str,
    catalog: str,
    table_name: str,
    old_fks: list[ForeignKey],
    new_fks: list[ForeignKey],
) -> list[str]:
    """Generate DROP / ADD FOREIGN KEY statements.

    FKs are matched by frozenset of their column names.
    old_fks = stored/target, new_fks = live/current.
    """
    old_by_cols = {frozenset(fk.columns): fk for fk in old_fks}
    new_by_cols = {frozenset(fk.columns): fk for fk in new_fks}

    stmts: list[str] = []
    # FKs to drop: in live (new) but not in stored (old)
    for cols_key, fk in new_by_cols.items():
        if cols_key not in old_by_cols:
            cols_list = ", ".join(_q(c) for c in fk.columns)
            stmts.append(f"ALTER TABLE {tref} DROP FOREIGN KEY IF EXISTS ({cols_list});")

    # FKs to add: in stored (old) but not in live (new)
    for cols_key, fk in old_by_cols.items():
        if cols_key not in new_by_cols:
            fk_cols = ", ".join(_q(c) for c in fk.columns)
            ref_cols = ", ".join(_q(c) for c in fk.ref_columns)
            constraint_name = fk.name or f"fk_{table_name}_{'_'.join(fk.columns)}"
            ref_tref = _table_ref(catalog, fk.ref_schema, fk.ref_table)
            stmts.append(
                f"ALTER TABLE {tref} ADD CONSTRAINT {_q(constraint_name)} "
                f"FOREIGN KEY ({fk_cols}) REFERENCES {ref_tref} ({ref_cols});"
            )

    return stmts


def _create_table(catalog: str, schema: str, table: Table) -> str:
    """Generate a CREATE TABLE IF NOT EXISTS statement with column definitions."""
    tref = _table_ref(catalog, schema, table.name)
    col_defs = ", ".join(_column_def(col) for col in table.columns)
    stmt = f"CREATE TABLE IF NOT EXISTS {tref} ({col_defs})"
    if table.comment:
        stmt += f" COMMENT '{_esc(table.comment)}'"
    stmt += ";"
    return stmt


def _col_diff_stmts(
    tref: str,
    col_diff: ColumnDiff,
    stored_col_map: dict[str, Column],
    allow_drop: bool,
) -> list[str]:
    """Generate SQL statements for a single column diff."""
    stmts: list[str] = []
    col_name = _q(col_diff.name)

    if col_diff.status == "removed":
        stored_col = stored_col_map.get(col_diff.name)
        if stored_col:
            stmts.append(f"ALTER TABLE {tref} ADD COLUMN {_column_def(stored_col)};")

    elif col_diff.status == "added":
        drop_stmt = f"ALTER TABLE {tref} DROP COLUMN {col_name};"
        if allow_drop:
            stmts.append(drop_stmt)
        else:
            stmts.append(f"-- {drop_stmt}")

    elif col_diff.status == "modified":
        for fc in col_diff.changes:
            if fc.field == "data_type":
                stmts.append(f"ALTER TABLE {tref} ALTER COLUMN {col_name} TYPE {fc.old};")
            elif fc.field == "comment":
                if fc.old is not None:
                    stmts.append(
                        f"ALTER TABLE {tref} ALTER COLUMN {col_name} COMMENT '{_esc(str(fc.old))}';"
                    )
                else:
                    stmts.append(f"ALTER TABLE {tref} ALTER COLUMN {col_name} COMMENT NULL;")
            elif fc.field == "nullable":
                if fc.old is False:
                    stmts.append(f"ALTER TABLE {tref} ALTER COLUMN {col_name} SET NOT NULL;")
                else:
                    stmts.append(f"ALTER TABLE {tref} ALTER COLUMN {col_name} DROP NOT NULL;")
            elif fc.field == "tags":
                stmts.extend(
                    _tag_stmts(
                        f"ALTER TABLE {tref} ALTER COLUMN {col_name}",
                        fc.old or {},
                        fc.new or {},
                    )
                )

    return stmts


def _table_diff_stmts(
    catalog: str,
    schema: str,
    table_diff: TableDiff,
    stored_table_map: dict[str, Table],
    allow_drop: bool,
) -> list[str]:
    """Generate SQL statements for a single table diff."""
    stmts: list[str] = []
    tref = _table_ref(catalog, schema, table_diff.name)

    if table_diff.status == "removed":
        stored_table = stored_table_map.get(table_diff.name)
        if stored_table:
            stmts.append(_create_table(catalog, schema, stored_table))
            if stored_table.owner:
                stmts.append(f"ALTER TABLE {tref} SET OWNER TO {_q(stored_table.owner)};")
            if stored_table.tags:
                stmts.append(f"ALTER TABLE {tref} SET TAGS {_tags_set(stored_table.tags)};")

    elif table_diff.status == "added":
        drop_stmt = f"DROP TABLE {tref};"
        if allow_drop:
            stmts.append(drop_stmt)
        else:
            stmts.append(f"-- {drop_stmt}")

    elif table_diff.status == "modified":
        stored_table = stored_table_map.get(table_diff.name)
        stored_col_map: dict[str, Column] = (
            {c.name: c for c in stored_table.columns} if stored_table else {}
        )

        for fc in table_diff.changes:
            if fc.field == "comment":
                if fc.old is not None:
                    stmts.append(f"COMMENT ON TABLE {tref} IS '{_esc(str(fc.old))}';")
                else:
                    stmts.append(f"COMMENT ON TABLE {tref} IS NULL;")
            elif fc.field == "owner":
                if fc.old is not None:
                    stmts.append(f"ALTER TABLE {tref} SET OWNER TO {_q(str(fc.old))};")
            elif fc.field == "tags":
                stmts.extend(_tag_stmts(f"ALTER TABLE {tref}", fc.old or {}, fc.new or {}))
            elif fc.field == "primary_key":
                stmts.extend(_pk_stmts(tref, table_diff.name, fc.old, fc.new))
            elif fc.field == "foreign_keys":
                stmts.extend(_fk_stmts(tref, catalog, table_diff.name, fc.old or [], fc.new or []))
            elif fc.field == "table_type":
                stmts.append(f"-- TODO: unsupported change: table_type {fc.new!r} -> {fc.old!r}")

        for col_diff in table_diff.columns:
            stmts.extend(_col_diff_stmts(tref, col_diff, stored_col_map, allow_drop))

    return stmts


def schema_diff_to_sql(
    catalog_name: str,
    schema_diff: SchemaDiff,
    stored_schema: Schema | None,
    allow_drop: bool = False,
) -> str:
    """Generate Databricks Spark SQL to bring the live catalog in line with the stored schema.

    diff_schemas(live, stored) returns FieldChange(old=stored_value, new=live_value).
    SQL must transform live → stored, so the target is always fc.old.

    Args:
        catalog_name: Catalog name for fully-qualified references.
        schema_diff: Diff result from diff_schemas() or a manually constructed SchemaDiff.
        stored_schema: The stored (local) Schema; required for "removed" and "modified" status.
        allow_drop: If True, emit real DROP statements; otherwise emit commented-out versions.

    Returns:
        SQL string with statements separated by newlines.
    """
    stmts: list[str] = []
    sref = _schema_ref(catalog_name, schema_diff.name)

    if schema_diff.status == "removed":
        # Schema is in stored (local) but not in live → CREATE it
        stmts.append(f"CREATE SCHEMA IF NOT EXISTS {sref};")
        if stored_schema is not None:
            if stored_schema.comment:
                stmts.append(f"COMMENT ON SCHEMA {sref} IS '{_esc(stored_schema.comment)}';")
            if stored_schema.owner:
                stmts.append(f"ALTER SCHEMA {sref} SET OWNER TO {_q(stored_schema.owner)};")
            if stored_schema.tags:
                stmts.append(f"ALTER SCHEMA {sref} SET TAGS {_tags_set(stored_schema.tags)};")
            for table in stored_schema.tables:
                stmts.append(_create_table(catalog_name, schema_diff.name, table))
                tref = _table_ref(catalog_name, schema_diff.name, table.name)
                if table.owner:
                    stmts.append(f"ALTER TABLE {tref} SET OWNER TO {_q(table.owner)};")
                if table.tags:
                    stmts.append(f"ALTER TABLE {tref} SET TAGS {_tags_set(table.tags)};")

    elif schema_diff.status == "added":
        # Schema is in live but not in stored → DROP it (commented by default)
        drop_stmt = f"DROP SCHEMA {sref} CASCADE;"
        if allow_drop:
            stmts.append(drop_stmt)
        else:
            stmts.append(f"-- {drop_stmt}")

    elif schema_diff.status == "modified":
        stored_table_map: dict[str, Table] = (
            {t.name: t for t in stored_schema.tables} if stored_schema else {}
        )

        for fc in schema_diff.changes:
            if fc.field == "comment":
                if fc.old is not None:
                    stmts.append(f"COMMENT ON SCHEMA {sref} IS '{_esc(str(fc.old))}';")
                else:
                    stmts.append(f"COMMENT ON SCHEMA {sref} IS NULL;")
            elif fc.field == "owner":
                if fc.old is not None:
                    stmts.append(f"ALTER SCHEMA {sref} SET OWNER TO {_q(str(fc.old))};")
            elif fc.field == "tags":
                stmts.extend(_tag_stmts(f"ALTER SCHEMA {sref}", fc.old or {}, fc.new or {}))

        for table_diff in schema_diff.tables:
            stmts.extend(
                _table_diff_stmts(
                    catalog_name, schema_diff.name, table_diff, stored_table_map, allow_drop
                )
            )

    return "\n".join(stmts)
