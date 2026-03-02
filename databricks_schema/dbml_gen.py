from __future__ import annotations

from databricks_schema.models import Column, ForeignKey, Schema, Table


def _q(name: str) -> str:
    """Double-quote a DBML identifier."""
    return f'"{name}"'


def _esc(s: str) -> str:
    """Escape single quotes for DBML note strings."""
    return s.replace("'", "\\'")


def _col_def(col: Column, is_pk: bool) -> str:
    """Generate a single DBML column definition line."""
    attrs: list[str] = []
    if is_pk:
        attrs.append("pk")
    if not col.nullable:
        attrs.append("not null")
    if col.comment:
        attrs.append(f"note: '{_esc(col.comment)}'")
    attr_str = f" [{', '.join(attrs)}]" if attrs else ""
    return f"  {_q(col.name)} {col.data_type}{attr_str}"


def _table_block(schema_name: str, table: Table, include_tags: bool) -> str:
    """Generate the full DBML Table block for a single table."""
    lines: list[str] = [f"Table {_q(schema_name)}.{_q(table.name)} {{"]

    pk_cols: set[str] = set()
    if table.primary_key and len(table.primary_key.columns) == 1:
        pk_cols = {table.primary_key.columns[0]}

    for col in table.columns:
        lines.append(_col_def(col, col.name in pk_cols))

    # Composite PK via indexes block
    if table.primary_key and len(table.primary_key.columns) > 1:
        pk = table.primary_key
        col_refs = ", ".join(_q(c) for c in pk.columns)
        name_attr = f", name: '{_esc(pk.name)}'" if pk.name else ""
        lines.append("")
        lines.append("  indexes {")
        lines.append(f"    ({col_refs}) [pk{name_attr}]")
        lines.append("  }")

    # Note: table comment + optional tags
    note_parts: list[str] = []
    if table.comment:
        note_parts.append(table.comment)
    if include_tags and table.tags:
        tag_str = ", ".join(f"{k}={v}" for k, v in table.tags.items())
        note_parts.append(f"tags: {tag_str}")
    if note_parts:
        lines.append("")
        lines.append(f"  Note: '{_esc(chr(10).join(note_parts))}'")

    lines.append("}")
    return "\n".join(lines)


def _ref_lines(schema_name: str, table_name: str, fks: list[ForeignKey]) -> list[str]:
    """Generate DBML Ref lines for all foreign keys of a table."""
    refs: list[str] = []
    for fk in fks:
        name_part = f" {_q(fk.name)}" if fk.name else ""
        if len(fk.columns) == 1:
            src = f"{_q(schema_name)}.{_q(table_name)}.{_q(fk.columns[0])}"
        else:
            col_list = ", ".join(_q(c) for c in fk.columns)
            src = f"{_q(schema_name)}.{_q(table_name)}.({col_list})"
        if len(fk.ref_columns) == 1:
            ref = f"{_q(fk.ref_schema)}.{_q(fk.ref_table)}.{_q(fk.ref_columns[0])}"
        else:
            ref_list = ", ".join(_q(c) for c in fk.ref_columns)
            ref = f"{_q(fk.ref_schema)}.{_q(fk.ref_table)}.({ref_list})"
        refs.append(f"Ref{name_part}: {src} > {ref}")
    return refs


def schema_to_dbml(schema: Schema, include_tags: bool = True) -> str:
    """Convert a Schema to a DBML string.

    Emits one Table block per table, followed by Ref declarations for all foreign keys.
    Fields with no DBML equivalent (owner, storage_location, table_type) are omitted.

    Args:
        schema: The schema to convert.
        include_tags: If True, Unity Catalog tags are appended to each table's Note.

    Returns:
        DBML string, or empty string if the schema has no tables.
    """
    blocks: list[str] = []
    refs: list[str] = []

    for table in schema.tables:
        blocks.append(_table_block(schema.name, table, include_tags))
        refs.extend(_ref_lines(schema.name, table.name, table.foreign_keys))

    if not blocks:
        return ""

    all_parts = blocks + refs
    return "\n\n".join(all_parts) + "\n"


def schemas_to_dbml(schemas: list[Schema], include_tags: bool = True) -> str:
    """Convert multiple schemas to a single combined DBML string.

    When more than one schema is present, wraps each schema's tables in a
    TableGroup block for organisation in tools like dbdiagram.io.

    Args:
        schemas: List of schemas to convert.
        include_tags: If True, Unity Catalog tags are appended to each table's Note.

    Returns:
        Combined DBML string.
    """
    non_empty = [s for s in schemas if s.tables]
    if not non_empty:
        return ""

    if len(non_empty) == 1:
        return schema_to_dbml(non_empty[0], include_tags)

    parts: list[str] = []
    all_refs: list[str] = []

    for schema in non_empty:
        schema_blocks: list[str] = []
        for table in schema.tables:
            schema_blocks.append(_table_block(schema.name, table, include_tags))
            all_refs.extend(_ref_lines(schema.name, table.name, table.foreign_keys))

        # Emit table blocks for this schema
        parts.append("\n\n".join(schema_blocks))

        # TableGroup to organise by schema
        table_refs = "\n".join(f"  {_q(schema.name)}.{_q(t.name)}" for t in schema.tables)
        parts.append(f"TableGroup {_q(schema.name)} {{\n{table_refs}\n}}")

    if all_refs:
        parts.append("\n".join(all_refs))

    return "\n\n".join(parts) + "\n"
