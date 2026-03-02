from __future__ import annotations

try:
    from pydbml import Database
    from pydbml.classes import Column as PyColumn
    from pydbml.classes import Index as PyIndex
    from pydbml.classes import Reference as PyReference
    from pydbml.classes import Table as PyTable
    from pydbml.classes import TableGroup as PyTableGroup
except ImportError as exc:
    raise ImportError(
        "pydbml is required for DBML output. Install it with: pip install 'databricks-schema[dbml]'"
    ) from exc

from databricks_schema.models import ForeignKey, Schema, Table


def _build_py_table(schema_name: str, table: Table, include_tags: bool) -> PyTable:
    """Convert a Schema Table to a pydbml Table object."""
    single_pk_col: str | None = None
    if table.primary_key and len(table.primary_key.columns) == 1:
        single_pk_col = table.primary_key.columns[0]

    note_parts: list[str] = []
    if table.comment:
        note_parts.append(table.comment)
    if include_tags and table.tags:
        note_parts.append("tags: " + ", ".join(f"{k}={v}" for k, v in table.tags.items()))

    py_table = PyTable(
        name=table.name,
        schema=schema_name,
        note="\n".join(note_parts) if note_parts else None,
    )

    for col in table.columns:
        py_table.add_column(
            PyColumn(
                name=col.name,
                type=col.data_type,
                pk=col.name == single_pk_col,
                not_null=not col.nullable,
                note=col.comment or None,
            )
        )

    # Composite PK via Index block
    if table.primary_key and len(table.primary_key.columns) > 1:
        pk = table.primary_key
        py_table.add_index(
            PyIndex(
                subjects=[py_table[c] for c in pk.columns],
                pk=True,
                name=pk.name or None,
            )
        )

    return py_table


def _add_refs(
    db: Database,
    src_table: PyTable,
    fks: list[ForeignKey],
    table_registry: dict[str, PyTable],
) -> None:
    """Build and add pydbml Reference objects to the database for all FKs of a table.

    For referenced tables not already in the registry (cross-schema refs), a placeholder
    PyTable is created to satisfy pydbml's requirement that columns know their parent table.
    Placeholders are not added to the database so they do not appear in the DBML output.
    Source FK columns that are not yet on src_table are also added as placeholders.
    """
    for fk in fks:
        existing_src_cols = {c.name for c in src_table.columns}
        for col_name in fk.columns:
            if col_name not in existing_src_cols:
                src_table.add_column(PyColumn(name=col_name, type="unknown"))
                existing_src_cols.add(col_name)

        ref_key = f"{fk.ref_schema}.{fk.ref_table}"
        if ref_key not in table_registry:
            placeholder = PyTable(name=fk.ref_table, schema=fk.ref_schema)
            table_registry[ref_key] = placeholder
        ref_table = table_registry[ref_key]
        existing_cols = {c.name for c in ref_table.columns}
        for col_name in fk.ref_columns:
            if col_name not in existing_cols:
                ref_table.add_column(PyColumn(name=col_name, type="unknown"))
                existing_cols.add(col_name)
        db.add(
            PyReference(
                type=">",
                col1=[src_table[c] for c in fk.columns],
                col2=[ref_table[c] for c in fk.ref_columns],
                name=fk.name or None,
            )
        )


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
    if not schema.tables:
        return ""

    db = Database()
    table_registry: dict[str, PyTable] = {}

    for table in schema.tables:
        py_table = _build_py_table(schema.name, table, include_tags)
        db.add(py_table)
        table_registry[f"{schema.name}.{table.name}"] = py_table

    for table in schema.tables:
        _add_refs(
            db, table_registry[f"{schema.name}.{table.name}"], table.foreign_keys, table_registry
        )

    return db.dbml + "\n"


def schemas_to_dbml(schemas: list[Schema], include_tags: bool = True) -> str:
    """Convert multiple schemas to a single combined DBML string.

    When more than one schema is present, a TableGroup is added per schema
    for visual organisation in tools like dbdiagram.io.

    Args:
        schemas: List of schemas to convert.
        include_tags: If True, Unity Catalog tags are appended to each table's Note.

    Returns:
        Combined DBML string.
    """
    non_empty = [s for s in schemas if s.tables]
    if not non_empty:
        return ""

    db = Database()
    table_registry: dict[str, PyTable] = {}

    for schema in non_empty:
        for table in schema.tables:
            py_table = _build_py_table(schema.name, table, include_tags)
            db.add(py_table)
            table_registry[f"{schema.name}.{table.name}"] = py_table

    for schema in non_empty:
        for table in schema.tables:
            _add_refs(
                db,
                table_registry[f"{schema.name}.{table.name}"],
                table.foreign_keys,
                table_registry,
            )

    if len(non_empty) > 1:
        for schema in non_empty:
            db.add(
                PyTableGroup(
                    name=schema.name,
                    items=[table_registry[f"{schema.name}.{t.name}"] for t in schema.tables],
                )
            )

    return db.dbml + "\n"
