"""Read and write schemas as DBML (https://dbml.dbdiagram.io/docs/).

Mapping between the models and DBML:

- Catalog -> ``Project "<catalog>"`` (catalog comment in the project ``Note``)
- Schema  -> ``TableGroup "<schema>"`` listing its tables (schema comment in its ``note``);
  emitted for every schema so empty schemas survive a round trip
- Table   -> ``Table "<schema>"."<table>"`` with a ``Note`` for the table comment
- Column  -> ``"<name>" <type> [not null, note: '...']``; types that are not a plain identifier
  (``struct<...>``, ``map<...>``, ``decimal(10, 2)`` ...) are double-quoted
- PrimaryKey -> ``indexes { ("a", "b") [pk, name: '...'] }``
- ForeignKey -> ``Ref "<name>": "s"."t".("a") > "rs"."rt".("x")``

DBML has no place for key/value metadata, so tags, owner, table_type and storage_location are
appended to the relevant note as trailing ``@key: value`` lines (tags as a JSON object)::

    Note: '''Orders placed by customers

    @table_type: MANAGED
    @tags: {"pii": "false"}'''

Parsing uses PyDBML's grammar but resolves tables, columns and references itself, so references
to tables outside the file (e.g. a foreign key into a schema that was not extracted) are kept
rather than rejected. Hand-written DBML is accepted too: unqualified tables land in schema
``public``, column ``pk`` settings and inline ``ref:`` settings are understood, and constructs
with no Unity Catalog equivalent (enums, sticky notes, non-PK indexes, defaults, ``<>`` refs,
TableGroups that do not match a schema) are ignored.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import pyparsing as pp
from pydbml.definitions.common import comment as dbml_comment
from pydbml.definitions.enum import enum as dbml_enum
from pydbml.definitions.project import project as dbml_project
from pydbml.definitions.reference import ref as dbml_ref
from pydbml.definitions.sticky_note import sticky_note as dbml_sticky_note
from pydbml.definitions.table import table as dbml_table
from pydbml.definitions.table_group import table_group as dbml_table_group
from pydbml.parser.blueprints import (
    ExpressionBlueprint,
    NoteBlueprint,
    ProjectBlueprint,
    ReferenceBlueprint,
    TableBlueprint,
    TableGroupBlueprint,
)

from databricks_schema.models import Catalog, Column, ForeignKey, PrimaryKey, Schema, Table

logger = logging.getLogger(__name__)

_INDENT = "    "
_PLAIN_TYPE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\(\d+(,\d+)*\))?$")
_META_LINE = re.compile(r"^@(owner|table_type|storage_location|tags): ?(.*)$")
_DEFAULT_SCHEMA = "public"


class DbmlParseError(ValueError):
    """Raised when DBML text cannot be parsed into the schema models."""


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def _ident(name: str) -> str:
    if '"' in name:
        raise ValueError(f"Cannot write identifier containing a double quote to DBML: {name!r}")
    return f'"{name}"'


def _string(text: str) -> str:
    escaped = text.replace("\\", "\\\\").replace("'", "\\'")
    if "\n" in text:
        return f"'''{escaped}'''"
    return f"'{escaped}'"


def _type(data_type: str) -> str:
    if _PLAIN_TYPE.match(data_type):
        return data_type
    return _ident(data_type)


def _col_list(table_ref: str, columns: list[str]) -> str:
    if len(columns) == 1:
        return f"{table_ref}.{_ident(columns[0])}"
    return f"{table_ref}.({', '.join(_ident(c) for c in columns)})"


def _note_text(comment: str | None, meta: dict[str, Any]) -> str | None:
    """Combine a comment and metadata into note text; None when there is nothing to write."""
    meta_lines = []
    for key, value in meta.items():
        if value is None or value == {}:
            continue
        if key == "tags":
            value = json.dumps(value, ensure_ascii=False, sort_keys=False)
        meta_lines.append(f"@{key}: {value}")
    if not meta_lines:
        return comment
    if comment:
        return comment + "\n\n" + "\n".join(meta_lines)
    return "\n".join(meta_lines)


def _write_column(col: Column) -> str:
    settings = []
    if not col.nullable:
        settings.append("not null")
    note = _note_text(col.comment, {"tags": col.tags})
    if note is not None:
        settings.append(f"note: {_string(note)}")
    line = f"{_INDENT}{_ident(col.name)} {_type(col.data_type)}"
    if settings:
        line += f" [{', '.join(settings)}]"
    return line


def _write_table(schema_name: str, table: Table) -> list[str]:
    lines = [f"Table {_ident(schema_name)}.{_ident(table.name)} {{"]
    lines.extend(_write_column(c) for c in table.columns)
    if table.primary_key is not None:
        pk = table.primary_key
        settings = ["pk"]
        if pk.name is not None:
            settings.append(f"name: {_string(pk.name)}")
        subjects = ", ".join(_ident(c) for c in pk.columns)
        lines += ["", f"{_INDENT}indexes {{", f"{_INDENT * 2}({subjects}) [{', '.join(settings)}]"]
        lines.append(f"{_INDENT}}}")
    note = _note_text(
        table.comment,
        {
            "owner": table.owner,
            "table_type": table.table_type.value if table.table_type else None,
            "storage_location": table.storage_location,
            "tags": table.tags,
        },
    )
    if note is not None:
        lines += ["", f"{_INDENT}Note: {_string(note)}"]
    lines.append("}")
    return lines


def _write_fk(schema_name: str, table: Table, fk: ForeignKey) -> str:
    head = f"Ref {_ident(fk.name)}:" if fk.name is not None else "Ref:"
    src = _col_list(f"{_ident(schema_name)}.{_ident(table.name)}", fk.columns)
    dst = _col_list(f"{_ident(fk.ref_schema)}.{_ident(fk.ref_table)}", fk.ref_columns)
    return f"{head} {src} > {dst}"


def _write_schema_blocks(schema: Schema) -> list[str]:
    blocks: list[str] = []
    note = _note_text(schema.comment, {"owner": schema.owner, "tags": schema.tags})
    head = f"TableGroup {_ident(schema.name)}"
    if note is not None:
        head += f" [note: {_string(note)}]"
    group = [head + " {"]
    group += [f"{_INDENT}{_ident(schema.name)}.{_ident(t.name)}" for t in schema.tables]
    group.append("}")
    blocks.append("\n".join(group))
    for table in schema.tables:
        blocks.append("\n".join(_write_table(schema.name, table)))
    refs = [_write_fk(schema.name, t, fk) for t in schema.tables for fk in t.foreign_keys]
    if refs:
        blocks.append("\n".join(refs))
    return blocks


def schema_to_dbml(schema: Schema) -> str:
    """Render a single schema as DBML (no Project block)."""
    return "\n\n".join(_write_schema_blocks(schema)) + "\n"


def catalog_to_dbml(catalog: Catalog) -> str:
    """Render a whole catalog as one DBML document."""
    project = [f"Project {_ident(catalog.name)} {{", f"{_INDENT}database_type: 'Databricks'"]
    note = _note_text(catalog.comment, {"tags": catalog.tags})
    if note is not None:
        project.append(f"{_INDENT}Note: {_string(note)}")
    project.append("}")
    blocks = ["\n".join(project)]
    for schema in catalog.schemas:
        blocks.extend(_write_schema_blocks(schema))
    return "\n\n".join(blocks) + "\n"


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------

_SYNTAX = (
    (dbml_table | dbml_ref | dbml_enum | dbml_table_group | dbml_project | dbml_sticky_note)[...]
    + ("\n" | dbml_comment)[...]
    + pp.StringEnd()
)


@dataclass
class _Note:
    comment: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)


def _parse_note(note: NoteBlueprint | None) -> _Note:
    """Split note text into a comment and trailing ``@key: value`` metadata lines."""
    if note is None:
        return _Note()
    lines = note.text.split("\n")
    meta: dict[str, Any] = {}
    while lines:
        m = _META_LINE.match(lines[-1])
        if m is None:
            break
        key, value = m.group(1), m.group(2)
        if key == "tags":
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as e:
                raise DbmlParseError(f"Invalid @tags JSON in note: {value!r}") from e
            if not isinstance(parsed, dict):
                raise DbmlParseError(f"@tags must be a JSON object, got: {value!r}")
            meta[key] = {str(k): str(v) for k, v in parsed.items()}
        else:
            meta[key] = value
        lines.pop()
    if meta:
        if lines and lines[-1] == "":
            lines.pop()
        comment = "\n".join(lines) if lines else None
    else:
        comment = note.text
    return _Note(comment=comment, meta=meta)


def _split_cols(cols: Any) -> list[str]:
    """Normalise a ref side's columns: 'a' or '(a, b)' (PyDBML's composite form) or a list."""
    if isinstance(cols, str):
        if cols.startswith("(") and cols.endswith(")"):
            return [c.strip() for c in cols[1:-1].split(",")]
        return [cols]
    return [str(c) for c in cols]


@dataclass
class _Parsed:
    project: ProjectBlueprint | None
    schemas: list[Schema]


def _parse(text: str) -> _Parsed:
    try:
        tokens = _SYNTAX.parse_string(text.lstrip("﻿"), parse_all=True)
    except pp.ParseBaseException as e:
        raise DbmlParseError(f"Invalid DBML at line {e.lineno}, column {e.col}: {e.msg}") from e

    project: ProjectBlueprint | None = None
    table_bps: list[TableBlueprint] = []
    ref_bps: list[ReferenceBlueprint] = []
    group_bps: list[TableGroupBlueprint] = []
    for bp in tokens:
        if isinstance(bp, TableBlueprint):
            table_bps.append(bp)
            ref_bps.extend(bp.get_reference_blueprints())
        elif isinstance(bp, ReferenceBlueprint):
            ref_bps.append(bp)
        elif isinstance(bp, TableGroupBlueprint):
            group_bps.append(bp)
        elif isinstance(bp, ProjectBlueprint):
            project = bp

    aliases = {bp.alias: (bp.schema, bp.name) for bp in table_bps if bp.alias}

    schemas: dict[str, Schema] = {}

    def _schema(name: str) -> Schema:
        if name not in schemas:
            schemas[name] = Schema(name=name)
        return schemas[name]

    # A TableGroup describes a schema when it is named after one and only holds its tables
    # (or is empty). Other groups are diagram-only groupings and are ignored.
    for group in group_bps:
        if not all(item.startswith(f"{group.name}.") for item in group.items):
            continue
        note = _parse_note(group.note)
        s = _schema(group.name)
        s.comment = note.comment
        s.owner = note.meta.get("owner")
        s.tags = note.meta.get("tags", {})

    tables: dict[tuple[str, str], Table] = {}
    for bp in table_bps:
        note = _parse_note(bp.note)
        columns: list[Column] = []
        pk_flag_cols: list[str] = []
        for col in bp.columns or []:
            col_note = _parse_note(col.note)
            columns.append(
                Column(
                    name=col.name,
                    data_type=col.type,
                    comment=col_note.comment,
                    nullable=not (col.not_null or col.pk),
                    tags=col_note.meta.get("tags", {}),
                )
            )
            if col.pk:
                pk_flag_cols.append(col.name)
        primary_key: PrimaryKey | None = None
        for idx in bp.indexes or []:
            if idx.pk:
                cols = [s for s in idx.subject_names if not isinstance(s, ExpressionBlueprint)]
                primary_key = PrimaryKey(name=idx.name, columns=[str(c) for c in cols])
                break
        if primary_key is None and pk_flag_cols:
            primary_key = PrimaryKey(columns=pk_flag_cols)
        if primary_key is not None:
            pk_set = set(primary_key.columns)
            for c in columns:
                if c.name in pk_set:
                    c.nullable = False
        table = Table(
            name=bp.name,
            table_type=note.meta.get("table_type"),
            comment=note.comment,
            owner=note.meta.get("owner"),
            tags=note.meta.get("tags", {}),
            storage_location=note.meta.get("storage_location"),
            columns=columns,
            primary_key=primary_key,
        )
        _schema(bp.schema).tables.append(table)
        tables[(bp.schema, bp.name)] = table

    def _resolve(schema: str, table: str) -> tuple[str, str]:
        if schema == _DEFAULT_SCHEMA and table in aliases and (schema, table) not in tables:
            return aliases[table]
        return schema, table

    for ref in ref_bps:
        if ref.type == "<>":
            logger.warning("Skipping many-to-many DBML ref %s (no Unity Catalog equivalent)", ref)
            continue
        side1 = (*_resolve(ref.schema1, ref.table1), _split_cols(ref.col1))
        side2 = (*_resolve(ref.schema2, ref.table2), _split_cols(ref.col2))
        src, dst = (side2, side1) if ref.type == "<" else (side1, side2)
        owner_table = tables.get((src[0], src[1]))
        if owner_table is None:
            logger.warning(
                "Skipping DBML ref from %s.%s: table not defined in this file", src[0], src[1]
            )
            continue
        owner_table.foreign_keys.append(
            ForeignKey(
                name=ref.name,
                columns=src[2],
                ref_schema=dst[0],
                ref_table=dst[1],
                ref_columns=dst[2],
            )
        )

    return _Parsed(project=project, schemas=list(schemas.values()))


def schemas_from_dbml(text: str) -> list[Schema]:
    """Parse every schema defined in a DBML document (Project block optional)."""
    return _parse(text).schemas


def schema_from_dbml(text: str) -> Schema:
    """Parse a DBML document that defines exactly one schema."""
    schemas = schemas_from_dbml(text)
    if len(schemas) != 1:
        names = ", ".join(s.name for s in schemas) or "none"
        raise DbmlParseError(f"Expected exactly one schema in DBML, found {len(schemas)}: {names}")
    return schemas[0]


def catalog_from_dbml(text: str) -> Catalog:
    """Parse a DBML document into a Catalog; the catalog name comes from its Project block."""
    parsed = _parse(text)
    if parsed.project is None:
        raise DbmlParseError("DBML has no Project block, so the catalog name is unknown")
    note = _parse_note(parsed.project.note)
    return Catalog(
        name=parsed.project.name,
        comment=note.comment,
        tags=note.meta.get("tags", {}),
        schemas=parsed.schemas,
    )
