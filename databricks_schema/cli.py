from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

from databricks_schema.diff import CatalogDiff, SchemaDiff, diff_catalog_with_dir, diff_schemas
from databricks_schema.extractor import CatalogExtractor
from databricks_schema.models import Schema
from databricks_schema.sql_gen import schema_diff_to_sql
from databricks_schema.yaml_io import (
    schema_from_json,
    schema_from_yaml,
    schema_to_json,
    schema_to_yaml,
)

_handler = logging.StreamHandler(sys.stderr)
_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logging.getLogger("databricks_schema").addHandler(_handler)
logging.getLogger("databricks_schema").setLevel(logging.DEBUG)


def _make_client(host: str | None, token: str | None) -> WorkspaceClient:
    kwargs = {}
    if host:
        kwargs["host"] = host
    if token:
        kwargs["token"] = token
    return WorkspaceClient(**kwargs)


def _add_connection_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--host",
        default=os.environ.get("DATABRICKS_HOST"),
        help="Databricks host URL",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("DATABRICKS_TOKEN"),
        help="Databricks access token",
    )


def _cmd_extract(args: argparse.Namespace) -> None:
    """Extract Unity Catalog schemas to YAML or JSON files."""
    client = _make_client(args.host, args.token)
    extractor = CatalogExtractor(client=client, max_workers=args.workers)

    serializer = schema_to_json if args.fmt == "json" else schema_to_yaml
    ext = ".json" if args.fmt == "json" else ".yaml"

    print(f"Extracting catalog '{args.catalog}'...", file=sys.stderr)

    if args.output_dir is None:
        schema_filter_set = set(args.schema) if args.schema else None
        matching_schemas = [
            s.name
            for s in client.schemas.list(catalog_name=args.catalog)
            if (s.name or "") not in {"information_schema"}
            and (schema_filter_set is None or (s.name or "") in schema_filter_set)
        ]
        if len(matching_schemas) != 1:
            print(
                "Error: --output-dir is required when extracting multiple schemas.",
                file=sys.stderr,
            )
            sys.exit(1)
        catalog_obj = extractor.extract_catalog(
            catalog_name=args.catalog,
            schema_filter=args.schema,
            include_metadata=args.include_metadata,
            include_tags=not args.no_tags,
        )
        print(serializer(catalog_obj.schemas[0]))
        return

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for s in extractor.iter_schemas(
        catalog_name=args.catalog,
        schema_filter=args.schema,
        include_metadata=args.include_metadata,
        include_tags=not args.no_tags,
    ):
        out_file = output_dir / f"{s.name}{ext}"
        out_file.write_text(serializer(s), encoding="utf-8")
        print(f"  Wrote {out_file}", file=sys.stderr)
        count += 1

    print(f"Done — {count} schema(s) written to {output_dir}", file=sys.stderr)


def _cmd_diff(args: argparse.Namespace) -> None:
    """Compare Unity Catalog schemas against local YAML or JSON files."""
    schema_dir: Path = args.schema_dir
    if not schema_dir.is_dir():
        print(f"Error: {schema_dir} is not a directory.", file=sys.stderr)
        sys.exit(2)

    yaml_files = list(schema_dir.glob("*.yaml"))
    json_files = list(schema_dir.glob("*.json"))
    if yaml_files and json_files:
        print("Error: mixed YAML and JSON files in schema directory.", file=sys.stderr)
        sys.exit(2)
    elif not yaml_files and not json_files:
        print(f"No YAML or JSON files found in {schema_dir}.", file=sys.stderr)
        sys.exit(2)
    fmt = "json" if json_files else "yaml"

    client = _make_client(args.host, args.token)
    extractor = CatalogExtractor(client=client, max_workers=args.workers)
    print(f"Comparing catalog '{args.catalog}' against {schema_dir}...", file=sys.stderr)
    catalog_obj = extractor.extract_catalog(
        catalog_name=args.catalog,
        schema_filter=args.schema,
        include_metadata=args.include_metadata,
        include_tags=not args.no_tags,
    )

    result = diff_catalog_with_dir(
        catalog_obj,
        schema_dir,
        schema_names=frozenset(args.schema) if args.schema else None,
        fmt=fmt,
        include_metadata=args.include_metadata,
    )

    if not result.has_changes:
        print("No differences found.")
        return

    _print_diff(result)
    sys.exit(1)


def _print_diff(result: CatalogDiff) -> None:
    markers = {"added": "+", "removed": "-", "modified": "~"}
    for s in result.schemas:
        if not s.has_changes:
            continue
        print(f"{markers.get(s.status, '~')} Schema: {s.name} [{s.status.upper()}]")
        for fc in s.changes:
            print(f"    {fc.field}: {fc.old!r} -> {fc.new!r}")
        for t in s.tables:
            print(f"  {markers.get(t.status, '~')} Table: {t.name} [{t.status.upper()}]")
            for fc in t.changes:
                print(f"      {fc.field}: {fc.old!r} -> {fc.new!r}")
            for c in t.columns:
                print(f"    {markers.get(c.status, '~')} Column: {c.name} [{c.status.upper()}]")
                for fc in c.changes:
                    print(f"        {fc.field}: {fc.old!r} -> {fc.new!r}")


def _cmd_generate_sql(args: argparse.Namespace) -> None:
    """Generate SQL statements to bring the live catalog in line with local files."""
    schema_dir: Path = args.schema_dir
    if not schema_dir.is_dir():
        print(f"Error: {schema_dir} is not a directory.", file=sys.stderr)
        sys.exit(2)

    yaml_files = list(schema_dir.glob("*.yaml"))
    json_files = list(schema_dir.glob("*.json"))
    if yaml_files and json_files:
        print("Error: mixed YAML and JSON files in schema directory.", file=sys.stderr)
        sys.exit(2)
    elif not yaml_files and not json_files:
        print(f"No YAML or JSON files found in {schema_dir}.", file=sys.stderr)
        sys.exit(2)
    fmt = "json" if json_files else "yaml"
    ext = ".json" if fmt == "json" else ".yaml"
    loader = schema_from_json if fmt == "json" else schema_from_yaml

    schema_filter_set: frozenset[str] | None = frozenset(args.schema) if args.schema else None
    stored: dict[str, Schema] = {}
    for schema_file in sorted(schema_dir.glob(f"*{ext}")):
        if schema_filter_set is not None and schema_file.stem not in schema_filter_set:
            continue
        schema = loader(schema_file.read_text(encoding="utf-8"))
        stored[schema.name] = schema

    client = _make_client(args.host, args.token)
    extractor = CatalogExtractor(client=client, max_workers=args.workers)
    print(f"Generating SQL for catalog '{args.catalog}' against {schema_dir}...", file=sys.stderr)
    catalog_obj = extractor.extract_catalog(
        catalog_name=args.catalog,
        schema_filter=args.schema,
        include_metadata=args.include_metadata,
        include_tags=not args.no_tags,
    )

    live = {s.name: s for s in catalog_obj.schemas}
    ignore_added: frozenset[str] = frozenset({"default"})
    sql_outputs: list[tuple[str, str]] = []

    for name, stored_schema in stored.items():
        if name not in live:
            sd: SchemaDiff = SchemaDiff(name=name, status="removed")
        else:
            sd = diff_schemas(live[name], stored_schema, args.include_metadata)

        if not sd.has_changes:
            continue

        sql = schema_diff_to_sql(args.catalog, sd, stored_schema, args.allow_drop)
        if sql:
            sql_outputs.append((name, sql))

    for name in live:
        if name not in stored and name not in ignore_added:
            sd = SchemaDiff(name=name, status="added")
            sql = schema_diff_to_sql(args.catalog, sd, None, args.allow_drop)
            if sql:
                sql_outputs.append((name, sql))

    if not sql_outputs:
        print("No differences found — no SQL generated.")
        return

    if args.output_dir:
        output_dir: Path = args.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        for schema_name, sql in sql_outputs:
            out_file = output_dir / f"{schema_name}.sql"
            out_file.write_text(sql, encoding="utf-8")
            print(f"  Wrote {out_file}", file=sys.stderr)
    else:
        for schema_name, sql in sql_outputs:
            print(f"-- Schema: {schema_name}")
            print(sql)
            print()


def _cmd_list_catalogs(args: argparse.Namespace) -> None:
    """List all accessible catalogs."""
    client = _make_client(args.host, args.token)
    for c in client.catalogs.list():
        print(c.name)


def _cmd_list_schemas(args: argparse.Namespace) -> None:
    """List schemas in a catalog."""
    client = _make_client(args.host, args.token)
    for s in client.schemas.list(catalog_name=args.catalog):
        print(s.name)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="databricks-schema",
        description="Databricks Unity Catalog schema extractor",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    # extract
    extract_p = subparsers.add_parser(
        "extract", help="Extract Unity Catalog schemas to YAML files."
    )
    extract_p.add_argument("catalog", help="Catalog name")
    extract_p.add_argument(
        "--schema",
        "-s",
        action="append",
        metavar="SCHEMA",
        help="Schema filter (repeatable)",
    )
    extract_p.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        dest="output_dir",
        metavar="DIR",
        help="Output directory for per-schema YAML files",
    )
    extract_p.add_argument(
        "--include-metadata",
        action="store_true",
        dest="include_metadata",
        help="Include additional metadata in output (owner, storage_location)",
    )
    extract_p.add_argument(
        "--no-tags",
        action="store_true",
        dest="no_tags",
        help="Skip tag lookups (faster, omits tags from output)",
    )
    extract_p.add_argument(
        "--format",
        "-f",
        choices=["yaml", "json"],
        default="yaml",
        dest="fmt",
        help="Output format (default: yaml)",
    )
    extract_p.add_argument(
        "--workers",
        type=int,
        default=4,
        metavar="N",
        help="Number of parallel workers for table extraction (default: 4)",
    )
    _add_connection_args(extract_p)
    extract_p.set_defaults(func=_cmd_extract)

    # diff
    diff_p = subparsers.add_parser(
        "diff", help="Compare Unity Catalog schemas against local YAML files."
    )
    diff_p.add_argument("catalog", help="Catalog name")
    diff_p.add_argument("schema_dir", type=Path, help="Directory containing per-schema YAML files")
    diff_p.add_argument(
        "--schema",
        "-s",
        action="append",
        metavar="SCHEMA",
        help="Schema filter (repeatable)",
    )
    diff_p.add_argument(
        "--no-tags",
        action="store_true",
        dest="no_tags",
        help="Skip tag lookups (faster, omits tags from comparison)",
    )
    diff_p.add_argument(
        "--include-metadata",
        action="store_true",
        dest="include_metadata",
        help="Include additional metadata in comparison (owner, storage_location)",
    )
    diff_p.add_argument(
        "--workers",
        type=int,
        default=4,
        metavar="N",
        help="Number of parallel workers for table extraction (default: 4)",
    )
    _add_connection_args(diff_p)
    diff_p.set_defaults(func=_cmd_diff)

    # generate-sql
    gen_sql_p = subparsers.add_parser(
        "generate-sql",
        help="Generate SQL statements to bring the live catalog in line with local files.",
    )
    gen_sql_p.add_argument("catalog", help="Catalog name")
    gen_sql_p.add_argument(
        "schema_dir", type=Path, help="Directory containing per-schema YAML or JSON files"
    )
    gen_sql_p.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        dest="output_dir",
        metavar="DIR",
        help="Write one .sql file per schema instead of printing to stdout",
    )
    gen_sql_p.add_argument(
        "--allow-drop",
        action="store_true",
        dest="allow_drop",
        help="Emit real DROP statements instead of commented-out ones",
    )
    gen_sql_p.add_argument(
        "--schema",
        "-s",
        action="append",
        metavar="SCHEMA",
        help="Schema filter (repeatable)",
    )
    gen_sql_p.add_argument(
        "--no-tags",
        action="store_true",
        dest="no_tags",
        help="Skip tag lookups (faster, omits tags from comparison)",
    )
    gen_sql_p.add_argument(
        "--include-metadata",
        action="store_true",
        dest="include_metadata",
        help="Include additional metadata in comparison (owner, storage_location)",
    )
    gen_sql_p.add_argument(
        "--workers",
        type=int,
        default=4,
        metavar="N",
        help="Number of parallel workers for table extraction (default: 4)",
    )
    _add_connection_args(gen_sql_p)
    gen_sql_p.set_defaults(func=_cmd_generate_sql)

    # list-catalogs
    list_catalogs_p = subparsers.add_parser("list-catalogs", help="List all accessible catalogs.")
    _add_connection_args(list_catalogs_p)
    list_catalogs_p.set_defaults(func=_cmd_list_catalogs)

    # list-schemas
    list_schemas_p = subparsers.add_parser("list-schemas", help="List schemas in a catalog.")
    list_schemas_p.add_argument("catalog", help="Catalog name")
    _add_connection_args(list_schemas_p)
    list_schemas_p.set_defaults(func=_cmd_list_schemas)

    return parser


def main() -> None:
    parser = _build_parser()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    args = parser.parse_args()
    args.func(args)
