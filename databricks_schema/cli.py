from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

from databricks_schema.diff import CatalogDiff, diff_catalog_with_dir
from databricks_schema.extractor import CatalogExtractor
from databricks_schema.yaml_io import schema_to_yaml

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
    """Extract Unity Catalog schemas to YAML files."""
    client = _make_client(args.host, args.token)
    extractor = CatalogExtractor(client=client)

    print(f"Extracting catalog '{args.catalog}'...", file=sys.stderr)

    if args.output_dir is None:
        schema_filter_set = set(args.schema) if args.schema else None
        matching_schemas = [
            s.name
            for s in client.schemas.list(catalog_name=args.catalog)
            if (args.include_system or (s.name or "") not in {"information_schema"})
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
            skip_system_schemas=not args.include_system,
            include_storage_location=args.storage_location,
        )
        print(schema_to_yaml(catalog_obj.schemas[0]))
        return

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for s in extractor.iter_schemas(
        catalog_name=args.catalog,
        schema_filter=args.schema,
        skip_system_schemas=not args.include_system,
        include_storage_location=args.storage_location,
    ):
        out_file = output_dir / f"{s.name}.yaml"
        out_file.write_text(schema_to_yaml(s), encoding="utf-8")
        print(f"  Wrote {out_file}", file=sys.stderr)
        count += 1

    print(f"Done — {count} schema(s) written to {output_dir}", file=sys.stderr)


def _cmd_diff(args: argparse.Namespace) -> None:
    """Compare Unity Catalog schemas against local YAML files."""
    schema_dir: Path = args.schema_dir
    if not schema_dir.is_dir():
        print(f"Error: {schema_dir} is not a directory.", file=sys.stderr)
        sys.exit(2)

    yaml_files = list(schema_dir.glob("*.yaml"))
    if not yaml_files:
        print(f"No YAML files found in {schema_dir}.", file=sys.stderr)
        sys.exit(2)

    client = _make_client(args.host, args.token)
    extractor = CatalogExtractor(client=client)
    print(f"Comparing catalog '{args.catalog}' against {schema_dir}...", file=sys.stderr)
    catalog_obj = extractor.extract_catalog(
        catalog_name=args.catalog,
        schema_filter=args.schema,
    )

    result = diff_catalog_with_dir(
        catalog_obj,
        schema_dir,
        schema_names=frozenset(args.schema) if args.schema else None,
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
        "--include-system",
        action="store_true",
        dest="include_system",
        help="Include system schemas (information_schema)",
    )
    extract_p.add_argument(
        "--storage-location",
        action="store_true",
        dest="storage_location",
        help="Include storage_location in output",
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
    _add_connection_args(diff_p)
    diff_p.set_defaults(func=_cmd_diff)

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
