from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer

if TYPE_CHECKING:
    from .diff import CatalogDiff

app = typer.Typer(name="databricks-schema", no_args_is_help=True)


def _make_client(host: str | None, token: str | None):
    from databricks.sdk import WorkspaceClient

    kwargs = {}
    if host:
        kwargs["host"] = host
    if token:
        kwargs["token"] = token
    return WorkspaceClient(**kwargs)


@app.command()
def extract(
    catalog: Annotated[str, typer.Argument(help="Catalog name")],
    schema: Annotated[
        list[str] | None, typer.Option("--schema", "-s", help="Schema filter (repeatable)")
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option("--output-dir", "-o", help="Output directory for per-schema YAML files"),
    ] = None,
    include_system: Annotated[
        bool, typer.Option("--include-system", help="Include system schemas (information_schema)")
    ] = False,
    storage_location: Annotated[
        bool, typer.Option("--storage-location", help="Include storage_location in output")
    ] = False,
    host: Annotated[
        str | None, typer.Option("--host", envvar="DATABRICKS_HOST", help="Databricks host URL")
    ] = None,
    token: Annotated[
        str | None,
        typer.Option("--token", envvar="DATABRICKS_TOKEN", help="Databricks access token"),
    ] = None,
) -> None:
    """Extract Unity Catalog schemas to YAML files."""
    from .extractor import CatalogExtractor
    from .yaml_io import schema_to_yaml

    client = _make_client(host, token)
    extractor = CatalogExtractor(client=client)

    print(f"Extracting catalog '{catalog}'...", file=sys.stderr)
    catalog_obj = extractor.extract_catalog(
        catalog_name=catalog,
        schema_filter=list(schema) if schema else None,
        skip_system_schemas=not include_system,
        include_storage_location=storage_location,
    )

    if output_dir is None:
        if len(catalog_obj.schemas) != 1:
            typer.echo(
                "Error: --output-dir is required when extracting multiple schemas.", err=True
            )
            raise typer.Exit(code=1)
        print(schema_to_yaml(catalog_obj.schemas[0]))
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    for s in catalog_obj.schemas:
        out_file = output_dir / f"{s.name}.yaml"
        out_file.write_text(schema_to_yaml(s), encoding="utf-8")
        print(f"  Wrote {out_file}", file=sys.stderr)

    print(f"Done — {len(catalog_obj.schemas)} schema(s) written to {output_dir}", file=sys.stderr)


@app.command()
def diff(
    catalog: Annotated[str, typer.Argument(help="Catalog name")],
    schema_dir: Annotated[Path, typer.Argument(help="Directory containing per-schema YAML files")],
    schema: Annotated[
        list[str] | None, typer.Option("--schema", "-s", help="Schema filter (repeatable)")
    ] = None,
    host: Annotated[
        str | None, typer.Option("--host", envvar="DATABRICKS_HOST", help="Databricks host URL")
    ] = None,
    token: Annotated[
        str | None,
        typer.Option("--token", envvar="DATABRICKS_TOKEN", help="Databricks access token"),
    ] = None,
) -> None:
    """Compare Unity Catalog schemas against local YAML files.

    Exits with code 1 if differences are found (useful in CI).
    """
    from .diff import diff_catalog_with_dir
    from .extractor import CatalogExtractor

    if not schema_dir.is_dir():
        typer.echo(f"Error: {schema_dir} is not a directory.", err=True)
        raise typer.Exit(code=2)

    yaml_files = list(schema_dir.glob("*.yaml"))
    if not yaml_files:
        typer.echo(f"No YAML files found in {schema_dir}.", err=True)
        raise typer.Exit(code=2)

    # Extract only schemas for which YAML files exist (unless filtered explicitly)
    schema_filter = list(schema) if schema else [f.stem for f in yaml_files]

    client = _make_client(host, token)
    extractor = CatalogExtractor(client=client)
    print(f"Comparing catalog '{catalog}' against {schema_dir}...", file=sys.stderr)
    catalog_obj = extractor.extract_catalog(catalog_name=catalog, schema_filter=schema_filter)

    result = diff_catalog_with_dir(catalog_obj, schema_dir)

    if not result.has_changes:
        print("No differences found.")
        return

    _print_diff(result)
    raise typer.Exit(code=1)


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


@app.command("list-catalogs")
def list_catalogs(
    host: Annotated[
        str | None, typer.Option("--host", envvar="DATABRICKS_HOST", help="Databricks host URL")
    ] = None,
    token: Annotated[
        str | None,
        typer.Option("--token", envvar="DATABRICKS_TOKEN", help="Databricks access token"),
    ] = None,
) -> None:
    """List all accessible catalogs."""
    client = _make_client(host, token)
    for c in client.catalogs.list():
        print(c.name)


@app.command("list-schemas")
def list_schemas(
    catalog: Annotated[str, typer.Argument(help="Catalog name")],
    host: Annotated[
        str | None, typer.Option("--host", envvar="DATABRICKS_HOST", help="Databricks host URL")
    ] = None,
    token: Annotated[
        str | None,
        typer.Option("--token", envvar="DATABRICKS_TOKEN", help="Databricks access token"),
    ] = None,
) -> None:
    """List schemas in a catalog."""
    client = _make_client(host, token)
    for s in client.schemas.list(catalog_name=catalog):
        print(s.name)
