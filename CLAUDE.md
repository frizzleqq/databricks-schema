# CLAUDE.md

## Project

`databricks-schema` — CLI + Python library that extracts, diffs, and generates SQL for Databricks Unity Catalog schemas stored as YAML or JSON files (one file per schema) or DBML (one file per catalog). Pydantic v2 models serve as the intermediate representation for future bidirectional sync.

## Key commands

```bash
uv sync --all-groups          # install deps (including dev)
uv run pytest                 # run tests
uv run ruff check databricks_schema/ tests/
uv run ruff format databricks_schema/ tests/
uv run databricks-schema --help
```

## Package layout

```
databricks_schema/
  models.py      # Pydantic v2 models: Catalog, Schema, Table, Column, PrimaryKey, ForeignKey
  extractor.py   # CatalogExtractor — wraps databricks-sdk
  yaml_io.py     # schema/catalog to/from YAML and JSON; _strip_empty removes None + empty collections; load_schema_dir (all formats)
  dbml_io.py     # schema/catalog to/from DBML; own writer, PyDBML grammar for parsing
  diff.py        # diff_schemas / diff_catalogs / diff_catalog_with_dir / diff_schema_dirs; FieldChange, ColumnDiff, TableDiff, SchemaDiff, CatalogDiff
  sql_gen.py     # schema_diff_to_sql — pure SQL generation from SchemaDiff; no SDK/IO
  cli.py         # argparse CLI: extract, diff, generate-sql, validate, diff-files, list-catalogs, list-schemas
  __init__.py    # public re-exports
tests/
  test_models.py
  test_extractor.py  # all SDK calls mocked with MagicMock
  test_yaml_io.py
  test_dbml_io.py
  test_diff.py       # pure model comparison, no SDK calls
  test_sql_gen.py    # pure SQL generation tests, no SDK calls
  test_validate.py
skills/databricks-schema-cli/SKILL.md  # Agent Skill: teaches an AI agent to drive the CLI
```

## Conventions

- Package manager: `uv`; do not use `pip` directly
- Version bumps: use `uv version --bump <major|minor|patch>` (updates `pyproject.toml` and `uv.lock` together) — do not hand-edit the version string
- Ruff: select E, W, F, I, UP; line-length 100; target py311
- Use `X | None` not `Optional[X]`; use `datetime.UTC` not `timezone.utc`
- `from __future__ import annotations` in every module
- Imports at the top of each module — never inside functions
- Use full package imports (`from databricks_schema.models import ...`), not relative imports (`from .models import ...`)
- Tags = Unity Catalog governance key/value tags — not `properties`
- FK refs store only `ref_schema` + `ref_table` (no catalog)
- Column order in YAML = SDK position (None → 9999)
- `_strip_empty`: removes `None` and empty `dict`/`list`; preserves `False`, `0`, empty strings
- `yaml_io.py` has parallel YAML and JSON functions (`schema_to_yaml`/`schema_to_json`, etc.)
- `diff_catalog_with_dir` / `diff_schema_dirs` / `load_schema_dir` take `fmt: SchemaFormat` (`"yaml" | "json" | "dbml"`, default `"yaml"`)
- DBML is one document per catalog (`extract -f dbml -o DIR` writes `DIR/<catalog>.dbml`): Catalog → `Project`, Schema → `TableGroup` (always emitted), PK → `indexes { (...) [pk] }`, FK → `Ref`
- DBML has no key/value metadata: tags/owner/table_type/storage_location are trailing `@key: value` lines in notes (`@tags` = JSON object); writer output must round-trip exactly and stay parseable by `@dbml/core`
- `dbml_io` parses via PyDBML's grammar (`pydbml.definitions`) but resolves refs itself, so refs to tables outside the file are kept; never build a PyDBML `Database` (it rejects such refs and reflows note text) and never use PyDBML's renderer (it leaves complex types unquoted)
- For DBML directories, `--schema` filters match schema names (YAML/JSON match file stems)
- CLI: catalog is a required positional argument (not a flag)
- `extract` `--format`/`-f` selects output format (`yaml` default, `json`/`dbml` opt-in); dest is `fmt`
- `diff` auto-detects format from files present in the directory; exits 2 on mixed formats
- `diff`'s second positional arg is a directory if one exists at that path, else a second catalog name (`diff_catalogs`); first `catalog` arg is always the live side
- `TableType` is re-exported from `databricks.sdk.service.catalog` — do not redefine it
- `--include-metadata` flag (on `extract`, `diff`, `generate-sql`, `diff-files`) adds `owner` to output/comparison; `extract` additionally includes `storage_location` in its output (not compared by `diff`/`generate-sql`/`diff-files`); excluded by default
- `--include-tags` flag (on `extract`, `diff`, `generate-sql`) enables Unity Catalog tag lookups; excluded by default
- `--quiet`/`-q` flag (on `extract`, `diff`, `generate-sql`, `diff-files`) raises the log level to suppress progress messages; errors always print
- `diff` command exits 0 (no changes) or 1 (differences found) — useful in CI
- Diff result types are dataclasses (not Pydantic); comparison functions are pure (no SDK calls)
- `generate-sql` auto-detects format from files; exits 2 on mixed formats or empty directory; invalid DBML exits 2
- `generate-sql` destructive statements commented out by default; `--allow-drop` emits real DROPs
- `sql_gen.py` is pure (no SDK, no I/O); diff direction: `FieldChange.old` = stored (target), `.new` = live
- FK refs in SQL use same catalog as the source table (`ref_schema` + `ref_table` from model)
- Unsupported field changes (`table_type`) emit `-- TODO: unsupported change: …` comments
- When CLI commands, flags, or output shape change, update `skills/databricks-schema-cli/SKILL.md` to match
