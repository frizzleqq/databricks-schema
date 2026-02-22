# CLAUDE.md

## Project

`databricks-schema` — CLI + Python library that extracts Databricks Unity Catalog schemas to YAML files (one file per schema). Pydantic v2 models serve as the intermediate representation for future bidirectional sync.

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
  models.py      # Pydantic v2 models: Catalog, Schema, Table, Column, PrimaryKey, ForeignKey, TableType
  extractor.py   # CatalogExtractor — wraps databricks-sdk
  yaml_io.py     # schema/catalog to/from YAML; _strip_empty removes None + empty collections
  cli.py         # Typer CLI: extract, list-catalogs, list-schemas
  __init__.py    # public re-exports
tests/
  test_models.py
  test_extractor.py  # all SDK calls mocked with MagicMock
  test_yaml_io.py
```

## Conventions

- Package manager: `uv`; do not use `pip` directly
- Ruff: select E, W, F, I, UP; line-length 100; target py313
- Use `X | None` not `Optional[X]`; use `StrEnum` not `(str, Enum)`
- `from __future__ import annotations` in every module
- Tags = Unity Catalog governance key/value tags — not `properties`
- FK refs store only `ref_schema` + `ref_table` (no catalog)
- Column order in YAML = SDK position (None → 9999)
- `_strip_empty`: removes `None` and empty `dict`/`list`; preserves `False`, `0`, empty strings
- CLI: catalog is a required positional argument (not a flag)
