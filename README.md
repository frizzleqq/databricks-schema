# databricks-schema

A CLI tool and Python library that uses the Databricks SDK to extract and diff Unity Catalog schemas as YAML files. It can also generate Databricks Spark SQL to apply schema changes across catalogs.

## Overview

Extract a catalog to YAML files, then diff those files against a catalog — the same one to detect drift or a different one to compare environments (e.g. prod vs test):

```bash
# 1. Find the catalog you want to snapshot
databricks-schema list-catalogs

# 2. Extract its schemas to YAML files (one file per schema)
databricks-schema extract prod_catalog --output-dir ./schemas/

# 3. Diff those files against a catalog (same or different)
databricks-schema diff test_catalog ./schemas/

# 4. Generate SQL to bring that catalog in line with the YAML files
databricks-schema generate-sql test_catalog ./schemas/ --output-dir ./migrations/
```

The YAML files act as a version-controllable snapshot of your schema. The `diff` command exits with code `1` when differences are found, making it suitable for CI pipelines.

## Output Format

Each schema is written to `{output-dir}/{schema-name}.yaml`. Fields with no value (null comments, empty tag dicts, empty FK lists) are omitted. Use `--format json` to write `.json` files with the same structure.

```yaml
name: main
comment: Main production schema
tags:
  env: prod
tables:
  - name: users
    table_type: MANAGED
    comment: User accounts
    tags:
      domain: identity
    columns:
      - name: id
        data_type: BIGINT
        nullable: false
        comment: Primary key
      - name: email
        data_type: STRING
      - name: org_id
        data_type: BIGINT
    primary_key:
      name: pk_users
      columns:
        - id
    foreign_keys:
      - name: fk_org
        columns:
          - org_id
        ref_schema: orgs
        ref_table: organizations
        ref_columns:
          - id
```

## Installation

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
git clone <repo>
cd databricks-schema
uv sync
```

For development (includes pytest and ruff):

```bash
uv sync --all-groups
```

## Authentication

The tool uses the [Databricks SDK](https://github.com/databricks/databricks-sdk-py) for auth. Configure it via environment variables:

```bash
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<your-personal-access-token>
```

Or use a [Databricks CLI profile](https://docs.databricks.com/dev-tools/cli/profiles.html) (`~/.databrickscfg`) — the SDK will pick it up automatically.

You can also pass credentials directly as flags (see `--host` / `--token` below).

## CLI Usage

```
databricks-schema [OPTIONS] COMMAND [ARGS]...
```

### `extract`

Extract all schemas from a catalog to YAML files:

```bash
databricks-schema extract <catalog> --output-dir ./schemas/
```

Use `--format json` to write `.json` files instead of `.yaml`.

Extract specific schemas only:

```bash
databricks-schema extract <catalog> --schema main --schema raw --output-dir ./schemas/
```

Print a single schema to stdout (no `--output-dir`):

```bash
databricks-schema extract <catalog> --schema main
```

Skip tag lookups for faster extraction (tags will be absent from output):

```bash
databricks-schema extract <catalog> --output-dir ./schemas/ --no-tags
```

Control the number of parallel workers (default: 4):

```bash
databricks-schema extract <catalog> --output-dir ./schemas/ --workers 8
```

### `diff`

Compare the live catalog against previously extracted schema files (format auto-detected from the directory — YAML or JSON, not mixed):

```bash
databricks-schema diff <catalog> ./schemas/
```

Compare specific schemas only:

```bash
databricks-schema diff <catalog> ./schemas/ --schema main --schema raw
```

Skip tag lookups during comparison:

```bash
databricks-schema diff <catalog> ./schemas/ --no-tags
```

Exits with code `0` if no differences are found, `1` if there are — making it suitable for CI pipelines. Output example:

```
~ Schema: main [MODIFIED]
  ~ Table: users [MODIFIED]
      owner: 'alice' -> 'bob'
    ~ Column: email [MODIFIED]
        data_type: 'STRING' -> 'VARCHAR(255)'
    + Column: phone [ADDED]
  + Table: events [ADDED]
- Schema: legacy [REMOVED]
```

Markers: `+` added in catalog, `-` removed from catalog, `~` modified.

### `generate-sql`

Generate Databricks Spark SQL statements to bring the live catalog in line with local schema files (format auto-detected, YAML or JSON, not mixed). Statements are printed to stdout by default:

```bash
databricks-schema generate-sql <catalog> ./schemas/
```

Write one `.sql` file per schema to a directory instead:

```bash
databricks-schema generate-sql <catalog> ./schemas/ --output-dir ./migrations/
```

Destructive statements (`DROP SCHEMA`, `DROP TABLE`, `DROP COLUMN`) are emitted as SQL comments by default. Pass `--allow-drop` to emit them as executable statements:

```bash
databricks-schema generate-sql <catalog> ./schemas/ --allow-drop
```

Filter to specific schemas:

```bash
databricks-schema generate-sql <catalog> ./schemas/ --schema main --schema raw
```

Skip tag lookups for faster comparison:

```bash
databricks-schema generate-sql <catalog> ./schemas/ --no-tags
```

### `list-catalogs`

List all accessible catalogs:

```bash
databricks-schema list-catalogs
```

### `list-schemas`

List schemas in a catalog:

```bash
databricks-schema list-schemas <catalog>
```

## Python Library Usage

```python
from pathlib import Path
from databricks_schema import CatalogExtractor, catalog_to_yaml, schema_from_yaml
from databricks_schema import diff_catalog_with_dir, diff_schemas, schema_diff_to_sql

# Extract using configured auth (max_workers controls parallel table extraction)
extractor = CatalogExtractor(max_workers=4)
catalog = extractor.extract_catalog("my_catalog", schema_filter=["main", "raw"])

# Skip tag lookups for faster extraction
catalog = extractor.extract_catalog("my_catalog", include_tags=False)

# Serialise to YAML
yaml_text = catalog_to_yaml(catalog)

# Deserialise from YAML
schema = schema_from_yaml(open("schemas/main.yaml").read())
print(schema.tables[0].columns)

# Compare live catalog against local YAML files
result = diff_catalog_with_dir(catalog, Path("./schemas/"))
if result.has_changes:
    for schema_diff in result.schemas:
        print(schema_diff.name, schema_diff.status)

# Compare two Schema objects directly
stored = schema_from_yaml(open("schemas/main.yaml").read())
diff = diff_schemas(live=catalog.schemas[0], stored=stored)

# Generate SQL to bring live in line with stored
sql = schema_diff_to_sql("my_catalog", diff, stored_schema=stored, allow_drop=False)
print(sql)
```

## Development

```bash
# Run tests
uv run pytest

# Lint
uv run ruff check databricks_schema/ tests/

# Format
uv run ruff format databricks_schema/ tests/
```
