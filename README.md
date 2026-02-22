# databricks-schema

A CLI tool and Python library for extracting Databricks Unity Catalog schemas to YAML files.

## Features

- Extracts catalog → schemas → tables → columns (with types, comments, nullability)
- Captures primary keys, foreign keys, and Unity Catalog governance tags
- Outputs one YAML file per schema for easy diffing and version control
- Pydantic v2 models as the intermediate representation (ready for bidirectional sync)

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

Extract specific schemas only:

```bash
databricks-schema extract <catalog> --schema main --schema raw --output-dir ./schemas/
```

Print a single schema to stdout (no `--output-dir`):

```bash
databricks-schema extract <catalog> --schema main
```

Include system schemas (`information_schema`):

```bash
databricks-schema extract <catalog> --output-dir ./schemas/ --include-system
```

Override auth inline:

```bash
databricks-schema extract <catalog> \
  --host https://<workspace>.cloud.databricks.com \
  --token <token> \
  --output-dir ./schemas/
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

## YAML Output Format

Each schema is written to `{output-dir}/{schema-name}.yaml`. Example:

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

Fields with no value (null comments, empty tag dicts, empty FK lists) are omitted from the YAML output.

## Python Library Usage

```python
from databricks_schema import CatalogExtractor, catalog_to_yaml, schema_from_yaml

# Extract using configured auth
extractor = CatalogExtractor()
catalog = extractor.extract_catalog("my_catalog", schema_filter=["main", "raw"])

# Serialise to YAML
yaml_text = catalog_to_yaml(catalog)

# Deserialise from YAML
schema = schema_from_yaml(open("schemas/main.yaml").read())
print(schema.tables[0].columns)
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
