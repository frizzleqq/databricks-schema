from __future__ import annotations

from pathlib import Path

from databricks_schema import CatalogExtractor, diff_catalog_with_dir, schema_to_yaml

# Diff live catalog schemas against local YAML files.
# Saves a baseline on first run, then diffs live state against it.
# Uses default Databricks auth (env vars, ~/.databrickscfg, etc.)

CATALOG = "lake_prod"
SCHEMAS = ["gold", "silver"]
SCHEMA_DIR = Path(".test_output/03_diff_schemas")

extractor = CatalogExtractor()
catalog = extractor.extract_catalog(CATALOG, schema_filter=SCHEMAS)

# Save baseline if not already present
SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
for schema in catalog.schemas:
    path = SCHEMA_DIR / f"{schema.name}.yaml"
    if not path.exists():
        path.write_text(schema_to_yaml(schema), encoding="utf-8")
        print(f"Saved baseline {path}")

result = diff_catalog_with_dir(catalog, SCHEMA_DIR, schema_names=frozenset(SCHEMAS))

if not result.has_changes:
    print("No changes — live catalog matches local files.")
else:
    for schema_diff in result.schemas:
        if not schema_diff.has_changes:
            continue
        print(f"\nSchema '{schema_diff.name}': {schema_diff.status}")
        for fc in schema_diff.changes:
            print(f"  field '{fc.field}': {fc.old!r} -> {fc.new!r}")
        for table_diff in schema_diff.tables:
            print(f"  table '{table_diff.name}': {table_diff.status}")
            for fc in table_diff.changes:
                print(f"    field '{fc.field}': {fc.old!r} -> {fc.new!r}")
            for col_diff in table_diff.columns:
                print(f"    column '{col_diff.name}': {col_diff.status}")
                for fc in col_diff.changes:
                    print(f"      {fc.field}: {fc.old!r} -> {fc.new!r}")
