from __future__ import annotations

from pathlib import Path

from databricks_schema import CatalogExtractor, schema_from_yaml, schema_to_yaml

# Extract schemas, save each to a YAML file, then reload and inspect the result.
# Uses default Databricks auth (env vars, ~/.databrickscfg, etc.)

CATALOG = "lake_prod"
SCHEMAS = ["gold", "silver"]
OUTPUT_DIR = Path(".test_output/02_save_load_yaml")

extractor = CatalogExtractor()
catalog = extractor.extract_catalog(CATALOG, schema_filter=SCHEMAS)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for schema in catalog.schemas:
    path = OUTPUT_DIR / f"{schema.name}.yaml"
    path.write_text(schema_to_yaml(schema), encoding="utf-8")
    print(f"Saved {path}")

# Reload and inspect
for schema in catalog.schemas:
    path = OUTPUT_DIR / f"{schema.name}.yaml"
    loaded = schema_from_yaml(path.read_text(encoding="utf-8"))
    print(f"Reloaded '{loaded.name}': {len(loaded.tables)} tables")
    for table in loaded.tables:
        print(f"  {table.name} ({len(table.columns)} columns)")
