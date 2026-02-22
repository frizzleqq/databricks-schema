import sys
from pathlib import Path

from databricks_schema import (
    CatalogExtractor,
    catalog_to_yaml,
    diff_catalog_with_dir,
    diff_schemas,
    schema_from_yaml,
    schema_to_yaml,
)

# Configuration
CATALOG_NAME = "lake_prod"
SCHEMA_FILTER = ["gold", "silver"]
SINGLE_SCHEMA = "gold"
OUTPUT_DIR = Path(".test_output/example")

# Extract using configured auth
extractor = CatalogExtractor()
catalog = extractor.extract_catalog(CATALOG_NAME, schema_filter=SCHEMA_FILTER)

# Serialise to YAML String
yaml_text = catalog_to_yaml(catalog)

# Serialise to YAML file(s)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for s in catalog.schemas:
    out_file = OUTPUT_DIR / f"{s.name}.yaml"
    out_file.write_text(schema_to_yaml(s), encoding="utf-8")
    print(f"  Wrote {out_file}", file=sys.stderr)

print(f"Done — {len(catalog.schemas)} schema(s) written to {OUTPUT_DIR}", file=sys.stderr)

# Deserialise from YAML
schema = schema_from_yaml(open(OUTPUT_DIR / f"{SINGLE_SCHEMA}.yaml").read())
print(schema.tables[0].columns)

# Compare live catalog against local YAML files
result = diff_catalog_with_dir(catalog, OUTPUT_DIR)
if result.has_changes:
    for schema_diff in result.schemas:
        print(schema_diff.name, schema_diff.status)

# Compare two Schema objects directly
stored = schema_from_yaml(open(OUTPUT_DIR / f"{SINGLE_SCHEMA}.yaml").read())
live = extractor._extract_schema(CATALOG_NAME, schema)
diff = diff_schemas(live=live, stored=stored)
