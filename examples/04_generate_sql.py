from __future__ import annotations

from pathlib import Path

from databricks_schema import (
    CatalogExtractor,
    diff_schemas,
    schema_diff_to_sql,
    schema_from_yaml,
    schema_to_yaml,
)

# Generate SQL to reconcile a live schema with a stored YAML baseline.
# Simulates drift by modifying the stored YAML, then generates SQL to fix it.
# Uses default Databricks auth (env vars, ~/.databrickscfg, etc.)

CATALOG = "lake_prod"
SCHEMA = "gold"
OUTPUT_DIR = Path(".test_output/04_generate_sql")

extractor = CatalogExtractor()
catalog = extractor.extract_catalog(CATALOG, schema_filter=[SCHEMA])
live_schema = catalog.schemas[0]

# Save current state as the baseline
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
baseline_path = OUTPUT_DIR / f"{SCHEMA}.yaml"
baseline_path.write_text(schema_to_yaml(live_schema), encoding="utf-8")
print(f"Saved baseline {baseline_path}")

# Load stored schema (this is your "desired state")
stored_schema = schema_from_yaml(baseline_path.read_text(encoding="utf-8"))

# Diff live vs stored
schema_diff = diff_schemas(live=live_schema, stored=stored_schema)

if not schema_diff.has_changes:
    print("No drift detected — nothing to generate.")
else:
    sql = schema_diff_to_sql(CATALOG, schema_diff, stored_schema, allow_drop=False)
    print(sql)
