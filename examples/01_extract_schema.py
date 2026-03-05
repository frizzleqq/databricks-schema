from __future__ import annotations

from databricks_schema import CatalogExtractor

# Extract a single schema and inspect its contents.
# Uses default Databricks auth (env vars, ~/.databrickscfg, etc.)

CATALOG = "lake_prod"
SCHEMA = ["gold", "silver"]

extractor = CatalogExtractor()
catalog = extractor.extract_catalog(CATALOG, schema_filter=SCHEMA)

schema = catalog.schemas[0]
print(f"Schema: {schema.name} ({len(schema.tables)} tables)")

for table in schema.tables:
    col_names = [c.name for c in table.columns]
    print(f"  {table.name} [{table.table_type}]: {', '.join(col_names)}")
