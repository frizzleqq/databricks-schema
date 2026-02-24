from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks_schema.models import Catalog, Column, ForeignKey, PrimaryKey, Schema, Table

logger = logging.getLogger(__name__)

_SYSTEM_SCHEMAS = {"information_schema"}


class CatalogExtractor:
    def __init__(self, client: WorkspaceClient | None = None) -> None:
        self.client = client or WorkspaceClient()

    def _fetch_tags(self, entity_type: str, entity_name: str) -> dict[str, str]:
        tags: dict[str, str] = {}
        try:
            for assignment in self.client.entity_tag_assignments.list(entity_type, entity_name):
                key = getattr(assignment, "tag_key", None)
                value = getattr(assignment, "tag_value", None)
                if key is not None:
                    tags[key] = value or ""
        except NotFound:
            logger.error("Not found when fetching tags for %s '%s'", entity_type, entity_name)
        return tags

    def iter_schemas(
        self,
        catalog_name: str,
        schema_filter: list[str] | None = None,
        skip_system_schemas: bool = True,
        include_storage_location: bool = False,
    ) -> Iterator[Schema]:
        for sdk_schema in self.client.schemas.list(catalog_name=catalog_name):
            schema_name = sdk_schema.name or ""
            if skip_system_schemas and schema_name in _SYSTEM_SCHEMAS:
                continue
            if schema_filter and schema_name not in schema_filter:
                continue
            yield self._extract_schema(catalog_name, sdk_schema, include_storage_location)

    def extract_catalog(
        self,
        catalog_name: str,
        schema_filter: list[str] | None = None,
        skip_system_schemas: bool = True,
        include_storage_location: bool = False,
    ) -> Catalog:
        sdk_catalog = self.client.catalogs.get(catalog_name)
        catalog_tags = self._fetch_tags("catalogs", catalog_name)

        schemas = list(
            self.iter_schemas(
                catalog_name, schema_filter, skip_system_schemas, include_storage_location
            )
        )

        return Catalog(
            name=catalog_name,
            comment=getattr(sdk_catalog, "comment", None),
            schemas=schemas,
            tags=catalog_tags,
        )

    def _extract_schema(
        self, catalog_name: str, sdk_schema, include_storage_location: bool = False
    ) -> Schema:
        schema_name = sdk_schema.name or ""
        schema_tags = self._fetch_tags("schemas", f"{catalog_name}.{schema_name}")

        tables: list[Table] = []
        for sdk_table_summary in self.client.tables.list(
            catalog_name=catalog_name, schema_name=schema_name
        ):
            table_name = sdk_table_summary.name or ""
            full_name = f"{catalog_name}.{schema_name}.{table_name}"
            table = self._extract_table(
                catalog_name, schema_name, full_name, include_storage_location
            )
            tables.append(table)

        return Schema(
            name=schema_name,
            comment=getattr(sdk_schema, "comment", None),
            owner=getattr(sdk_schema, "owner", None),
            tables=tables,
            tags=schema_tags,
        )

    def _extract_table(
        self,
        catalog_name: str,
        schema_name: str,
        full_name: str,
        include_storage_location: bool = False,
    ) -> Table:
        sdk_table = self.client.tables.get(full_name)
        table_tags = self._fetch_tags("tables", full_name)

        # Build columns sorted by position
        sdk_columns = list(getattr(sdk_table, "columns", None) or [])
        sdk_columns.sort(key=lambda c: getattr(c, "position", None) or 9999)

        columns: list[Column] = []
        for sdk_col in sdk_columns:
            # Prefer type_text (e.g. "ARRAY<STRING>"), fall back to type_name
            type_text = getattr(sdk_col, "type_text", None)
            type_name = getattr(sdk_col, "type_name", None)
            if type_text:
                data_type = type_text
            elif type_name is not None:
                data_type = type_name.value if hasattr(type_name, "value") else str(type_name)
            else:
                data_type = "UNKNOWN"

            col_tags = self._fetch_tags("columns", f"{full_name}.{sdk_col.name or ''}")
            columns.append(
                Column(
                    name=sdk_col.name or "",
                    data_type=data_type,
                    comment=getattr(sdk_col, "comment", None),
                    nullable=(
                        getattr(sdk_col, "nullable", True)
                        if getattr(sdk_col, "nullable", None) is not None
                        else True
                    ),
                    tags=col_tags,
                )
            )

        # Parse constraints
        primary_key: PrimaryKey | None = None
        foreign_keys: list[ForeignKey] = []

        for constraint in list(getattr(sdk_table, "table_constraints", None) or []):
            pk = getattr(constraint, "primary_key_constraint", None)
            if pk is not None:
                primary_key = PrimaryKey(
                    name=getattr(pk, "name", None),
                    columns=list(getattr(pk, "child_columns", None) or []),
                )

            fk = getattr(constraint, "foreign_key_constraint", None)
            if fk is not None:
                parent_table = getattr(fk, "parent_table", None) or ""
                parts = parent_table.split(".")
                # Expected: catalog.schema.table
                if len(parts) >= 3:
                    ref_schema = parts[-2]
                    ref_table = parts[-1]
                elif len(parts) == 2:
                    ref_schema = parts[0]
                    ref_table = parts[1]
                else:
                    ref_schema = ""
                    ref_table = parent_table

                foreign_keys.append(
                    ForeignKey(
                        name=getattr(fk, "name", None),
                        columns=list(getattr(fk, "child_columns", None) or []),
                        ref_schema=ref_schema,
                        ref_table=ref_table,
                        ref_columns=list(getattr(fk, "parent_columns", None) or []),
                    )
                )

        # created_at is a Unix timestamp in milliseconds
        created_at_ms = getattr(sdk_table, "created_at", None)
        created_at = (
            datetime.fromtimestamp(created_at_ms / 1000, tz=UTC)
            if created_at_ms is not None
            else None
        )

        table_name = full_name.split(".")[-1]
        return Table(
            name=table_name,
            table_type=getattr(sdk_table, "table_type", None),
            comment=getattr(sdk_table, "comment", None),
            owner=getattr(sdk_table, "owner", None),
            created_at=created_at,
            columns=columns,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
            tags=table_tags,
            storage_location=(
                getattr(sdk_table, "storage_location", None) if include_storage_location else None
            ),
        )
