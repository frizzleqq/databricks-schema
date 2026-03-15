from __future__ import annotations

from dataclasses import dataclass, field

from databricks_schema.models import Schema, Table


@dataclass
class ValidationIssue:
    schema: str
    table: str | None
    message: str

    def __str__(self) -> str:
        location = f"{self.schema}.{self.table}" if self.table else self.schema
        return f"{location}: {self.message}"


@dataclass
class ValidationResult:
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.issues)


def _validate_table(
    schema_name: str, table: Table, all_schemas: dict[str, Schema]
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    col_names = [c.name for c in table.columns]
    col_set = set(col_names)

    # Duplicate column names
    seen: set[str] = set()
    for name in col_names:
        if name in seen:
            issues.append(
                ValidationIssue(
                    schema=schema_name,
                    table=table.name,
                    message=f"duplicate column name: '{name}'",
                )
            )
        seen.add(name)

    # PK columns exist in table
    if table.primary_key:
        for col in table.primary_key.columns:
            if col not in col_set:
                issues.append(
                    ValidationIssue(
                        schema=schema_name,
                        table=table.name,
                        message=f"primary key references unknown column: '{col}'",
                    )
                )

    # FK checks
    for fk in table.foreign_keys:
        # FK source columns exist in this table
        for col in fk.columns:
            if col not in col_set:
                issues.append(
                    ValidationIssue(
                        schema=schema_name,
                        table=table.name,
                        message=f"foreign key references unknown source column: '{col}'",
                    )
                )

        # FK ref_schema + ref_table exist in loaded schemas
        ref_schema = all_schemas.get(fk.ref_schema)
        if ref_schema is None:
            issues.append(
                ValidationIssue(
                    schema=schema_name,
                    table=table.name,
                    message=f"foreign key references unknown schema: '{fk.ref_schema}'",
                )
            )
        else:
            ref_table_map = {t.name: t for t in ref_schema.tables}
            ref_table = ref_table_map.get(fk.ref_table)
            if ref_table is None:
                issues.append(
                    ValidationIssue(
                        schema=schema_name,
                        table=table.name,
                        message=(
                            f"foreign key references unknown table:"
                            f" '{fk.ref_schema}.{fk.ref_table}'"
                        ),
                    )
                )
            else:
                # FK ref_columns exist in referenced table
                ref_col_set = {c.name for c in ref_table.columns}
                for col in fk.ref_columns:
                    if col not in ref_col_set:
                        issues.append(
                            ValidationIssue(
                                schema=schema_name,
                                table=table.name,
                                message=(
                                    f"foreign key references unknown column '{col}'"
                                    f" in '{fk.ref_schema}.{fk.ref_table}'"
                                ),
                            )
                        )

    return issues


def validate_schemas(schemas: dict[str, Schema]) -> ValidationResult:
    """Validate a collection of schemas for structural integrity.

    Checks performed (no SDK or I/O required):
    - Duplicate column names within a table
    - PK columns that reference columns not present in the table
    - FK source columns that reference columns not present in the table
    - FK ref_schema + ref_table combinations not found in the loaded schemas
    - FK ref_columns not found in the referenced table

    Args:
        schemas: mapping of schema name → Schema, as loaded from a directory.

    Returns:
        ValidationResult with a list of ValidationIssue for every problem found.
    """
    issues: list[ValidationIssue] = []
    for schema_name, schema in schemas.items():
        for table in schema.tables:
            issues.extend(_validate_table(schema_name, table, schemas))
    return ValidationResult(issues=issues)
