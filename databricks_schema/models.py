from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class TableType(StrEnum):
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"
    STREAMING_TABLE = "STREAMING_TABLE"


class Column(BaseModel):
    name: str
    data_type: str
    comment: str | None = None
    nullable: bool = True
    tags: dict[str, str] = Field(default_factory=dict)


class PrimaryKey(BaseModel):
    name: str | None = None
    columns: list[str]


class ForeignKey(BaseModel):
    name: str | None = None
    columns: list[str]
    ref_schema: str
    ref_table: str
    ref_columns: list[str]


class Table(BaseModel):
    name: str
    table_type: TableType | None = None
    comment: str | None = None
    columns: list[Column] = Field(default_factory=list)
    primary_key: PrimaryKey | None = None
    foreign_keys: list[ForeignKey] = Field(default_factory=list)
    tags: dict[str, str] = Field(default_factory=dict)
    storage_location: str | None = None


class Schema(BaseModel):
    name: str
    comment: str | None = None
    tables: list[Table] = Field(default_factory=list)
    tags: dict[str, str] = Field(default_factory=dict)


class Catalog(BaseModel):
    name: str
    comment: str | None = None
    schemas: list[Schema] = Field(default_factory=list)
    tags: dict[str, str] = Field(default_factory=dict)
