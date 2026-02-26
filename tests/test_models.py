import pytest
from databricks.sdk.service.catalog import TableType
from pydantic import ValidationError

from databricks_schema.models import (
    Catalog,
    Column,
    ForeignKey,
    PrimaryKey,
    Schema,
    Table,
)


class TestColumn:
    def test_defaults(self):
        col = Column(name="id", data_type="BIGINT")
        assert col.nullable is True
        assert col.comment is None
        assert col.tags == {}

    def test_nullable_false(self):
        col = Column(name="id", data_type="BIGINT", nullable=False)
        assert col.nullable is False

    def test_with_comment_and_tags(self):
        col = Column(name="x", data_type="STRING", comment="desc", tags={"owner": "team"})
        assert col.comment == "desc"
        assert col.tags == {"owner": "team"}

    def test_required_fields(self):
        with pytest.raises(ValidationError):
            Column(name="id")  # missing data_type


class TestTableType:
    def test_enum_values(self):
        assert TableType.MANAGED.value == "MANAGED"
        assert TableType.EXTERNAL.value == "EXTERNAL"
        assert TableType.VIEW.value == "VIEW"
        assert TableType.MATERIALIZED_VIEW.value == "MATERIALIZED_VIEW"
        assert TableType.STREAMING_TABLE.value == "STREAMING_TABLE"

    def test_from_string(self):
        assert TableType("VIEW") is TableType.VIEW

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            TableType("INVALID")


class TestPrimaryKey:
    def test_basic(self):
        pk = PrimaryKey(columns=["id"])
        assert pk.name is None
        assert pk.columns == ["id"]

    def test_with_name(self):
        pk = PrimaryKey(name="pk_orders", columns=["order_id"])
        assert pk.name == "pk_orders"


class TestForeignKey:
    def test_basic(self):
        fk = ForeignKey(
            columns=["user_id"],
            ref_schema="users",
            ref_table="accounts",
            ref_columns=["id"],
        )
        assert fk.name is None
        assert fk.ref_schema == "users"
        assert fk.ref_table == "accounts"


class TestTable:
    def test_defaults(self):
        t = Table(name="orders")
        assert t.table_type is None
        assert t.columns == []
        assert t.foreign_keys == []
        assert t.primary_key is None
        assert t.tags == {}
        assert t.storage_location is None
        assert t.owner is None

    def test_with_type(self):
        t = Table(name="v", table_type=TableType.VIEW)
        assert t.table_type is TableType.VIEW

    def test_owner(self):
        t = Table(name="t", owner="alice")
        assert t.owner == "alice"


class TestSchema:
    def test_defaults(self):
        s = Schema(name="main")
        assert s.tables == []
        assert s.tags == {}
        assert s.comment is None


class TestCatalog:
    def test_round_trip(self):
        col = Column(name="id", data_type="BIGINT", nullable=False)
        pk = PrimaryKey(columns=["id"])
        table = Table(name="users", columns=[col], primary_key=pk, table_type=TableType.MANAGED)
        schema = Schema(name="main", tables=[table])
        catalog = Catalog(name="prod", schemas=[schema])

        data = catalog.model_dump(mode="json")
        restored = Catalog.model_validate(data)

        assert restored.name == "prod"
        assert restored.schemas[0].name == "main"
        assert restored.schemas[0].tables[0].columns[0].nullable is False
        assert restored.schemas[0].tables[0].primary_key.columns == ["id"]
