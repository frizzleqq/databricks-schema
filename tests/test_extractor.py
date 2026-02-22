from datetime import UTC
from unittest.mock import MagicMock

from databricks.sdk.service.catalog import TableType

from databricks_schema.extractor import CatalogExtractor


def _make_col(name, position, type_text=None, type_name=None, nullable=True, comment=None):
    col = MagicMock()
    col.name = name
    col.position = position
    col.type_text = type_text
    col.type_name = type_name
    col.nullable = nullable
    col.comment = comment
    col.tags = {}
    return col


def _make_table_summary(name):
    ts = MagicMock()
    ts.name = name
    return ts


def _make_sdk_table(
    name,
    table_type_val=None,
    columns=None,
    constraints=None,
    comment=None,
    owner=None,
    created_at=None,
):
    t = MagicMock()
    t.name = name
    t.comment = comment
    t.tags = {}
    t.storage_location = None
    t.columns = columns or []
    t.table_constraints = constraints or []
    t.owner = owner
    t.created_at = created_at
    t.table_type = TableType(table_type_val) if table_type_val else None
    return t


def _make_constraint(pk=None, fk=None):
    c = MagicMock()
    c.primary_key_constraint = pk
    c.foreign_key_constraint = fk
    return c


class TestCatalogExtractor:
    def _extractor(self):
        client = MagicMock()
        return CatalogExtractor(client=client), client

    def test_empty_catalog(self):
        extractor, client = self._extractor()
        sdk_catalog = MagicMock()
        sdk_catalog.comment = None
        sdk_catalog.tags = {}
        client.catalogs.get.return_value = sdk_catalog
        client.schemas.list.return_value = []

        catalog = extractor.extract_catalog("mycat")
        assert catalog.name == "mycat"
        assert catalog.schemas == []
        client.catalogs.get.assert_called_once_with("mycat")

    def test_schema_filter(self):
        extractor, client = self._extractor()
        sdk_catalog = MagicMock()
        sdk_catalog.comment = None
        sdk_catalog.tags = {}
        client.catalogs.get.return_value = sdk_catalog

        s1 = MagicMock()
        s1.name = "keep"
        s1.comment = None
        s1.owner = None
        s1.tags = {}
        s2 = MagicMock()
        s2.name = "skip"
        s2.comment = None
        s2.owner = None
        s2.tags = {}
        client.schemas.list.return_value = [s1, s2]
        client.tables.list.return_value = []

        catalog = extractor.extract_catalog("mycat", schema_filter=["keep"])
        assert len(catalog.schemas) == 1
        assert catalog.schemas[0].name == "keep"

    def test_system_schema_skipped_by_default(self):
        extractor, client = self._extractor()
        sdk_catalog = MagicMock()
        sdk_catalog.comment = None
        sdk_catalog.tags = {}
        client.catalogs.get.return_value = sdk_catalog

        sys_schema = MagicMock()
        sys_schema.name = "information_schema"
        sys_schema.comment = None
        sys_schema.owner = None
        sys_schema.tags = {}
        client.schemas.list.return_value = [sys_schema]

        catalog = extractor.extract_catalog("mycat", skip_system_schemas=True)
        assert catalog.schemas == []

    def test_system_schema_included_when_flag_off(self):
        extractor, client = self._extractor()
        sdk_catalog = MagicMock()
        sdk_catalog.comment = None
        sdk_catalog.tags = {}
        client.catalogs.get.return_value = sdk_catalog

        sys_schema = MagicMock()
        sys_schema.name = "information_schema"
        sys_schema.comment = None
        sys_schema.owner = None
        sys_schema.tags = {}
        client.schemas.list.return_value = [sys_schema]
        client.tables.list.return_value = []

        catalog = extractor.extract_catalog("mycat", skip_system_schemas=False)
        assert len(catalog.schemas) == 1

    def test_columns_sorted_by_position(self):
        extractor, client = self._extractor()
        sdk_catalog = MagicMock()
        sdk_catalog.comment = None
        sdk_catalog.tags = {}
        client.catalogs.get.return_value = sdk_catalog

        s = MagicMock()
        s.name = "main"
        s.comment = None
        s.owner = None
        s.tags = {}
        client.schemas.list.return_value = [s]

        client.tables.list.return_value = [_make_table_summary("orders")]

        col_a = _make_col("a", position=2, type_text="STRING")
        col_b = _make_col("b", position=1, type_text="BIGINT")
        col_c = _make_col("c", position=None, type_text="BOOLEAN")  # None → 9999

        sdk_table = _make_sdk_table("orders", columns=[col_a, col_b, col_c])
        client.tables.get.return_value = sdk_table

        catalog = extractor.extract_catalog("mycat")
        cols = catalog.schemas[0].tables[0].columns
        assert [c.name for c in cols] == ["b", "a", "c"]

    def test_pk_constraint(self):
        extractor, client = self._extractor()
        sdk_catalog = MagicMock()
        sdk_catalog.comment = None
        sdk_catalog.tags = {}
        client.catalogs.get.return_value = sdk_catalog

        s = MagicMock()
        s.name = "main"
        s.comment = None
        s.owner = None
        s.tags = {}
        client.schemas.list.return_value = [s]
        client.tables.list.return_value = [_make_table_summary("orders")]

        pk_c = MagicMock()
        pk_c.name = "pk_orders"
        pk_c.child_columns = ["order_id"]
        constraint = _make_constraint(pk=pk_c)

        sdk_table = _make_sdk_table("orders", constraints=[constraint])
        client.tables.get.return_value = sdk_table

        catalog = extractor.extract_catalog("mycat")
        table = catalog.schemas[0].tables[0]
        assert table.primary_key is not None
        assert table.primary_key.name == "pk_orders"
        assert table.primary_key.columns == ["order_id"]

    def test_fk_constraint(self):
        extractor, client = self._extractor()
        sdk_catalog = MagicMock()
        sdk_catalog.comment = None
        sdk_catalog.tags = {}
        client.catalogs.get.return_value = sdk_catalog

        s = MagicMock()
        s.name = "main"
        s.comment = None
        s.owner = None
        s.tags = {}
        client.schemas.list.return_value = [s]
        client.tables.list.return_value = [_make_table_summary("orders")]

        fk_c = MagicMock()
        fk_c.name = "fk_user"
        fk_c.child_columns = ["user_id"]
        fk_c.parent_table = "prod.users.accounts"
        fk_c.parent_columns = ["id"]
        constraint = _make_constraint(fk=fk_c)

        sdk_table = _make_sdk_table("orders", constraints=[constraint])
        client.tables.get.return_value = sdk_table

        catalog = extractor.extract_catalog("mycat")
        table = catalog.schemas[0].tables[0]
        assert len(table.foreign_keys) == 1
        fk = table.foreign_keys[0]
        assert fk.ref_schema == "users"
        assert fk.ref_table == "accounts"
        assert fk.ref_columns == ["id"]
        assert fk.columns == ["user_id"]

    def test_owner_and_created_at_extracted(self):

        extractor, client = self._extractor()
        sdk_catalog = MagicMock()
        sdk_catalog.comment = None
        sdk_catalog.tags = {}
        client.catalogs.get.return_value = sdk_catalog

        s = MagicMock()
        s.name = "main"
        s.comment = None
        s.owner = None
        s.tags = {}
        client.schemas.list.return_value = [s]
        client.tables.list.return_value = [_make_table_summary("t")]

        created_at_ms = 1_700_000_000_000  # milliseconds
        sdk_table = _make_sdk_table("t", owner="alice", created_at=created_at_ms)
        client.tables.get.return_value = sdk_table

        catalog = extractor.extract_catalog("mycat")
        table = catalog.schemas[0].tables[0]
        assert table.owner == "alice"
        assert table.created_at is not None
        assert table.created_at.tzinfo == UTC
        assert int(table.created_at.timestamp() * 1000) == created_at_ms

    def test_schema_owner_extracted(self):
        extractor, client = self._extractor()
        sdk_catalog = MagicMock()
        sdk_catalog.comment = None
        sdk_catalog.tags = {}
        client.catalogs.get.return_value = sdk_catalog

        s = MagicMock()
        s.name = "main"
        s.comment = None
        s.owner = "data_team"
        s.tags = {}
        client.schemas.list.return_value = [s]
        client.tables.list.return_value = []

        catalog = extractor.extract_catalog("mycat")
        assert catalog.schemas[0].owner == "data_team"

    def test_table_type_extracted(self):
        extractor, client = self._extractor()
        sdk_catalog = MagicMock()
        sdk_catalog.comment = None
        sdk_catalog.tags = {}
        client.catalogs.get.return_value = sdk_catalog

        s = MagicMock()
        s.name = "main"
        s.comment = None
        s.owner = None
        s.tags = {}
        client.schemas.list.return_value = [s]
        client.tables.list.return_value = [_make_table_summary("t")]

        sdk_table = _make_sdk_table("t", table_type_val="EXTERNAL")
        client.tables.get.return_value = sdk_table

        catalog = extractor.extract_catalog("mycat")
        assert catalog.schemas[0].tables[0].table_type == TableType.EXTERNAL
