import yaml

from dbx_schema.models import Catalog, Column, ForeignKey, PrimaryKey, Schema, Table, TableType
from dbx_schema.yaml_io import (
    catalog_from_yaml,
    catalog_to_yaml,
    schema_from_yaml,
    schema_to_yaml,
)


def _make_catalog() -> Catalog:
    col1 = Column(name="id", data_type="BIGINT", nullable=False, comment="primary key col")
    col2 = Column(name="name", data_type="STRING", nullable=True)
    pk = PrimaryKey(name="pk_users", columns=["id"])
    fk = ForeignKey(
        name="fk_org",
        columns=["org_id"],
        ref_schema="orgs",
        ref_table="organizations",
        ref_columns=["id"],
    )
    table = Table(
        name="users",
        table_type=TableType.MANAGED,
        comment="User accounts",
        columns=[col1, col2],
        primary_key=pk,
        foreign_keys=[fk],
        tags={"domain": "identity"},
    )
    schema = Schema(name="main", comment="Main schema", tables=[table], tags={"env": "prod"})
    return Catalog(name="prod", comment="Production catalog", schemas=[schema])


class TestSchemaYamlRoundTrip:
    def test_round_trip(self):
        original = _make_catalog().schemas[0]
        text = schema_to_yaml(original)
        restored = schema_from_yaml(text)
        assert restored.name == original.name
        assert restored.comment == original.comment
        assert len(restored.tables) == 1
        t = restored.tables[0]
        assert t.name == "users"
        assert t.table_type == TableType.MANAGED
        assert len(t.columns) == 2

    def test_nullable_false_preserved(self):
        original = _make_catalog().schemas[0]
        text = schema_to_yaml(original)
        restored = schema_from_yaml(text)
        id_col = restored.tables[0].columns[0]
        assert id_col.name == "id"
        assert id_col.nullable is False

    def test_none_fields_absent_from_yaml(self):
        schema = Schema(name="empty")
        text = schema_to_yaml(schema)
        data = yaml.safe_load(text)
        assert "comment" not in data
        assert "tags" not in data
        assert "tables" not in data

    def test_empty_tags_absent(self):
        table = Table(name="t")
        schema = Schema(name="s", tables=[table])
        text = schema_to_yaml(schema)
        data = yaml.safe_load(text)
        # tags dict is empty → should be stripped
        assert "tags" not in data
        assert "tags" not in data["tables"][0]

    def test_empty_foreign_keys_absent(self):
        table = Table(name="t")
        schema = Schema(name="s", tables=[table])
        text = schema_to_yaml(schema)
        data = yaml.safe_load(text)
        assert "foreign_keys" not in data["tables"][0]


class TestCatalogYamlRoundTrip:
    def test_round_trip(self):
        original = _make_catalog()
        text = catalog_to_yaml(original)
        restored = catalog_from_yaml(text)
        assert restored.name == "prod"
        assert len(restored.schemas) == 1
        assert restored.schemas[0].name == "main"

    def test_tags_preserved(self):
        original = _make_catalog()
        text = catalog_to_yaml(original)
        restored = catalog_from_yaml(text)
        assert restored.schemas[0].tags == {"env": "prod"}
        assert restored.schemas[0].tables[0].tags == {"domain": "identity"}

    def test_pk_fk_preserved(self):
        original = _make_catalog()
        text = catalog_to_yaml(original)
        restored = catalog_from_yaml(text)
        table = restored.schemas[0].tables[0]
        assert table.primary_key is not None
        assert table.primary_key.columns == ["id"]
        assert len(table.foreign_keys) == 1
        fk = table.foreign_keys[0]
        assert fk.ref_schema == "orgs"
        assert fk.ref_table == "organizations"

    def test_none_catalog_comment_absent(self):
        cat = Catalog(name="x")
        text = catalog_to_yaml(cat)
        data = yaml.safe_load(text)
        assert "comment" not in data
