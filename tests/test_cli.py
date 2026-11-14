from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from databricks.sdk.service.catalog import TableType

from databricks_schema.cli import _json_default, _serialize_structured, main
from databricks_schema.dbml_io import catalog_from_dbml, catalog_to_dbml
from databricks_schema.models import Catalog, Column, PrimaryKey, Schema, Table
from databricks_schema.yaml_io import schema_to_yaml

# Importing cli disables propagation on the "databricks_schema" logger for real CLI runs;
# undo that here so other test modules' caplog-based assertions still see log records.
logging.getLogger("databricks_schema").propagate = True


def _run(monkeypatch, argv: list[str]) -> int:
    monkeypatch.setattr(sys, "argv", ["databricks-schema", *argv])
    try:
        main()
    except SystemExit as e:
        return e.code or 0
    return 0


def _write_schema(path: Path, name: str, yaml_text: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / f"{name}.yaml").write_text(yaml_text, encoding="utf-8")


class TestJsonDefault:
    def test_pydantic_model_dumped(self):
        assert _json_default(Column(name="id", data_type="bigint")) == {
            "name": "id",
            "data_type": "bigint",
            "comment": None,
            "nullable": True,
            "tags": {},
        }

    def test_enum_dumped_as_value(self):
        assert _json_default(TableType.MANAGED) == "MANAGED"

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError):
            _json_default(object())


class TestJsonSchemaCommand:
    @pytest.mark.parametrize("model", ["catalog", "schema", "diff", "validate"])
    def test_prints_valid_json_schema(self, monkeypatch, capsys, model):
        exit_code = _run(monkeypatch, ["json-schema", model])
        assert exit_code == 0
        data = json.loads(capsys.readouterr().out)
        assert data["type"] == "object"
        assert "properties" in data

    def test_rejects_unknown_model(self, monkeypatch, capsys):
        with pytest.raises(SystemExit) as exc_info:
            monkeypatch.setattr(sys, "argv", ["databricks-schema", "json-schema", "bogus"])
            main()
        assert exc_info.value.code == 2

    def test_format_yaml_matches_json_shape(self, monkeypatch, capsys):
        _run(monkeypatch, ["json-schema", "validate"])
        json_data = json.loads(capsys.readouterr().out)

        _run(monkeypatch, ["json-schema", "validate", "--format", "yaml"])
        yaml_data = yaml.safe_load(capsys.readouterr().out)

        assert yaml_data == json_data


class TestSerializeStructured:
    def test_yaml_and_json_carry_the_same_data(self):
        data = {"changes": [{"field": "data_type", "old": "bigint", "new": "int"}]}

        json_out = _serialize_structured(data, "json")
        yaml_out = _serialize_structured(data, "yaml")

        assert json.loads(json_out) == data
        assert yaml.safe_load(yaml_out) == data

    def test_yaml_resolves_pydantic_models_and_enums(self):
        data = {"pk": PrimaryKey(columns=["id"]), "table_type": TableType.MANAGED}

        yaml_out = _serialize_structured(data, "yaml")

        assert yaml.safe_load(yaml_out) == {
            "pk": {"name": None, "columns": ["id"]},
            "table_type": "MANAGED",
        }


class TestDiffFilesJson:
    def test_no_changes_exits_0_with_empty_schemas(self, tmp_path, monkeypatch, capsys):
        dir1, dir2 = tmp_path / "old", tmp_path / "new"
        _write_schema(dir1, "main", "name: main\n")
        _write_schema(dir2, "main", "name: main\n")

        exit_code = _run(
            monkeypatch, ["diff-files", str(dir1), str(dir2), "--format", "json", "--quiet"]
        )

        assert exit_code == 0
        data = json.loads(capsys.readouterr().out)
        assert data == {
            "schemas": [{"name": "main", "status": "unchanged", "changes": [], "tables": []}]
        }

    def test_changes_exit_1_with_structured_diff(self, tmp_path, monkeypatch, capsys):
        dir1, dir2 = tmp_path / "old", tmp_path / "new"
        _write_schema(
            dir1,
            "main",
            "name: main\ntables:\n  - name: users\n    columns:\n"
            "      - name: id\n        data_type: bigint\n",
        )
        _write_schema(
            dir2,
            "main",
            "name: main\ntables:\n  - name: users\n    columns:\n"
            "      - name: id\n        data_type: int\n",
        )

        exit_code = _run(
            monkeypatch, ["diff-files", str(dir1), str(dir2), "--format", "json", "--quiet"]
        )

        assert exit_code == 1
        data = json.loads(capsys.readouterr().out)
        assert data["schemas"][0]["status"] == "modified"
        col_change = data["schemas"][0]["tables"][0]["columns"][0]
        assert col_change == {
            "name": "id",
            "status": "modified",
            "changes": [{"field": "data_type", "old": "bigint", "new": "int"}],
        }

    def test_serializes_pydantic_fields_in_changes(self, tmp_path, monkeypatch, capsys):
        dir1, dir2 = tmp_path / "old", tmp_path / "new"
        _write_schema(
            dir1,
            "main",
            "name: main\ntables:\n  - name: users\n    columns:\n"
            "      - name: id\n        data_type: bigint\n",
        )
        _write_schema(
            dir2,
            "main",
            "name: main\ntables:\n  - name: users\n    primary_key:\n"
            "      columns: [id]\n    columns:\n      - name: id\n        data_type: bigint\n",
        )

        exit_code = _run(
            monkeypatch, ["diff-files", str(dir1), str(dir2), "--format", "json", "--quiet"]
        )

        assert exit_code == 1
        data = json.loads(capsys.readouterr().out)
        table_changes = {c["field"]: c for c in data["schemas"][0]["tables"][0]["changes"]}
        assert table_changes["primary_key"]["old"] is None
        assert table_changes["primary_key"]["new"] == PrimaryKey(columns=["id"]).model_dump(
            mode="json"
        )

    def test_format_yaml_matches_json_shape(self, tmp_path, monkeypatch, capsys):
        dir1, dir2 = tmp_path / "old", tmp_path / "new"
        _write_schema(
            dir1,
            "main",
            "name: main\ntables:\n  - name: users\n    columns:\n"
            "      - name: id\n        data_type: bigint\n",
        )
        _write_schema(
            dir2,
            "main",
            "name: main\ntables:\n  - name: users\n    columns:\n"
            "      - name: id\n        data_type: int\n",
        )

        exit_code = _run(
            monkeypatch, ["diff-files", str(dir1), str(dir2), "--format", "json", "--quiet"]
        )
        json_data = json.loads(capsys.readouterr().out)
        assert exit_code == 1

        exit_code = _run(
            monkeypatch, ["diff-files", str(dir1), str(dir2), "--format", "yaml", "--quiet"]
        )
        yaml_data = yaml.safe_load(capsys.readouterr().out)
        assert exit_code == 1

        assert yaml_data == json_data


class _FakeExtractor:
    """Stand-in for CatalogExtractor that filters an in-memory dict of Catalogs."""

    def __init__(self, catalogs: dict[str, Catalog]):
        self._catalogs = catalogs

    def extract_catalog(
        self,
        catalog_name,
        schema_filter=None,
        include_metadata=False,
        include_tags=False,
        table_filter=None,
        include_grants=False,
    ) -> Catalog:
        catalog = self._catalogs[catalog_name]
        schemas = []
        for s in catalog.schemas:
            if schema_filter and s.name not in schema_filter:
                continue
            tables = s.tables
            if table_filter:
                tables = [t for t in tables if t.name in table_filter]
            schemas.append(
                Schema(name=s.name, comment=s.comment, owner=s.owner, tables=tables, tags=s.tags)
            )
        return Catalog(
            name=catalog.name, comment=catalog.comment, schemas=schemas, tags=catalog.tags
        )


def _mock_extractor(monkeypatch, catalogs: dict[str, Catalog]) -> None:
    fake = _FakeExtractor(catalogs)
    monkeypatch.setattr(
        "databricks_schema.cli.CatalogExtractor", lambda client=None, max_workers=4: fake
    )
    monkeypatch.setattr("databricks_schema.cli._make_client", lambda host, token: None)


class TestDiffDottedArgs:
    def test_schema_level_diff_across_differently_named_schemas(self, monkeypatch, capsys):
        mycat = Catalog(
            name="mycat",
            schemas=[
                Schema(name="orders", comment="new", tables=[]),
                Schema(name="orders_test", comment="old", tables=[]),
            ],
        )
        _mock_extractor(monkeypatch, {"mycat": mycat})

        exit_code = _run(
            monkeypatch, ["diff", "mycat.orders", "mycat.orders_test", "--format", "json", "-q"]
        )

        assert exit_code == 1
        data = json.loads(capsys.readouterr().out)
        assert len(data["schemas"]) == 1
        assert data["schemas"][0]["name"] == "orders"
        assert data["schemas"][0]["status"] == "modified"
        assert data["schemas"][0]["changes"] == [{"field": "comment", "old": "old", "new": "new"}]

    def test_table_level_diff_across_catalogs_and_names(self, monkeypatch, capsys):
        cat1 = Catalog(
            name="mycat",
            schemas=[
                Schema(
                    name="sales",
                    tables=[
                        Table(
                            name="orders",
                            comment="new",
                            columns=[Column(name="id", data_type="bigint")],
                        )
                    ],
                )
            ],
        )
        cat2 = Catalog(
            name="othercat",
            schemas=[
                Schema(
                    name="sales",
                    tables=[
                        Table(
                            name="orders_v2",
                            comment="old",
                            columns=[Column(name="id", data_type="bigint")],
                        )
                    ],
                )
            ],
        )
        _mock_extractor(monkeypatch, {"mycat": cat1, "othercat": cat2})

        exit_code = _run(
            monkeypatch,
            [
                "diff",
                "mycat.sales.orders",
                "othercat.sales.orders_v2",
                "--format",
                "json",
                "-q",
            ],
        )

        assert exit_code == 1
        data = json.loads(capsys.readouterr().out)
        assert data["schemas"][0]["status"] == "modified"
        table_diff = data["schemas"][0]["tables"][0]
        assert table_diff["name"] == "orders"
        assert table_diff["changes"] == [{"field": "comment", "old": "old", "new": "new"}]

    def test_table_level_no_changes_reports_unchanged(self, monkeypatch, capsys):
        table = Table(name="orders", columns=[Column(name="id", data_type="bigint")])
        cat = Catalog(name="mycat", schemas=[Schema(name="sales", tables=[table])])
        _mock_extractor(monkeypatch, {"mycat": cat})

        exit_code = _run(
            monkeypatch,
            ["diff", "mycat.sales.orders", "mycat.sales.orders", "--format", "json", "-q"],
        )

        assert exit_code == 0
        data = json.loads(capsys.readouterr().out)
        assert data["schemas"][0]["status"] == "unchanged"
        assert data["schemas"][0]["tables"] == []

    def test_depth_mismatch_exits_2(self, monkeypatch, capsys):
        exit_code = _run(monkeypatch, ["diff", "mycat.orders", "mycat", "-q"])
        assert exit_code == 2
        assert "same depth" in capsys.readouterr().err

    def test_schema_flag_conflicts_with_dotted_schema(self, monkeypatch, capsys):
        exit_code = _run(
            monkeypatch, ["diff", "mycat.orders", "mycat.orders_test", "-s", "orders", "-q"]
        )
        assert exit_code == 2
        assert "--schema" in capsys.readouterr().err

    def test_directory_target_rejects_dotted_catalog(self, monkeypatch, capsys, tmp_path):
        exit_code = _run(monkeypatch, ["diff", "mycat.orders", str(tmp_path), "-q"])
        assert exit_code == 2
        assert "directory target" in capsys.readouterr().err


class TestListCatalogsFormats:
    def _mock_client(self, monkeypatch, names):
        client = MagicMock()
        client.catalogs.list.return_value = [MagicMock(name=n) for n in names]
        for mock_catalog, n in zip(client.catalogs.list.return_value, names, strict=True):
            mock_catalog.name = n
        monkeypatch.setattr("databricks_schema.cli._make_client", lambda host, token: client)

    def test_json_and_yaml_both_list_sorted_names(self, monkeypatch, capsys):
        self._mock_client(monkeypatch, ["raw", "main"])

        _run(monkeypatch, ["list-catalogs", "--format", "json"])
        assert json.loads(capsys.readouterr().out) == ["main", "raw"]

        _run(monkeypatch, ["list-catalogs", "--format", "yaml"])
        assert yaml.safe_load(capsys.readouterr().out) == ["main", "raw"]


class TestValidateJson:
    def test_clean_schemas_exit_0(self, tmp_path, monkeypatch, capsys):
        _write_schema(
            tmp_path,
            "main",
            "name: main\ntables:\n  - name: users\n    columns:\n"
            "      - name: id\n        data_type: bigint\n",
        )

        exit_code = _run(monkeypatch, ["validate", str(tmp_path), "--format", "json"])

        assert exit_code == 0
        assert json.loads(capsys.readouterr().out) == {"issues": []}

    def test_issues_exit_1_with_structured_output(self, tmp_path, monkeypatch, capsys):
        _write_schema(
            tmp_path,
            "main",
            "name: main\ntables:\n  - name: users\n    primary_key:\n"
            "      columns: [missing]\n    columns:\n"
            "      - name: id\n        data_type: bigint\n",
        )

        exit_code = _run(monkeypatch, ["validate", str(tmp_path), "--format", "json"])

        assert exit_code == 1
        data = json.loads(capsys.readouterr().out)
        assert data["issues"] == [
            {
                "schema": "main",
                "table": "users",
                "message": "primary key references unknown column: 'missing'",
            }
        ]

    def test_format_yaml_matches_json_shape(self, tmp_path, monkeypatch, capsys):
        _write_schema(
            tmp_path,
            "main",
            "name: main\ntables:\n  - name: users\n    primary_key:\n"
            "      columns: [missing]\n    columns:\n"
            "      - name: id\n        data_type: bigint\n",
        )

        exit_code = _run(monkeypatch, ["validate", str(tmp_path), "--format", "json"])
        json_data = json.loads(capsys.readouterr().out)
        assert exit_code == 1

        exit_code = _run(monkeypatch, ["validate", str(tmp_path), "--format", "yaml"])
        yaml_data = yaml.safe_load(capsys.readouterr().out)
        assert exit_code == 1

        assert yaml_data == json_data


def _dbml_catalog() -> Catalog:
    return Catalog(
        name="mycat",
        comment="My catalog",
        schemas=[
            Schema(
                name="sales",
                comment="Sales",
                tables=[
                    Table(
                        name="orders",
                        table_type=TableType.MANAGED,
                        columns=[
                            Column(name="id", data_type="bigint", nullable=False),
                            Column(name="meta", data_type="struct<a:int>"),
                        ],
                        primary_key=PrimaryKey(columns=["id"]),
                    )
                ],
            ),
            Schema(name="crm"),
        ],
    )


class TestDbmlFormat:
    def test_extract_dbml_to_stdout(self, monkeypatch, capsys):
        _mock_extractor(monkeypatch, {"mycat": _dbml_catalog()})
        assert _run(monkeypatch, ["extract", "mycat", "-f", "dbml"]) == 0
        out = capsys.readouterr().out
        assert out.startswith('Project "mycat" {')
        assert catalog_from_dbml(out) == _dbml_catalog()

    def test_extract_dbml_writes_one_catalog_file(self, tmp_path, monkeypatch):
        _mock_extractor(monkeypatch, {"mycat": _dbml_catalog()})
        assert _run(monkeypatch, ["extract", "mycat", "-f", "dbml", "-o", str(tmp_path)]) == 0
        assert [p.name for p in tmp_path.iterdir()] == ["mycat.dbml"]
        text = (tmp_path / "mycat.dbml").read_text(encoding="utf-8")
        assert catalog_from_dbml(text) == _dbml_catalog()

    def test_diff_against_dbml_dir_no_changes(self, tmp_path, monkeypatch, capsys):
        _mock_extractor(monkeypatch, {"mycat": _dbml_catalog()})
        (tmp_path / "mycat.dbml").write_text(catalog_to_dbml(_dbml_catalog()), encoding="utf-8")
        assert _run(monkeypatch, ["diff", "mycat", str(tmp_path)]) == 0
        assert "No differences found." in capsys.readouterr().out

    def test_diff_against_dbml_dir_detects_change(self, tmp_path, monkeypatch, capsys):
        stored = _dbml_catalog()
        stored.schemas[0].tables[0].columns[1].data_type = "string"
        _mock_extractor(monkeypatch, {"mycat": _dbml_catalog()})
        (tmp_path / "mycat.dbml").write_text(catalog_to_dbml(stored), encoding="utf-8")
        assert _run(monkeypatch, ["diff", "mycat", str(tmp_path)]) == 1
        assert "data_type: 'string' -> 'struct<a:int>'" in capsys.readouterr().out

    def test_diff_files_dbml_against_yaml(self, tmp_path, monkeypatch, capsys):
        dbml_dir, yaml_dir = tmp_path / "dbml", tmp_path / "yaml"
        dbml_dir.mkdir()
        yaml_dir.mkdir()
        catalog = _dbml_catalog()
        (dbml_dir / "mycat.dbml").write_text(catalog_to_dbml(catalog), encoding="utf-8")
        for s in catalog.schemas:
            (yaml_dir / f"{s.name}.yaml").write_text(schema_to_yaml(s), encoding="utf-8")
        assert _run(monkeypatch, ["diff-files", str(dbml_dir), str(yaml_dir)]) == 0
        assert "No differences found." in capsys.readouterr().out

    def test_validate_dbml_dir_with_schema_filter(self, tmp_path, monkeypatch, capsys):
        (tmp_path / "mycat.dbml").write_text(catalog_to_dbml(_dbml_catalog()), encoding="utf-8")
        assert _run(monkeypatch, ["validate", str(tmp_path), "-s", "sales"]) == 0
        assert "1 schema(s) validated" in capsys.readouterr().out

    def test_mixed_dbml_and_yaml_exits_2(self, tmp_path, monkeypatch, capsys):
        (tmp_path / "mycat.dbml").write_text(catalog_to_dbml(_dbml_catalog()), encoding="utf-8")
        _write_schema(tmp_path, "sales", "name: sales\n")
        assert _run(monkeypatch, ["validate", str(tmp_path)]) == 2
        assert "mixed YAML and DBML files" in capsys.readouterr().err

    def test_invalid_dbml_exits_2(self, tmp_path, monkeypatch, capsys):
        (tmp_path / "bad.dbml").write_text("Table t {\n a int [nope]\n}\n", encoding="utf-8")
        assert _run(monkeypatch, ["validate", str(tmp_path)]) == 2
        assert "Invalid DBML" in capsys.readouterr().err

    def test_reading_dbml_without_extra_exits_2(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr("databricks_schema.dbml_io.PYDBML_AVAILABLE", False)
        (tmp_path / "mycat.dbml").write_text(catalog_to_dbml(_dbml_catalog()), encoding="utf-8")
        assert _run(monkeypatch, ["validate", str(tmp_path)]) == 2
        assert "databricks-schema[dbml]" in capsys.readouterr().err


class TestDiffFilesGrants:
    def test_grants_only_compared_with_flag(self, tmp_path, monkeypatch, capsys):
        dir1, dir2 = tmp_path / "old", tmp_path / "new"
        _write_schema(dir1, "main", "name: main\ngrants:\n  analysts: [USE_SCHEMA]\n")
        _write_schema(dir2, "main", "name: main\n")

        assert _run(monkeypatch, ["diff-files", str(dir1), str(dir2), "--quiet"]) == 0
        capsys.readouterr()

        exit_code = _run(
            monkeypatch,
            ["diff-files", str(dir1), str(dir2), "--include-grants", "-f", "json", "--quiet"],
        )
        assert exit_code == 1
        data = json.loads(capsys.readouterr().out)
        assert data["schemas"][0]["changes"] == [
            {"field": "grants", "old": {"analysts": ["USE_SCHEMA"]}, "new": {}}
        ]
