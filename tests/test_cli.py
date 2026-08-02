from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import pytest
from databricks.sdk.service.catalog import TableType

from databricks_schema.cli import _json_default, main
from databricks_schema.models import Column, PrimaryKey

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
