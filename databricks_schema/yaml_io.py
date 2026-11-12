from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

import yaml

from databricks_schema.dbml_io import DbmlParseError, schemas_from_dbml
from databricks_schema.models import Catalog, Schema

SchemaFormat = Literal["yaml", "json", "dbml"]
FORMAT_EXTENSIONS: dict[str, str] = {"yaml": ".yaml", "json": ".json", "dbml": ".dbml"}


def _strip_empty(obj: Any) -> Any:
    """Recursively remove None values and empty dicts/lists.

    Preserves False, 0, and empty strings as intentional values.
    """
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            stripped = _strip_empty(v)
            if stripped is None:
                continue
            if isinstance(stripped, (dict, list)) and len(stripped) == 0:
                continue
            result[k] = stripped
        return result
    if isinstance(obj, list):
        return [_strip_empty(item) for item in obj]
    return obj


def _to_yaml(data: dict) -> str:
    return yaml.dump(
        _strip_empty(data),
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )


def schema_to_yaml(schema: Schema) -> str:
    return _to_yaml(schema.model_dump(mode="json"))


def schema_from_yaml(text: str) -> Schema:
    data = yaml.safe_load(text)
    return Schema.model_validate(data)


def catalog_to_yaml(catalog: Catalog) -> str:
    return _to_yaml(catalog.model_dump(mode="json"))


def catalog_from_yaml(text: str) -> Catalog:
    data = yaml.safe_load(text)
    return Catalog.model_validate(data)


def _to_json(data: dict) -> str:
    return json.dumps(_strip_empty(data), indent=2, ensure_ascii=False)


def schema_to_json(schema: Schema) -> str:
    return _to_json(schema.model_dump(mode="json"))


def schema_from_json(text: str) -> Schema:
    return Schema.model_validate(json.loads(text))


def catalog_to_json(catalog: Catalog) -> str:
    return _to_json(catalog.model_dump(mode="json"))


def catalog_from_json(text: str) -> Catalog:
    return Catalog.model_validate(json.loads(text))


def load_schema_dir(
    path: Path,
    fmt: SchemaFormat = "yaml",
    schema_names: frozenset[str] | None = None,
) -> dict[str, Schema]:
    """Load every schema stored in a directory, keyed by schema name.

    YAML/JSON directories hold one file per schema; ``schema_names`` filters on the file stem.
    DBML directories hold one or more DBML documents (typically one per catalog), each of which
    may define several schemas; ``schema_names`` filters on the schema name. A schema defined
    in more than one DBML file raises DbmlParseError.
    """
    files = sorted(path.glob(f"*{FORMAT_EXTENSIONS[fmt]}"))
    schemas: dict[str, Schema] = {}
    if fmt == "dbml":
        origin: dict[str, Path] = {}
        for f in files:
            for schema in schemas_from_dbml(f.read_text(encoding="utf-8")):
                if schema_names is not None and schema.name not in schema_names:
                    continue
                if schema.name in schemas:
                    raise DbmlParseError(
                        f"Schema '{schema.name}' is defined in both {origin[schema.name]} and {f}"
                    )
                schemas[schema.name] = schema
                origin[schema.name] = f
        return schemas
    loader = schema_from_json if fmt == "json" else schema_from_yaml
    for f in files:
        if schema_names is not None and f.stem not in schema_names:
            continue
        schema = loader(f.read_text(encoding="utf-8"))
        schemas[schema.name] = schema
    return schemas
