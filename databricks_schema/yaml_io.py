from __future__ import annotations

from typing import Any

import yaml

from databricks_schema.models import Catalog, Schema


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
