from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from metadata_etl.config import ETLConfig
from metadata_etl.connectors.base import BaseConnector, ExtractedSource
from metadata_etl.errors import ExtractionError
from metadata_etl.schema import raw_json_schema_fingerprint
from metadata_etl.source import preserve_raw_copy
from metadata_etl.sql_compiler import csv_relation


def _read_json(path: Path) -> Any:
    try:
        text = path.read_text(encoding="utf-8-sig")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return [json.loads(line) for line in text.splitlines() if line.strip()]
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ExtractionError(f"Could not read JSON source {path}: {exc}") from exc


def _resolve(value: Any, path: str) -> Any:
    current = value
    for part in path.split(".") if path else ():
        if not isinstance(current, dict) or part not in current:
            raise ExtractionError(f"Configured JSON path {path!r} does not exist")
        current = current[part]
    return current


def _scalar(value: Any, path: str) -> Any:
    if isinstance(value, dict | list):
        raise ExtractionError(
            f"Configured JSON field {path!r} resolves to nested data; map or explode it explicitly"
        )
    return value


def _records(payload: Any, root_path: str | None) -> list[dict[str, Any]]:
    rooted = _resolve(payload, root_path) if root_path else payload
    if isinstance(rooted, dict):
        rooted = [rooted]
    if not isinstance(rooted, list) or not all(isinstance(item, dict) for item in rooted):
        raise ExtractionError("JSON root must resolve to an object or array of objects")
    return rooted


def _normalize(config: ETLConfig, payload: Any) -> tuple[list[str], list[list[Any]]]:
    normalization = config.json_normalization
    records = _records(payload, normalization.root_path if normalization else None)
    if normalization is None:
        if any(
            any(isinstance(value, dict | list) for value in record.values()) for record in records
        ):
            raise ExtractionError(
                "Nested JSON requires explicit normalization.json field and explode configuration"
            )
        names = [column.source for column in config.columns]
        rows = [[_scalar(_resolve(record, name), name) for name in names] for record in records]
        return names, rows

    base_fields = list(normalization.fields)
    explode = normalization.explode
    names = [name for name, _ in base_fields]
    if explode:
        names.extend(name for name, _ in explode.fields)
    rows: list[list[Any]] = []
    for record in records:
        base = [_scalar(_resolve(record, path), path) for _, path in base_fields]
        if not explode:
            rows.append(base)
            continue
        items = _resolve(record, explode.path)
        if not isinstance(items, list):
            raise ExtractionError(f"JSON explode path {explode.path!r} must resolve to an array")
        for item in items:
            context = dict(record)
            context[explode.alias] = item
            exploded = [_scalar(_resolve(context, path), path) for _, path in explode.fields]
            rows.append(base + exploded)
    return names, rows


class JSONConnector(BaseConnector):
    source_type = "json"

    def extract(self, config: ETLConfig, run_id: str, raw_root: Path) -> ExtractedSource:
        assert config.source_path is not None
        artifact = preserve_raw_copy(config.source_path, raw_root, config.dataset, run_id)
        payload = _read_json(artifact.path)
        names, rows = _normalize(config, payload)
        normalized_path = artifact.path.with_name(f"{artifact.path.stem}__relational.csv")
        try:
            with normalized_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerow(names)
                writer.writerows(rows)
        except OSError as exc:
            raise ExtractionError(f"Could not materialize normalized JSON rows: {exc}") from exc
        return ExtractedSource(
            artifact,
            csv_relation(normalized_path, config.delimiter),
            raw_json_schema_fingerprint(payload),
        )
