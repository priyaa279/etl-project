from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb

from metadata_etl.config import ColumnConfig
from metadata_etl.errors import ExtractionError

INTEGER = re.compile(r"^[+-]?\d+$")
DECIMAL = re.compile(r"^[+-]?(?:\d+\.\d*|\d*\.\d+)$")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?$"
)
BOOLEAN_VALUES = {"true", "false"}


@dataclass(frozen=True)
class SchemaField:
    name: str
    datatype: str
    position: int
    nullable: bool | None = None
    date_format: str | None = None


@dataclass(frozen=True)
class SchemaFingerprint:
    level: str
    fields: tuple[SchemaField, ...]
    hash: str

    @property
    def payload(self) -> dict[str, object]:
        return {"level": self.level, "fields": [asdict(field) for field in self.fields]}

    @property
    def json(self) -> str:
        return json.dumps(self.payload, sort_keys=True, separators=(",", ":"))

    @classmethod
    def build(cls, level: str, fields: tuple[SchemaField, ...]) -> SchemaFingerprint:
        payload = {"level": level, "fields": [asdict(field) for field in fields]}
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return cls(level, fields, hashlib.sha256(encoded).hexdigest())

    @classmethod
    def from_json(cls, value: str) -> SchemaFingerprint:
        payload = json.loads(value)
        fields = tuple(SchemaField(**field) for field in payload["fields"])
        return cls.build(payload["level"], fields)


def _value_representation(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        return "empty"
    if len(stripped) > 1 and stripped[0] == "0" and stripped.isdigit():
        return "string"
    if ISO_TIMESTAMP.fullmatch(stripped):
        return "iso_timestamp"
    if ISO_DATE.fullmatch(stripped):
        return "iso_date"
    if stripped.lower() in BOOLEAN_VALUES:
        return "boolean"
    if INTEGER.fullmatch(stripped):
        return "integer"
    if DECIMAL.fullmatch(stripped):
        return "decimal"
    return "string"


def _column_representation(values: list[str]) -> str:
    representations = {_value_representation(value) for value in values}
    representations.discard("empty")
    if not representations:
        return "empty"
    if representations <= {"integer", "decimal"}:
        return "decimal" if "decimal" in representations else "integer"
    if len(representations) == 1:
        return next(iter(representations))
    return "mixed"


def raw_csv_schema_fingerprint(
    path: Path, delimiter: str = ",", *, sample_size: int = 10_000
) -> SchemaFingerprint:
    """Fingerprint CSV headers, order, and observed physical value representations."""
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle, delimiter=delimiter)
            header = next(reader)
            if not header:
                raise ExtractionError(f"CSV source has no header: {path}")
            samples: list[list[str]] = [[] for _ in header]
            for row_number, row in enumerate(reader, start=2):
                if not row:
                    continue
                if len(row) != len(header):
                    raise ExtractionError(
                        f"CSV row {row_number} has {len(row)} values; expected {len(header)}"
                    )
                if row_number <= sample_size + 1:
                    for index, value in enumerate(row):
                        samples[index].append(value)
    except (OSError, UnicodeError, csv.Error) as exc:
        raise ExtractionError(f"Could not inspect raw CSV schema at {path}: {exc}") from exc

    fields = tuple(
        SchemaField(name, _column_representation(samples[index]), index)
        for index, name in enumerate(header)
    )
    return SchemaFingerprint.build("raw", fields)


def canonical_schema_fingerprint(columns: tuple[ColumnConfig, ...]) -> SchemaFingerprint:
    """Fingerprint the approved normalized names and datatypes, independent of source names."""
    fields = tuple(
        SchemaField(column.name, column.datatype, index, column.nullable, column.date_format)
        for index, column in enumerate(columns)
    )
    return SchemaFingerprint.build("canonical", fields)


def raw_json_schema_fingerprint(payload: Any) -> SchemaFingerprint:
    """Fingerprint nested JSON paths and observed container/scalar representations."""
    observed: dict[str, set[str]] = {}

    def visit(value: Any, path: str) -> None:
        if value is None:
            kind = "null"
        elif isinstance(value, bool):
            kind = "boolean"
        elif isinstance(value, int):
            kind = "integer"
        elif isinstance(value, float):
            kind = "decimal"
        elif isinstance(value, str):
            kind = "string"
        elif isinstance(value, dict):
            kind = "object"
        elif isinstance(value, list):
            kind = "array"
        else:
            kind = type(value).__name__
        observed.setdefault(path, set()).add(kind)
        if isinstance(value, dict):
            for key in sorted(value):
                visit(value[key], f"{path}.{key}")
        elif isinstance(value, list):
            for item in value:
                visit(item, f"{path}[]")

    visit(payload, "$")
    fields = tuple(
        SchemaField(path, "|".join(sorted(types)), index)
        for index, (path, types) in enumerate(sorted(observed.items()))
    )
    return SchemaFingerprint.build("raw", fields)


def parquet_schema_fingerprint(path: Path) -> SchemaFingerprint:
    try:
        with duckdb.connect(":memory:") as connection:
            escaped = str(path.as_posix()).replace("'", "''")
            rows = connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{escaped}')"
            ).fetchall()
    except duckdb.Error as exc:
        raise ExtractionError(f"Could not inspect Parquet schema at {path}: {exc}") from exc
    fields = tuple(
        SchemaField(str(row[0]), str(row[1]).lower(), index) for index, row in enumerate(rows)
    )
    return SchemaFingerprint.build("raw", fields)
