from __future__ import annotations

import csv
import json
import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import mean
from typing import Any

import duckdb

from metadata_etl.errors import ProfilingError

NULL_TOKEN_NAMES = {"", "null", "n/a", "na", "none", "nil"}
INTEGER_PATTERN = re.compile(r"^[+-]?\d+$")
DECIMAL_PATTERN = re.compile(r"^[+-]?(?:\d+\.\d+|\d+\.|\.\d+)$")
LEADING_ZERO_PATTERN = re.compile(r"^[+-]?0\d+$")
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
YEAR_FIRST_DATE_PATTERN = re.compile(r"^\d{4}/\d{2}/\d{2}$")
SLASH_DATE_PATTERN = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
ISO_TIMESTAMP_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$")
TRUE_FALSE_VALUES = {"true", "false"}
OBSERVED_EXAMPLE_MAX_LENGTH = 120


@dataclass(frozen=True)
class Inference:
    confidence: str
    reason: str


@dataclass(frozen=True)
class ColumnProfile:
    source_name: str
    canonical_name: str
    inferred_type: str
    inferred_format: str | None
    inference: Inference
    review_required: bool
    review_reasons: tuple[str, ...]
    rows_profiled: int
    null_count: int
    null_percentage: float
    possible_null_tokens: tuple[str, ...]
    distinct_count: int
    cardinality_ratio: float
    leading_zeros_detected: bool
    possible_key_candidate: bool
    numeric_min: str | None
    numeric_max: str | None
    string_length_min: int | None
    string_length_max: int | None
    string_length_average: float | None
    observed_examples: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DatasetProfile:
    source_path: Path
    dataset_name: str
    delimiter: str
    sample_size_requested: int
    rows_scanned: int
    rows_profiled: int
    exact_duplicate_count: int
    columns: tuple[ColumnProfile, ...]
    source_type: str = "csv"
    nested_fields: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["source_path"] = str(self.source_path)
        return result


def canonical_name(value: str, *, fallback: str = "column") -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", value.strip()).strip("_").lower()
    normalized = normalized or fallback
    if normalized[0].isdigit():
        normalized = f"{fallback}_{normalized}"
    return normalized


def _unique_canonical_names(headers: Iterable[str]) -> list[str]:
    names: list[str] = []
    counts: dict[str, int] = {}
    for index, header in enumerate(headers, start=1):
        base = canonical_name(header, fallback=f"column_{index}")
        counts[base] = counts.get(base, 0) + 1
        names.append(base if counts[base] == 1 else f"{base}_{counts[base]}")
    return names


def _observed_null_tokens(values: list[str]) -> tuple[str, ...]:
    observed: list[str] = []
    seen: set[str] = set()
    for value in values:
        stripped = value.strip()
        if stripped.casefold() in NULL_TOKEN_NAMES and stripped not in seen:
            observed.append(stripped)
            seen.add(stripped)
    return tuple(observed)


def _is_null(value: str) -> bool:
    return value.strip().casefold() in NULL_TOKEN_NAMES


def _all_valid_dates(values: list[str], date_format: str) -> bool:
    try:
        for value in values:
            datetime.strptime(value, date_format).replace(tzinfo=UTC)
    except ValueError:
        return False
    return True


def _infer_slash_date(values: list[str]) -> tuple[str, str | None, Inference, bool]:
    parts = [SLASH_DATE_PATTERN.fullmatch(value) for value in values]
    if not all(parts):
        raise AssertionError("slash-date inference called with non-matching values")
    first = [int(match.group(1)) for match in parts if match]
    second = [int(match.group(2)) for match in parts if match]
    day_first_valid = (
        all(value <= 31 for value in first)
        and all(value <= 12 for value in second)
        and _all_valid_dates(values, "%d/%m/%Y")
    )
    month_first_valid = (
        all(value <= 12 for value in first)
        and all(value <= 31 for value in second)
        and _all_valid_dates(values, "%m/%d/%Y")
    )
    if day_first_valid and not month_first_valid:
        return "date", "%d/%m/%Y", Inference("high", "unambiguous_day_first_date"), False
    if month_first_valid and not day_first_valid:
        return "date", "%m/%d/%Y", Inference("high", "unambiguous_month_first_date"), False
    if day_first_valid and month_first_valid:
        return "date", None, Inference("medium", "ambiguous_date_format"), True
    return "string", None, Inference("low", "invalid_date_values"), True


def _infer_type(values: list[str]) -> tuple[str, str | None, Inference, bool]:
    if not values:
        return "string", None, Inference("low", "all_values_are_null_tokens"), True
    if any(LEADING_ZERO_PATTERN.fullmatch(value) for value in values):
        return "string", None, Inference("high", "leading_zeros"), False
    lowered = {value.casefold() for value in values}
    if lowered and lowered <= TRUE_FALSE_VALUES:
        return "boolean", None, Inference("high", "boolean_literals"), False
    if all(ISO_DATE_PATTERN.fullmatch(value) for value in values) and _all_valid_dates(
        values, "%Y-%m-%d"
    ):
        return "date", "%Y-%m-%d", Inference("high", "iso_date_format"), False
    if all(YEAR_FIRST_DATE_PATTERN.fullmatch(value) for value in values) and _all_valid_dates(
        values, "%Y/%m/%d"
    ):
        return "date", "%Y/%m/%d", Inference("high", "unambiguous_year_first_date"), False
    if all(SLASH_DATE_PATTERN.fullmatch(value) for value in values):
        return _infer_slash_date(values)
    if all(ISO_TIMESTAMP_PATTERN.fullmatch(value) for value in values):
        date_format = (
            "%Y-%m-%dT%H:%M:%S" if all("T" in value for value in values) else "%Y-%m-%d %H:%M:%S"
        )
        if _all_valid_dates(values, date_format):
            return "timestamp", date_format, Inference("high", "iso_timestamp_format"), False
    if all(INTEGER_PATTERN.fullmatch(value) for value in values):
        return "integer", None, Inference("high", "integer_pattern"), False
    if all(
        INTEGER_PATTERN.fullmatch(value) or DECIMAL_PATTERN.fullmatch(value) for value in values
    ):
        return "decimal", None, Inference("high", "decimal_pattern"), False
    numeric_like = sum(
        bool(INTEGER_PATTERN.fullmatch(value) or DECIMAL_PATTERN.fullmatch(value))
        for value in values
    )
    if numeric_like:
        return "string", None, Inference("medium", "mixed_numeric_and_text_values"), True
    return "string", None, Inference("high", "text_values"), False


def _numeric_bounds(values: list[str], inferred_type: str) -> tuple[str | None, str | None]:
    if inferred_type not in {"integer", "decimal"} or not values:
        return None, None
    try:
        numbers = [Decimal(value) for value in values]
    except InvalidOperation:
        return None, None
    return format(min(numbers), "f"), format(max(numbers), "f")


def _bounded_observation(value: str) -> str:
    if len(value) <= OBSERVED_EXAMPLE_MAX_LENGTH:
        return value
    return f"{value[: OBSERVED_EXAMPLE_MAX_LENGTH - 1]}…"


def _nested_observation(value: str) -> str:
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return "nested value"
    if isinstance(parsed, dict):
        count = len(parsed)
        return f"object ({count} field{'s' if count != 1 else ''})"
    if isinstance(parsed, list):
        count = len(parsed)
        noun = "item" if count == 1 else "items"
        element_type = (
            "object" if parsed and all(isinstance(item, dict) for item in parsed) else "value"
        )
        return f"array ({count} {noun}; {element_type} elements)"
    return "nested value"


def _profile_column(
    source_name: str,
    canonical: str,
    values: list[str],
    rows_profiled: int,
    *,
    nested: bool = False,
) -> ColumnProfile:
    possible_null_tokens = _observed_null_tokens(values)
    non_null_values = [value.strip() for value in values if not _is_null(value)]
    null_count = rows_profiled - len(non_null_values)
    inferred_type, inferred_format, inference, inference_review = _infer_type(non_null_values)
    if nested:
        inferred_type = "string"
        inferred_format = None
        inference = Inference("low", "nested_structure_requires_explicit_normalization")
        inference_review = True
    distinct_count = len(set(non_null_values))
    possible_key = bool(rows_profiled) and null_count == 0 and distinct_count == rows_profiled
    review_reasons: list[str] = []
    if inference_review:
        review_reasons.append(inference.reason)
    if any(token for token in possible_null_tokens):
        review_reasons.append("non_empty_null_token")
    numeric_min, numeric_max = _numeric_bounds(non_null_values, inferred_type)
    lengths = [len(value) for value in non_null_values] if inferred_type == "string" else []
    return ColumnProfile(
        source_name=source_name,
        canonical_name=canonical,
        inferred_type=inferred_type,
        inferred_format=inferred_format,
        inference=inference,
        review_required=bool(review_reasons),
        review_reasons=tuple(review_reasons),
        rows_profiled=rows_profiled,
        null_count=null_count,
        null_percentage=round((null_count / rows_profiled * 100) if rows_profiled else 0.0, 4),
        possible_null_tokens=possible_null_tokens,
        distinct_count=distinct_count,
        cardinality_ratio=round(
            (distinct_count / len(non_null_values)) if non_null_values else 0.0, 4
        ),
        leading_zeros_detected=any(
            LEADING_ZERO_PATTERN.fullmatch(value) for value in non_null_values
        ),
        possible_key_candidate=possible_key,
        numeric_min=numeric_min,
        numeric_max=numeric_max,
        string_length_min=min(lengths) if lengths else None,
        string_length_max=max(lengths) if lengths else None,
        string_length_average=round(mean(lengths), 4) if lengths else None,
        observed_examples=tuple(
            dict.fromkeys(
                _nested_observation(value) if nested else _bounded_observation(value)
                for value in non_null_values
            )
        )[:5],
    )


def _dataset_profile(
    path: Path,
    *,
    source_type: str,
    headers: list[str],
    sampled_rows: list[list[str]],
    rows_scanned: int,
    exact_duplicate_count: int,
    sample_size: int,
    delimiter: str = ",",
    nested_fields: set[str] | None = None,
) -> DatasetProfile:
    canonical_names = _unique_canonical_names(headers)
    rows_profiled = len(sampled_rows)
    nested_fields = nested_fields or set()
    columns = tuple(
        _profile_column(
            source_name,
            canonical,
            [row[index] for row in sampled_rows],
            rows_profiled,
            nested=source_name in nested_fields,
        )
        for index, (source_name, canonical) in enumerate(zip(headers, canonical_names, strict=True))
    )
    return DatasetProfile(
        source_path=path,
        dataset_name=canonical_name(path.stem, fallback="dataset"),
        delimiter=delimiter,
        sample_size_requested=sample_size,
        rows_scanned=rows_scanned,
        rows_profiled=rows_profiled,
        exact_duplicate_count=exact_duplicate_count,
        columns=columns,
        source_type=source_type,
        nested_fields=tuple(sorted(nested_fields)),
    )


def profile_csv(
    source_path: str | Path, *, sample_size: int = 10_000, delimiter: str = ","
) -> DatasetProfile:
    path = Path(source_path).resolve()
    if not path.is_file():
        raise ProfilingError(f"CSV source does not exist: {path}")
    if sample_size <= 0:
        raise ProfilingError("sample_size must be greater than zero")
    if len(delimiter) != 1:
        raise ProfilingError("delimiter must be exactly one character")

    sampled_rows: list[list[str]] = []
    seen_rows: set[tuple[str, ...]] = set()
    exact_duplicate_count = 0
    rows_scanned = 0
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.reader(source, delimiter=delimiter)
            headers = next(reader, None)
            if not headers:
                raise ProfilingError(f"CSV source has no header row: {path}")
            if len(set(headers)) != len(headers):
                raise ProfilingError("CSV source contains duplicate column names")
            for row_number, row in enumerate(reader, start=2):
                if len(row) != len(headers):
                    raise ProfilingError(
                        f"CSV row {row_number} has {len(row)} values; expected {len(headers)}"
                    )
                rows_scanned += 1
                row_key = tuple(row)
                if row_key in seen_rows:
                    exact_duplicate_count += 1
                else:
                    seen_rows.add(row_key)
                if len(sampled_rows) < sample_size:
                    sampled_rows.append(row)
    except (OSError, UnicodeError, csv.Error) as exc:
        raise ProfilingError(f"Could not profile CSV source {path}: {exc}") from exc

    return _dataset_profile(
        path,
        source_type="csv",
        headers=headers,
        sampled_rows=sampled_rows,
        rows_scanned=rows_scanned,
        exact_duplicate_count=exact_duplicate_count,
        sample_size=sample_size,
        delimiter=delimiter,
    )


def _json_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, dict | list):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value)


def profile_json(source_path: str | Path, *, sample_size: int = 10_000) -> DatasetProfile:
    path = Path(source_path).resolve()
    if not path.is_file():
        raise ProfilingError(f"JSON source does not exist: {path}")
    if sample_size <= 0:
        raise ProfilingError("sample_size must be greater than zero")
    try:
        text = path.read_text(encoding="utf-8-sig")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = [json.loads(line) for line in text.splitlines() if line.strip()]
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProfilingError("The JSON source could not be parsed.") from exc
    records = [payload] if isinstance(payload, dict) else payload
    if (
        not isinstance(records, list)
        or not records
        or not all(isinstance(record, dict) for record in records)
    ):
        raise ProfilingError("JSON root must be an object or a non-empty array of objects")
    headers = list(dict.fromkeys(key for record in records for key in record))
    if not headers:
        raise ProfilingError("JSON source does not contain fields")
    nested_fields = {
        key for record in records for key, value in record.items() if isinstance(value, dict | list)
    }
    rows = [[_json_value(record.get(header)) for header in headers] for record in records]
    keys = [tuple(row) for row in rows]
    return _dataset_profile(
        path,
        source_type="json",
        headers=headers,
        sampled_rows=rows[:sample_size],
        rows_scanned=len(rows),
        exact_duplicate_count=len(keys) - len(set(keys)),
        sample_size=sample_size,
        nested_fields=nested_fields,
    )


def profile_parquet(source_path: str | Path, *, sample_size: int = 10_000) -> DatasetProfile:
    path = Path(source_path).resolve()
    if not path.is_file():
        raise ProfilingError(f"Parquet source does not exist: {path}")
    if sample_size <= 0:
        raise ProfilingError("sample_size must be greater than zero")
    try:
        with duckdb.connect(":memory:") as connection:
            relation = connection.execute(
                "SELECT * FROM read_parquet(?) LIMIT ?", [str(path), sample_size]
            )
            headers = [item[0] for item in relation.description]
            values = relation.fetchall()
            rows_scanned = connection.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(path)]
            ).fetchone()[0]
            distinct_rows = connection.execute(
                "SELECT count(*) FROM (SELECT DISTINCT * FROM read_parquet(?))", [str(path)]
            ).fetchone()[0]
    except (duckdb.Error, OSError) as exc:
        raise ProfilingError("The Parquet source could not be profiled.") from exc
    rows = [[_json_value(value) for value in row] for row in values]
    return _dataset_profile(
        path,
        source_type="parquet",
        headers=headers,
        sampled_rows=rows,
        rows_scanned=int(rows_scanned),
        exact_duplicate_count=int(rows_scanned - distinct_rows),
        sample_size=sample_size,
    )


def profile_file(
    source_path: str | Path, source_type: str, *, sample_size: int = 10_000
) -> DatasetProfile:
    profilers = {
        "csv": profile_csv,
        "json": profile_json,
        "parquet": profile_parquet,
    }
    try:
        profiler = profilers[source_type]
    except KeyError as exc:
        raise ProfilingError(f"Unsupported profiling source type: {source_type}") from exc
    return profiler(source_path, sample_size=sample_size)
