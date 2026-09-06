from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from metadata_etl.errors import ConfigError

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SUPPORTED_TYPES = {"string", "integer", "decimal", "date", "timestamp", "boolean"}
SUPPORTED_TRANSFORMS = {"filter", "derive"}
FORBIDDEN_SQL = re.compile(
    r"(;|--|/\*|\*/|\b(attach|copy|create|delete|drop|insert|install|load|pragma|update)\b)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ColumnConfig:
    name: str
    source: str
    datatype: str
    nullable: bool
    date_format: str | None = None


@dataclass(frozen=True)
class TransformConfig:
    id: str
    type: str
    values: dict[str, Any]


@dataclass(frozen=True)
class ETLConfig:
    path: Path
    raw: dict[str, Any]
    config_hash: str
    schema_version: str
    dataset: str
    source_path: Path
    delimiter: str
    raw_root: Path
    trim_strings: bool
    null_tokens: tuple[str, ...]
    columns: tuple[ColumnConfig, ...]
    transformations: tuple[TransformConfig, ...]
    connection_env: str
    destination_schema: str
    staging_table: str
    target_table: str
    load_strategy: str

    @property
    def dsn(self) -> str:
        value = os.getenv(self.connection_env)
        if not value:
            raise ConfigError(
                f"Environment variable {self.connection_env!r} is required for the PostgreSQL load"
            )
        return value


def _require_mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{path} must be a YAML mapping")
    return value


def _require_string(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{path} must be a non-empty string")
    return value.strip()


def _identifier(value: Any, path: str) -> str:
    name = _require_string(value, path)
    if not IDENTIFIER.fullmatch(name):
        raise ConfigError(f"{path} must be a safe SQL identifier; got {name!r}")
    return name


def _validate_reviews(node: Any, path: str = "config") -> None:
    if isinstance(node, dict):
        review = node.get("review")
        if review is not None:
            review = _require_mapping(review, f"{path}.review")
            if review.get("required") is True:
                if review.get("approved") is not True:
                    raise ConfigError(f"{path}.review requires explicit approval")
                _require_string(review.get("approved_by"), f"{path}.review.approved_by")
        for key, value in node.items():
            if key != "review":
                _validate_reviews(value, f"{path}.{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            _validate_reviews(value, f"{path}[{index}]")


def _validate_expression(value: Any, path: str) -> str:
    expression = _require_string(value, path)
    if FORBIDDEN_SQL.search(expression):
        raise ConfigError(f"{path} contains a forbidden SQL token")
    return expression


def load_config(config_path: str | Path, *, require_source: bool = True) -> ETLConfig:
    path = Path(config_path).resolve()
    if not path.is_file():
        raise ConfigError(f"Config file does not exist: {path}")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {path}: {exc}") from exc

    root = _require_mapping(raw, "config")
    schema_version = _require_string(root.get("config_schema_version"), "config_schema_version")
    if schema_version != "1.0":
        raise ConfigError(f"Unsupported config_schema_version: {schema_version!r}")

    if "review" not in root:
        raise ConfigError("A top-level review section with explicit approval is required")
    _validate_reviews(root)

    dataset_section = _require_mapping(root.get("dataset"), "dataset")
    dataset = _identifier(dataset_section.get("name"), "dataset.name")

    source = _require_mapping(root.get("source"), "source")
    source_type = _require_string(source.get("type"), "source.type").lower()
    if source_type != "csv":
        raise ConfigError("Milestone 1 supports source.type: csv only")
    source_value = _require_string(source.get("path"), "source.path")
    source_path = Path(source_value)
    if not source_path.is_absolute():
        source_path = (Path.cwd() / source_path).resolve()
    if require_source and not source_path.is_file():
        raise ConfigError(f"CSV source does not exist: {source_path}")

    options = _require_mapping(source.get("options", {}), "source.options")
    delimiter = options.get("delimiter", ",")
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ConfigError("source.options.delimiter must be exactly one character")

    runtime = _require_mapping(root.get("runtime", {}), "runtime")
    raw_root_value = runtime.get("raw_root", "data/raw")
    raw_root = Path(_require_string(raw_root_value, "runtime.raw_root"))
    if not raw_root.is_absolute():
        raw_root = (Path.cwd() / raw_root).resolve()

    normalization = _require_mapping(root.get("normalization", {}), "normalization")
    trim_strings = normalization.get("trim_strings", False)
    if not isinstance(trim_strings, bool):
        raise ConfigError("normalization.trim_strings must be true or false")
    null_tokens_value = normalization.get("null_tokens", [])
    if not isinstance(null_tokens_value, list) or not all(
        isinstance(token, str) for token in null_tokens_value
    ):
        raise ConfigError("normalization.null_tokens must be a list of strings")

    column_section = _require_mapping(root.get("columns"), "columns")
    if not column_section:
        raise ConfigError("columns must contain at least one column")
    columns: list[ColumnConfig] = []
    for canonical_name, column_value in column_section.items():
        canonical_name = _identifier(canonical_name, f"columns.{canonical_name}")
        column = _require_mapping(column_value, f"columns.{canonical_name}")
        source_name = _require_string(
            column.get("source", canonical_name), f"columns.{canonical_name}.source"
        )
        datatype = _require_string(column.get("type"), f"columns.{canonical_name}.type").lower()
        if datatype not in SUPPORTED_TYPES:
            raise ConfigError(
                f"columns.{canonical_name}.type must be one of {sorted(SUPPORTED_TYPES)}"
            )
        nullable = column.get("nullable", True)
        if not isinstance(nullable, bool):
            raise ConfigError(f"columns.{canonical_name}.nullable must be true or false")
        date_format = column.get("format")
        if date_format is not None:
            date_format = _require_string(date_format, f"columns.{canonical_name}.format")
        if datatype == "date" and not date_format:
            raise ConfigError(
                f"columns.{canonical_name}.format is required; dates are never interpreted ambiguously"
            )
        columns.append(ColumnConfig(canonical_name, source_name, datatype, nullable, date_format))

    transform_values = root.get("transformations", [])
    if not isinstance(transform_values, list):
        raise ConfigError("transformations must be a list")
    transformations: list[TransformConfig] = []
    seen_transform_ids: set[str] = set()
    known_columns = {column.name for column in columns}
    for index, value in enumerate(transform_values):
        item_path = f"transformations[{index}]"
        transform = _require_mapping(value, item_path)
        transform_id = _identifier(transform.get("id"), f"{item_path}.id")
        if transform_id in seen_transform_ids:
            raise ConfigError(f"Duplicate transformation id: {transform_id}")
        seen_transform_ids.add(transform_id)
        transform_type = _require_string(transform.get("type"), f"{item_path}.type").lower()
        if transform_type not in SUPPORTED_TRANSFORMS:
            raise ConfigError(
                f"{item_path}.type must be one of {sorted(SUPPORTED_TRANSFORMS)} in milestone 1"
            )
        values = dict(transform)
        if transform_type == "filter":
            values["condition"] = _validate_expression(
                transform.get("condition"), f"{item_path}.condition"
            )
        if transform_type == "derive":
            target = _identifier(transform.get("target_column"), f"{item_path}.target_column")
            if target in known_columns:
                raise ConfigError(f"{item_path}.target_column already exists: {target}")
            datatype = _require_string(transform.get("datatype"), f"{item_path}.datatype").lower()
            if datatype not in SUPPORTED_TYPES:
                raise ConfigError(f"{item_path}.datatype must be one of {sorted(SUPPORTED_TYPES)}")
            values["target_column"] = target
            values["datatype"] = datatype
            values["expression"] = _validate_expression(
                transform.get("expression"), f"{item_path}.expression"
            )
            known_columns.add(target)
        transformations.append(TransformConfig(transform_id, transform_type, values))

    load = _require_mapping(root.get("load"), "load")
    strategy = _require_string(load.get("strategy"), "load.strategy").lower()
    if strategy != "full":
        raise ConfigError("Milestone 1 supports load.strategy: full only")
    connection_env = _identifier(
        load.get("connection_env", "ETL_POSTGRES_DSN"), "load.connection_env"
    )
    destination_schema = _identifier(load.get("schema", "public"), "load.schema")
    staging_table = _identifier(load.get("staging_table"), "load.staging_table")
    target_table = _identifier(load.get("target_table"), "load.target_table")

    config_hash = hashlib.sha256(path.read_bytes()).hexdigest()

    return ETLConfig(
        path=path,
        raw=root,
        config_hash=config_hash,
        schema_version=schema_version,
        dataset=dataset,
        source_path=source_path,
        delimiter=delimiter,
        raw_root=raw_root,
        trim_strings=trim_strings,
        null_tokens=tuple(null_tokens_value),
        columns=tuple(columns),
        transformations=tuple(transformations),
        connection_env=connection_env,
        destination_schema=destination_schema,
        staging_table=staging_table,
        target_table=target_table,
        load_strategy=strategy,
    )
