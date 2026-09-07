from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

from metadata_etl.errors import ConfigError
from metadata_etl.quality.contracts import default_contract_registry
from metadata_etl.transformations.operators.base import IDENTIFIER, SUPPORTED_TYPES
from metadata_etl.transformations.registry import default_registry


@dataclass(frozen=True)
class ColumnConfig:
    name: str
    source: str
    datatype: str
    nullable: bool
    date_format: str | None = None
    classification: str | None = None
    quarantine_value: str = "none"


@dataclass(frozen=True)
class TransformConfig:
    id: str
    type: str
    values: dict[str, Any]


@dataclass(frozen=True)
class ContractConfig:
    id: str
    type: str
    values: dict[str, Any]


@dataclass(frozen=True)
class WatermarkConfig:
    column: str
    datatype: str
    initial_value: Any | None = None


@dataclass(frozen=True)
class JSONExplodeConfig:
    path: str
    alias: str
    fields: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class JSONNormalizationConfig:
    root_path: str | None
    fields: tuple[tuple[str, str], ...]
    explode: JSONExplodeConfig | None


@dataclass(frozen=True)
class SCD2Config:
    keys: tuple[str, ...]
    tracked_columns: tuple[str, ...]
    effective_timestamp: str
    valid_from: str
    valid_to: str
    is_current: str


@dataclass(frozen=True)
class ETLConfig:
    path: Path
    raw: dict[str, Any]
    config_hash: str
    schema_version: str
    dataset: str
    source_type: str
    source_path: Path | None
    source_connection_env: str | None
    source_table: tuple[str, str] | None
    delimiter: str
    raw_root: Path
    trim_strings: bool
    null_tokens: tuple[str, ...]
    columns: tuple[ColumnConfig, ...]
    transformations: tuple[TransformConfig, ...]
    contracts: tuple[ContractConfig, ...]
    connection_env: str
    destination_schema: str
    staging_table: str
    target_table: str
    load_strategy: str
    load_keys: tuple[str, ...]
    watermark: WatermarkConfig | None
    schema_drift: dict[str, str]
    json_normalization: JSONNormalizationConfig | None
    scd2: SCD2Config | None

    @property
    def dsn(self) -> str:
        value = os.getenv(self.connection_env)
        if not value:
            raise ConfigError(
                f"Environment variable {self.connection_env!r} is required for the PostgreSQL load"
            )
        return value

    @property
    def source_dsn(self) -> str:
        if not self.source_connection_env:
            raise ConfigError("This source does not define a PostgreSQL connection")
        value = os.getenv(self.source_connection_env)
        if not value:
            raise ConfigError(
                f"Environment variable {self.source_connection_env!r} is required for the "
                "PostgreSQL source"
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


def _validate_watermark_initial(value: Any, datatype: str, path: str) -> Any:
    if value is None:
        return None
    try:
        if datatype == "string":
            if not isinstance(value, str):
                raise ValueError
        elif datatype == "integer":
            if isinstance(value, bool) or int(value) != float(value):
                raise ValueError
        elif datatype == "decimal":
            if isinstance(value, bool) or not Decimal(str(value)).is_finite():
                raise ValueError
        elif datatype == "date":
            if isinstance(value, datetime):
                raise ValueError
            if not isinstance(value, date):
                date.fromisoformat(str(value))
        elif datatype == "timestamp" and not isinstance(value, datetime):
            datetime.fromisoformat(str(value))
    except (InvalidOperation, OverflowError, TypeError, ValueError) as exc:
        raise ConfigError(f"{path} is not a valid {datatype} value") from exc
    return value


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
    if source_type not in {"csv", "json", "parquet", "postgres"}:
        raise ConfigError("source.type must be csv, json, parquet, or postgres")
    source_path: Path | None = None
    source_connection_env: str | None = None
    source_table: tuple[str, str] | None = None
    if source_type in {"csv", "json", "parquet"}:
        source_value = _require_string(source.get("path"), "source.path")
        source_path = Path(source_value)
        if not source_path.is_absolute():
            source_path = (Path.cwd() / source_path).resolve()
        if require_source and not source_path.is_file():
            raise ConfigError(f"{source_type.upper()} source does not exist: {source_path}")
    else:
        source_connection_env = _identifier(source.get("connection_env"), "source.connection_env")
        table_value = _require_string(source.get("table"), "source.table")
        table_parts = table_value.split(".")
        if len(table_parts) != 2:
            raise ConfigError("source.table must use schema.table notation")
        source_table = (
            _identifier(table_parts[0], "source.table schema"),
            _identifier(table_parts[1], "source.table name"),
        )
        if "query" in source:
            raise ConfigError("Custom PostgreSQL source queries are not supported yet")

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

    json_normalization: JSONNormalizationConfig | None = None
    json_value = normalization.get("json")
    if json_value is not None:
        if source_type != "json":
            raise ConfigError("normalization.json is supported only for source.type: json")
        json_mapping = _require_mapping(json_value, "normalization.json")
        root_path = json_mapping.get("root_path")
        if root_path is not None:
            root_path = _require_string(root_path, "normalization.json.root_path")
        field_mapping = _require_mapping(
            json_mapping.get("fields", {}), "normalization.json.fields"
        )
        fields = tuple(
            (
                _identifier(name, f"normalization.json.fields.{name}"),
                _require_string(value, f"normalization.json.fields.{name}"),
            )
            for name, value in field_mapping.items()
        )
        explode_config: JSONExplodeConfig | None = None
        if "explode" in json_mapping:
            explode = _require_mapping(json_mapping["explode"], "normalization.json.explode")
            explode_fields_mapping = _require_mapping(
                explode.get("fields"), "normalization.json.explode.fields"
            )
            if not explode_fields_mapping:
                raise ConfigError("normalization.json.explode.fields cannot be empty")
            explode_fields = tuple(
                (
                    _identifier(name, f"normalization.json.explode.fields.{name}"),
                    _require_string(value, f"normalization.json.explode.fields.{name}"),
                )
                for name, value in explode_fields_mapping.items()
            )
            explode_config = JSONExplodeConfig(
                _require_string(explode.get("path"), "normalization.json.explode.path"),
                _identifier(explode.get("as"), "normalization.json.explode.as"),
                explode_fields,
            )
        output_names = [name for name, _ in fields]
        if explode_config:
            output_names.extend(name for name, _ in explode_config.fields)
        if len(set(output_names)) != len(output_names):
            raise ConfigError("normalization.json output field names must be unique")
        if not output_names:
            raise ConfigError("normalization.json must configure fields or an explode")
        json_normalization = JSONNormalizationConfig(root_path, fields, explode_config)

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
        classification = column.get("classification")
        if classification is not None:
            classification = _require_string(
                classification, f"columns.{canonical_name}.classification"
            )
        quarantine = _require_mapping(
            column.get("quarantine", {}), f"columns.{canonical_name}.quarantine"
        )
        quarantine_value = _require_string(
            quarantine.get("value", "none"), f"columns.{canonical_name}.quarantine.value"
        ).lower()
        if quarantine_value not in {"full", "masked", "hashed", "none"}:
            raise ConfigError(
                f"columns.{canonical_name}.quarantine.value must be full, masked, hashed, or none"
            )
        columns.append(
            ColumnConfig(
                canonical_name,
                source_name,
                datatype,
                nullable,
                date_format,
                classification,
                quarantine_value,
            )
        )

    if json_normalization is not None:
        normalized_names = {name for name, _ in json_normalization.fields}
        if json_normalization.explode:
            normalized_names.update(name for name, _ in json_normalization.explode.fields)
        missing_json_fields = {column.source for column in columns} - normalized_names
        if missing_json_fields:
            raise ConfigError(
                "columns reference JSON fields not produced by normalization.json: "
                f"{sorted(missing_json_fields)}"
            )

    transform_values = root.get("transformations", [])
    if not isinstance(transform_values, list):
        raise ConfigError("transformations must be a list")
    transformations: list[TransformConfig] = []
    seen_transform_ids: set[str] = set()
    known_schema = {column.name: column.datatype for column in columns}
    registry = default_registry()
    for index, value in enumerate(transform_values):
        item_path = f"transformations[{index}]"
        transform = _require_mapping(value, item_path)
        transform_id = _identifier(transform.get("id"), f"{item_path}.id")
        if transform_id in seen_transform_ids:
            raise ConfigError(f"Duplicate transformation id: {transform_id}")
        seen_transform_ids.add(transform_id)
        transform_type = _require_string(transform.get("type"), f"{item_path}.type").lower()
        values = registry.validate(transform_type, transform, item_path, known_schema)
        transformations.append(TransformConfig(transform_id, transform_type, values))

    contract_values = root.get("contracts", [])
    if not isinstance(contract_values, list):
        raise ConfigError("contracts must be a list")
    contracts: list[ContractConfig] = []
    seen_contract_ids: set[str] = set()
    contract_registry = default_contract_registry()
    for index, value in enumerate(contract_values):
        item_path = f"contracts[{index}]"
        contract = _require_mapping(value, item_path)
        contract_id = _identifier(contract.get("id"), f"{item_path}.id")
        if contract_id in seen_contract_ids:
            raise ConfigError(f"Duplicate contract id: {contract_id}")
        seen_contract_ids.add(contract_id)
        contract_type = _require_string(contract.get("type"), f"{item_path}.type").lower()
        values = contract_registry.validate(contract_type, contract, item_path, known_schema)
        contracts.append(ContractConfig(contract_id, contract_type, values))

    drift_value = _require_mapping(root.get("schema_drift", {}), "schema_drift")
    drift_defaults = {
        "added_columns": "warn",
        "removed_columns": "fail",
        "datatype_change": "fail",
        "canonical_change": "fail",
        "raw_structure_change": "warn",
    }
    unknown_drift_settings = set(drift_value) - set(drift_defaults)
    if unknown_drift_settings:
        raise ConfigError(
            f"schema_drift contains unsupported settings: {sorted(unknown_drift_settings)}"
        )
    schema_drift: dict[str, str] = {}
    for setting, default in drift_defaults.items():
        action = _require_string(
            drift_value.get(setting, default), f"schema_drift.{setting}"
        ).lower()
        if action not in {"allow", "warn", "fail"}:
            raise ConfigError(f"schema_drift.{setting} must be allow, warn, or fail")
        schema_drift[setting] = action

    load = _require_mapping(root.get("load"), "load")
    strategy = _require_string(load.get("strategy"), "load.strategy").lower()
    if strategy not in {"full", "incremental", "upsert", "scd2"}:
        raise ConfigError("load.strategy must be full, incremental, upsert, or scd2")
    connection_env = _identifier(
        load.get("connection_env", "ETL_POSTGRES_DSN"), "load.connection_env"
    )
    destination_schema = _identifier(load.get("schema", "public"), "load.schema")
    staging_table = _identifier(load.get("staging_table"), "load.staging_table")
    target_table = _identifier(load.get("target_table"), "load.target_table")

    load_keys: tuple[str, ...] = ()
    watermark: WatermarkConfig | None = None
    scd2: SCD2Config | None = None
    if strategy == "upsert":
        keys_value = load.get("keys")
        if not isinstance(keys_value, list) or not keys_value:
            raise ConfigError("load.keys must be a non-empty list for upsert")
        keys = tuple(
            _identifier(value, f"load.keys[{index}]") for index, value in enumerate(keys_value)
        )
        if len(set(keys)) != len(keys):
            raise ConfigError("load.keys cannot contain duplicate columns")
        for key in keys:
            if key not in known_schema:
                raise ConfigError(
                    f"load.keys references unknown post-transformation column {key!r}"
                )
        load_keys = keys
    elif strategy != "scd2" and "keys" in load:
        raise ConfigError("load.keys is supported only for load.strategy: upsert")

    if strategy == "incremental":
        watermark_value = _require_mapping(load.get("watermark"), "load.watermark")
        watermark_column = _identifier(watermark_value.get("column"), "load.watermark.column")
        source_columns = {column.name: column for column in columns}
        if watermark_column not in source_columns:
            raise ConfigError(
                f"load.watermark.column references unknown canonical source column "
                f"{watermark_column!r}"
            )
        if source_columns[watermark_column].nullable:
            raise ConfigError("load.watermark.column must be configured nullable: false")
        watermark_type = _require_string(watermark_value.get("type"), "load.watermark.type").lower()
        if watermark_type not in {"string", "integer", "decimal", "date", "timestamp"}:
            raise ConfigError(
                "load.watermark.type must be string, integer, decimal, date, or timestamp"
            )
        if watermark_type != source_columns[watermark_column].datatype:
            raise ConfigError(
                f"load.watermark.type {watermark_type!r} must match canonical column "
                f"type {source_columns[watermark_column].datatype!r}"
            )
        initial_value = watermark_value.get("initial_value")
        if isinstance(initial_value, dict | list | tuple | set):
            raise ConfigError("load.watermark.initial_value must be a scalar value or null")
        initial_value = _validate_watermark_initial(
            initial_value, watermark_type, "load.watermark.initial_value"
        )
        watermark = WatermarkConfig(watermark_column, watermark_type, initial_value)
    elif "watermark" in load:
        raise ConfigError("load.watermark is supported only for load.strategy: incremental")

    if strategy == "scd2":
        keys_value = load.get("keys")
        if not isinstance(keys_value, list) or not keys_value:
            raise ConfigError("load.keys must be a non-empty list for scd2")
        keys = tuple(
            _identifier(value, f"load.keys[{index}]") for index, value in enumerate(keys_value)
        )
        if len(set(keys)) != len(keys):
            raise ConfigError("load.keys cannot contain duplicate columns")
        tracked_value = load.get("tracked_columns")
        if not isinstance(tracked_value, list) or not tracked_value:
            raise ConfigError("load.tracked_columns must be a non-empty list for scd2")
        tracked = tuple(
            _identifier(value, f"load.tracked_columns[{index}]")
            for index, value in enumerate(tracked_value)
        )
        if len(set(tracked)) != len(tracked):
            raise ConfigError("load.tracked_columns cannot contain duplicate columns")
        for name in keys + tracked:
            if name not in known_schema:
                raise ConfigError(f"SCD2 configuration references unknown column {name!r}")
        if set(keys) & set(tracked):
            raise ConfigError("load.tracked_columns cannot overlap load.keys")
        effective = _require_mapping(load.get("effective_timestamp"), "load.effective_timestamp")
        effective_column = _identifier(effective.get("column"), "load.effective_timestamp.column")
        if effective_column not in known_schema or known_schema[effective_column] not in {
            "date",
            "timestamp",
        }:
            raise ConfigError(
                "load.effective_timestamp.column must reference a date or timestamp column"
            )
        history = _require_mapping(load.get("history_columns"), "load.history_columns")
        valid_from = _identifier(history.get("valid_from"), "load.history_columns.valid_from")
        valid_to = _identifier(history.get("valid_to"), "load.history_columns.valid_to")
        is_current = _identifier(history.get("is_current"), "load.history_columns.is_current")
        history_names = {valid_from, valid_to, is_current}
        if len(history_names) != 3:
            raise ConfigError("SCD2 history column names must be distinct")
        if history_names & set(known_schema):
            raise ConfigError("SCD2 history columns cannot overlap data columns")
        if history_names & (set(keys) | set(tracked) | {effective_column}):
            raise ConfigError("SCD2 tracked/key/effective columns cannot use history columns")
        load_keys = keys
        scd2 = SCD2Config(keys, tracked, effective_column, valid_from, valid_to, is_current)

    config_hash = hashlib.sha256(path.read_bytes()).hexdigest()

    return ETLConfig(
        path=path,
        raw=root,
        config_hash=config_hash,
        schema_version=schema_version,
        dataset=dataset,
        source_type=source_type,
        source_path=source_path,
        source_connection_env=source_connection_env,
        source_table=source_table,
        delimiter=delimiter,
        raw_root=raw_root,
        trim_strings=trim_strings,
        null_tokens=tuple(null_tokens_value),
        columns=tuple(columns),
        transformations=tuple(transformations),
        contracts=tuple(contracts),
        connection_env=connection_env,
        destination_schema=destination_schema,
        staging_table=staging_table,
        target_table=target_table,
        load_strategy=strategy,
        load_keys=load_keys,
        watermark=watermark,
        schema_drift=schema_drift,
        json_normalization=json_normalization,
        scd2=scd2,
    )
