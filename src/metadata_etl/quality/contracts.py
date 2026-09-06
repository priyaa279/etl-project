from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from metadata_etl.errors import ConfigError, ETLError
from metadata_etl.transformations.operators.base import (
    Schema,
    require_column,
)


@dataclass(frozen=True)
class ContractFailure:
    row_index: int
    columns: tuple[str, ...]
    reason: str


class ContractOperator(ABC):
    name: str

    @abstractmethod
    def validate(self, spec: dict[str, Any], path: str, schema: Schema) -> dict[str, Any]:
        """Validate and normalize a contract specification."""

    @abstractmethod
    def evaluate(
        self,
        spec: dict[str, Any],
        column_positions: dict[str, int],
        rows: Sequence[tuple[Any, ...]],
    ) -> list[ContractFailure]:
        """Return one failure for each row that violates this rule."""


class NotNullContract(ContractOperator):
    name = "not_null"

    def validate(self, spec: dict[str, Any], path: str, schema: Schema) -> dict[str, Any]:
        values = dict(spec)
        values["column"] = require_column(spec.get("column"), f"{path}.column", schema)
        return values

    def evaluate(
        self,
        spec: dict[str, Any],
        column_positions: dict[str, int],
        rows: Sequence[tuple[Any, ...]],
    ) -> list[ContractFailure]:
        column = spec["column"]
        position = column_positions[column]
        return [
            ContractFailure(index, (column,), f"{column} must not be null")
            for index, row in enumerate(rows)
            if row[position] is None
        ]


class UniqueContract(ContractOperator):
    name = "unique"

    def validate(self, spec: dict[str, Any], path: str, schema: Schema) -> dict[str, Any]:
        values = dict(spec)
        columns_value = spec.get("columns")
        if not isinstance(columns_value, list) or not columns_value:
            raise ConfigError(f"{path}.columns must be a non-empty list")
        columns = tuple(
            require_column(value, f"{path}.columns[{index}]", schema)
            for index, value in enumerate(columns_value)
        )
        if len(set(columns)) != len(columns):
            raise ConfigError(f"{path}.columns cannot contain duplicates")
        values["columns"] = columns
        return values

    def evaluate(
        self,
        spec: dict[str, Any],
        column_positions: dict[str, int],
        rows: Sequence[tuple[Any, ...]],
    ) -> list[ContractFailure]:
        columns = spec["columns"]
        positions = tuple(column_positions[column] for column in columns)
        keys = [tuple(row[position] for position in positions) for row in rows]
        counts = Counter(key for key in keys if all(value is not None for value in key))
        return [
            ContractFailure(index, columns, f"duplicate value for unique key {list(columns)}")
            for index, key in enumerate(keys)
            if all(value is not None for value in key) and counts[key] > 1
        ]


def _decimal_bound(value: Any, path: str) -> Decimal:
    if isinstance(value, bool | dict | list | tuple | set) or value is None:
        raise ConfigError(f"{path} must be a finite numeric value")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ConfigError(f"{path} must be a finite numeric value") from exc
    if not result.is_finite():
        raise ConfigError(f"{path} must be a finite numeric value")
    return result


class RangeContract(ContractOperator):
    name = "range"

    def validate(self, spec: dict[str, Any], path: str, schema: Schema) -> dict[str, Any]:
        values = dict(spec)
        column = require_column(spec.get("column"), f"{path}.column", schema)
        if schema[column] not in {"integer", "decimal"}:
            raise ConfigError(f"{path}.column must reference an integer or decimal column")
        if "min" not in spec and "max" not in spec:
            raise ConfigError(f"{path} must define min, max, or both")
        minimum = _decimal_bound(spec["min"], f"{path}.min") if "min" in spec else None
        maximum = _decimal_bound(spec["max"], f"{path}.max") if "max" in spec else None
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ConfigError(f"{path}.min cannot be greater than {path}.max")
        values.update(column=column, min=minimum, max=maximum)
        return values

    def evaluate(
        self,
        spec: dict[str, Any],
        column_positions: dict[str, int],
        rows: Sequence[tuple[Any, ...]],
    ) -> list[ContractFailure]:
        column = spec["column"]
        position = column_positions[column]
        failures: list[ContractFailure] = []
        for index, row in enumerate(rows):
            value = row[position]
            if value is None:
                continue
            try:
                numeric = Decimal(str(value))
            except InvalidOperation as exc:
                raise ETLError(f"Contract {spec['id']} received a non-numeric value") from exc
            below = spec["min"] is not None and numeric < spec["min"]
            above = spec["max"] is not None and numeric > spec["max"]
            if below or above:
                failures.append(
                    ContractFailure(
                        index,
                        (column,),
                        f"{column} is outside configured range [{spec['min']}, {spec['max']}]",
                    )
                )
        return failures


class RegexContract(ContractOperator):
    name = "regex"

    def validate(self, spec: dict[str, Any], path: str, schema: Schema) -> dict[str, Any]:
        values = dict(spec)
        column = require_column(spec.get("column"), f"{path}.column", schema)
        if schema[column] != "string":
            raise ConfigError(f"{path}.column must reference a string column")
        pattern = spec.get("pattern")
        if not isinstance(pattern, str) or not pattern:
            raise ConfigError(f"{path}.pattern must be a non-empty string")
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ConfigError(f"{path}.pattern is not a valid regular expression: {exc}") from exc
        values.update(column=column, pattern=pattern)
        return values

    def evaluate(
        self,
        spec: dict[str, Any],
        column_positions: dict[str, int],
        rows: Sequence[tuple[Any, ...]],
    ) -> list[ContractFailure]:
        column = spec["column"]
        position = column_positions[column]
        pattern = re.compile(spec["pattern"])
        return [
            ContractFailure(index, (column,), f"{column} does not match configured regex")
            for index, row in enumerate(rows)
            if row[position] is not None and pattern.search(str(row[position])) is None
        ]


class ContractRegistry:
    def __init__(self, operators: Iterable[ContractOperator] = ()) -> None:
        self._operators: dict[str, ContractOperator] = {}
        for operator in operators:
            self.register(operator)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._operators))

    def register(self, operator: ContractOperator) -> None:
        if operator.name in self._operators:
            raise ValueError(f"Contract operator already registered: {operator.name}")
        self._operators[operator.name] = operator

    def get(self, name: str) -> ContractOperator:
        try:
            return self._operators[name]
        except KeyError as exc:
            raise ConfigError(
                f"Unsupported contract type {name!r}; supported types: {list(self.names)}"
            ) from exc

    def validate(
        self, name: str, spec: dict[str, Any], path: str, schema: Schema
    ) -> dict[str, Any]:
        return self.get(name).validate(spec, path, schema)


_DEFAULT_REGISTRY = ContractRegistry(
    [NotNullContract(), UniqueContract(), RangeContract(), RegexContract()]
)


def default_contract_registry() -> ContractRegistry:
    return _DEFAULT_REGISTRY
