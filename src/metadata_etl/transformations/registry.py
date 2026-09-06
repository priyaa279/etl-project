from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from metadata_etl.errors import ConfigError
from metadata_etl.transformations.operators import (
    CastOperator,
    DeduplicateOperator,
    DeriveOperator,
    FilterOperator,
    MapOperator,
)
from metadata_etl.transformations.operators.base import Schema, TransformationOperator


class TransformationRegistry:
    def __init__(self, operators: Iterable[TransformationOperator] = ()) -> None:
        self._operators: dict[str, TransformationOperator] = {}
        for operator in operators:
            self.register(operator)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._operators))

    def register(self, operator: TransformationOperator) -> None:
        if operator.name in self._operators:
            raise ValueError(f"Transformation operator already registered: {operator.name}")
        self._operators[operator.name] = operator

    def get(self, name: str) -> TransformationOperator:
        try:
            return self._operators[name]
        except KeyError as exc:
            raise ConfigError(
                f"Unsupported transformation type {name!r}; supported types: {list(self.names)}"
            ) from exc

    def validate(
        self, name: str, spec: dict[str, Any], path: str, schema: Schema
    ) -> dict[str, Any]:
        return self.get(name).validate(spec, path, schema)

    def compile(self, name: str, spec: dict[str, Any], input_relation: str) -> str:
        return self.get(name).compile(spec, input_relation)


_DEFAULT_REGISTRY = TransformationRegistry(
    [CastOperator(), FilterOperator(), DeriveOperator(), MapOperator(), DeduplicateOperator()]
)


def default_registry() -> TransformationRegistry:
    return _DEFAULT_REGISTRY
