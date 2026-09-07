from collections.abc import Iterable

from metadata_etl.connectors.base import BaseConnector
from metadata_etl.connectors.csv import CSVConnector
from metadata_etl.connectors.json import JSONConnector
from metadata_etl.connectors.parquet import ParquetConnector
from metadata_etl.connectors.postgres import PostgresConnector
from metadata_etl.errors import ConfigError


class ConnectorRegistry:
    def __init__(self, connectors: Iterable[BaseConnector]) -> None:
        self._connectors = {connector.source_type: connector for connector in connectors}

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._connectors))

    def get(self, source_type: str) -> BaseConnector:
        try:
            return self._connectors[source_type]
        except KeyError as exc:
            raise ConfigError(f"Unsupported source type {source_type!r}") from exc


_DEFAULT_REGISTRY = ConnectorRegistry(
    [CSVConnector(), JSONConnector(), ParquetConnector(), PostgresConnector()]
)


def default_connector_registry() -> ConnectorRegistry:
    return _DEFAULT_REGISTRY
