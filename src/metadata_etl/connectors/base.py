from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from metadata_etl.config import ETLConfig
from metadata_etl.schema import SchemaFingerprint
from metadata_etl.source import RawArtifact


@dataclass(frozen=True)
class ExtractedSource:
    artifact: RawArtifact
    relation_sql: str
    raw_schema: SchemaFingerprint


class BaseConnector(ABC):
    source_type: str

    @abstractmethod
    def extract(self, config: ETLConfig, run_id: str, raw_root: Path) -> ExtractedSource:
        """Preserve an extracted source and expose it as a DuckDB relation."""
