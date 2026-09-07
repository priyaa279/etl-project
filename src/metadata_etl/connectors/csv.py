from pathlib import Path

from metadata_etl.config import ETLConfig
from metadata_etl.connectors.base import BaseConnector, ExtractedSource
from metadata_etl.schema import raw_csv_schema_fingerprint
from metadata_etl.source import preserve_raw_copy
from metadata_etl.sql_compiler import csv_relation


class CSVConnector(BaseConnector):
    source_type = "csv"

    def extract(self, config: ETLConfig, run_id: str, raw_root: Path) -> ExtractedSource:
        assert config.source_path is not None
        artifact = preserve_raw_copy(config.source_path, raw_root, config.dataset, run_id)
        return ExtractedSource(
            artifact,
            csv_relation(artifact.path, config.delimiter),
            raw_csv_schema_fingerprint(artifact.path, config.delimiter),
        )
