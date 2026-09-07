from pathlib import Path

from metadata_etl.config import ETLConfig
from metadata_etl.connectors.base import BaseConnector, ExtractedSource
from metadata_etl.schema import parquet_schema_fingerprint
from metadata_etl.source import preserve_raw_copy
from metadata_etl.transformations.sql import sql_literal


class ParquetConnector(BaseConnector):
    source_type = "parquet"

    def extract(self, config: ETLConfig, run_id: str, raw_root: Path) -> ExtractedSource:
        assert config.source_path is not None
        artifact = preserve_raw_copy(config.source_path, raw_root, config.dataset, run_id)
        relation = f"read_parquet({sql_literal(artifact.path.as_posix())})"
        return ExtractedSource(artifact, relation, parquet_schema_fingerprint(artifact.path))
