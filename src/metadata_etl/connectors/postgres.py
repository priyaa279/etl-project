from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import psycopg
from psycopg import sql

from metadata_etl.config import ETLConfig
from metadata_etl.connectors.base import BaseConnector, ExtractedSource
from metadata_etl.errors import ExtractionError
from metadata_etl.schema import SchemaField, SchemaFingerprint
from metadata_etl.source import RawArtifact
from metadata_etl.sql_compiler import csv_relation


def _deterministic_row_key(row: tuple[object, ...]) -> tuple[tuple[str, str], ...]:
    return tuple((type(value).__name__, repr(value)) for value in row)


class PostgresConnector(BaseConnector):
    source_type = "postgres"

    def extract(self, config: ETLConfig, run_id: str, raw_root: Path) -> ExtractedSource:
        assert config.source_table is not None
        destination_dir = raw_root / config.dataset / run_id.lower()
        snapshot = destination_dir / f"{config.source_table[1]}.csv"
        try:
            destination_dir.mkdir(parents=True, exist_ok=False)
            with psycopg.connect(config.source_dsn) as connection:
                cursor = connection.execute(
                    sql.SQL("SELECT * FROM {}").format(sql.Identifier(*config.source_table))
                )
                rows = sorted(cursor.fetchall(), key=_deterministic_row_key)
                description = cursor.description
                if description is None:
                    raise ExtractionError("PostgreSQL source query returned no schema")
                type_oids = sorted({column.type_code for column in description})
                type_rows = connection.execute(
                    "SELECT oid, typname FROM pg_type WHERE oid = ANY(%s)", (type_oids,)
                ).fetchall()
                types = {row[0]: row[1] for row in type_rows}
            with snapshot.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerow(column.name for column in description)
                writer.writerows(rows)
            digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
        except (OSError, psycopg.Error) as exc:
            raise ExtractionError(f"Could not extract PostgreSQL source table: {exc}") from exc

        fields = tuple(
            SchemaField(column.name, types.get(column.type_code, str(column.type_code)), index)
            for index, column in enumerate(description)
        )
        artifact = RawArtifact(snapshot, digest, snapshot.stat().st_size)
        return ExtractedSource(
            artifact,
            csv_relation(snapshot, config.delimiter),
            SchemaFingerprint.build("raw", fields),
        )
