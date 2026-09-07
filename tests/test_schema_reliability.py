from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self

from metadata_etl.config import ColumnConfig
from metadata_etl.postgres import PostgresStore
from metadata_etl.schema import (
    SchemaField,
    SchemaFingerprint,
    canonical_schema_fingerprint,
    detect_schema_drift,
    raw_csv_schema_fingerprint,
)

POLICIES = {
    "added_columns": "warn",
    "removed_columns": "fail",
    "datatype_change": "fail",
    "canonical_change": "fail",
    "raw_structure_change": "warn",
}


def _schema(level: str, *fields: tuple[str, str]) -> SchemaFingerprint:
    return SchemaFingerprint.build(
        level,
        tuple(SchemaField(name, datatype, index) for index, (name, datatype) in enumerate(fields)),
    )


def test_raw_and_canonical_schema_hashes_are_deterministic(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    source.write_text("ID,Amount,Updated At\n001,10.5,2026-09-01 10:00:00\n", encoding="utf-8")
    first = raw_csv_schema_fingerprint(source)
    second = raw_csv_schema_fingerprint(source)
    columns = (
        ColumnConfig("id", "ID", "string", False),
        ColumnConfig("amount", "Amount", "decimal", True),
    )

    assert first.hash == second.hash
    assert first.fields[0].datatype == "string"
    assert canonical_schema_fingerprint(columns).hash == canonical_schema_fingerprint(columns).hash


def test_unchanged_schema_has_no_drift() -> None:
    raw = _schema("raw", ("id", "integer"), ("name", "string"))
    canonical = _schema("canonical", ("id", "integer"), ("name", "string"))

    result = detect_schema_drift(raw, raw, canonical, canonical, POLICIES)

    assert result.events == ()
    assert result.status == "NONE"


def test_added_removed_datatype_and_canonical_changes_are_detected() -> None:
    old_raw = _schema("raw", ("id", "integer"), ("name", "string"))
    new_raw = _schema("raw", ("id", "string"), ("email", "string"))
    old_canonical = _schema("canonical", ("id", "integer"))
    new_canonical = _schema("canonical", ("id", "string"))

    result = detect_schema_drift(old_raw, new_raw, old_canonical, new_canonical, POLICIES)
    types = {event.drift_type for event in result.events}

    assert {
        "column_added",
        "column_removed",
        "datatype_change",
        "possible_column_rename",
        "canonical_change",
    } <= types
    assert result.status == "FAILED"


def test_allow_warn_and_fail_policies_control_status() -> None:
    old = _schema("raw", ("id", "integer"))
    added = _schema("raw", ("id", "integer"), ("name", "string"))
    canonical = _schema("canonical", ("id", "integer"))

    for policy, expected in (("allow", "ALLOWED"), ("warn", "WARN"), ("fail", "FAILED")):
        policies = dict(POLICIES, added_columns=policy)
        assert detect_schema_drift(old, added, canonical, canonical, policies).status == expected


class DriftConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    def transaction(self):
        return nullcontext()

    def cursor(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def executemany(self, statement: str, values: list[tuple[Any, ...]]) -> None:
        self.calls.append((statement, values))


def test_drift_history_is_persisted() -> None:
    old = _schema("raw", ("id", "integer"))
    new = _schema("raw", ("id", "integer"), ("name", "string"))
    canonical = _schema("canonical", ("id", "integer"))
    event = detect_schema_drift(old, new, canonical, canonical, POLICIES).events
    connection = DriftConnection()
    store = PostgresStore("unused")
    store.connection = connection  # type: ignore[assignment]

    store.record_schema_drift(
        run_id="RUN_TEST",
        dataset="example",
        old_raw_hash=old.hash,
        new_raw_hash=new.hash,
        old_canonical_hash=canonical.hash,
        new_canonical_hash=canonical.hash,
        events=event,
        detected_at=datetime.now(UTC),
    )

    assert len(connection.calls) == 1
    assert "schema_drift_history" in connection.calls[0][0]
    assert connection.calls[0][1][0][5] == "column_added"


class ReadConnection:
    def __init__(self) -> None:
        self.transactions = 0
        self.result: tuple[Any, ...] | None = None

    def transaction(self):
        self.transactions += 1
        return nullcontext()

    def execute(self, statement: str, parameters: tuple[str, ...]) -> Self:
        if "raw_schema_json" in statement:
            self.result = ('{"level":"raw","fields":[]}', '{"level":"canonical","fields":[]}')
        else:
            self.result = ("2026-09-01 00:00:00",)
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.result


def test_metadata_reads_close_their_transactions() -> None:
    connection = ReadConnection()
    store = PostgresStore("unused")
    store.connection = connection  # type: ignore[assignment]

    schemas = store.get_previous_successful_schemas("example")
    watermark = store.get_watermark("example", "updated_at")

    assert schemas[0] is not None
    assert watermark == "2026-09-01 00:00:00"
    assert connection.transactions == 2
