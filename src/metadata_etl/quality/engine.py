from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from metadata_etl.config import ContractConfig
from metadata_etl.quality.contracts import default_contract_registry
from metadata_etl.quality.privacy import protect_value


@dataclass(frozen=True)
class QualitySummary:
    rule_id: str
    rule_type: str
    records_checked: int
    records_failed: int
    status: str
    timestamp: datetime


@dataclass(frozen=True)
class QuarantineRecord:
    rule_id: str
    rule_type: str
    failure_reason: str
    record_identifier: str
    failed_column: str | None
    failed_value: str | None
    quarantined_at: datetime


@dataclass(frozen=True)
class QualityEvaluation:
    valid_rows: list[tuple[Any, ...]]
    quarantine_records: list[QuarantineRecord]
    summaries: list[QualitySummary]
    rows_contract_passed: int
    rows_quarantined: int


def _record_identifier(row: tuple[Any, ...]) -> str:
    serialized = json.dumps(row, default=str, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _protected_failure_value(
    row: tuple[Any, ...],
    columns: tuple[str, ...],
    positions: dict[str, int],
    policies: dict[str, str],
) -> str | None:
    if len(columns) == 1:
        column = columns[0]
        return protect_value(row[positions[column]], policies.get(column, "none"))
    protected = {
        column: value
        for column in columns
        if (value := protect_value(row[positions[column]], policies.get(column, "none")))
        is not None
    }
    return json.dumps(protected, sort_keys=True, separators=(",", ":")) if protected else None


def evaluate_contracts(
    contracts: Sequence[ContractConfig],
    columns: Sequence[str],
    rows: Sequence[tuple[Any, ...]],
    quarantine_policies: dict[str, str],
) -> QualityEvaluation:
    registry = default_contract_registry()
    positions = {column: index for index, column in enumerate(columns)}
    invalid_indexes: set[int] = set()
    quarantine_records: list[QuarantineRecord] = []
    summaries: list[QualitySummary] = []
    evaluated_at = datetime.now(UTC)

    for contract in contracts:
        failures = registry.get(contract.type).evaluate(contract.values, positions, rows)
        summaries.append(
            QualitySummary(
                rule_id=contract.id,
                rule_type=contract.type,
                records_checked=len(rows),
                records_failed=len(failures),
                status="PASSED" if not failures else "FAILED",
                timestamp=evaluated_at,
            )
        )
        for failure in failures:
            invalid_indexes.add(failure.row_index)
            row = rows[failure.row_index]
            quarantine_records.append(
                QuarantineRecord(
                    rule_id=contract.id,
                    rule_type=contract.type,
                    failure_reason=failure.reason,
                    record_identifier=_record_identifier(row),
                    failed_column=",".join(failure.columns) if failure.columns else None,
                    failed_value=_protected_failure_value(
                        row, failure.columns, positions, quarantine_policies
                    ),
                    quarantined_at=evaluated_at,
                )
            )

    valid_rows = [row for index, row in enumerate(rows) if index not in invalid_indexes]
    return QualityEvaluation(
        valid_rows=valid_rows,
        quarantine_records=quarantine_records,
        summaries=summaries,
        rows_contract_passed=len(valid_rows),
        rows_quarantined=len(invalid_indexes),
    )
