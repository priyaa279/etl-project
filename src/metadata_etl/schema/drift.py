from __future__ import annotations

from dataclasses import dataclass

from metadata_etl.schema.fingerprint import SchemaFingerprint


@dataclass(frozen=True)
class DriftEvent:
    schema_level: str
    drift_type: str
    description: str
    policy: str

    @property
    def action_taken(self) -> str:
        return {"allow": "ALLOWED", "warn": "WARNED", "fail": "FAILED"}[self.policy]


@dataclass(frozen=True)
class DriftResult:
    events: tuple[DriftEvent, ...]

    @property
    def status(self) -> str:
        if any(event.policy == "fail" for event in self.events):
            return "FAILED"
        if any(event.policy == "warn" for event in self.events):
            return "WARN"
        if self.events:
            return "ALLOWED"
        return "NONE"

    @property
    def failed(self) -> bool:
        return self.status == "FAILED"


def _raw_events(
    previous: SchemaFingerprint,
    current: SchemaFingerprint,
    policies: dict[str, str],
) -> list[DriftEvent]:
    events: list[DriftEvent] = []
    old = {field.name: field for field in previous.fields}
    new = {field.name: field for field in current.fields}
    added = [field for field in current.fields if field.name not in old]
    removed = [field for field in previous.fields if field.name not in new]
    for field in added:
        events.append(
            DriftEvent(
                "raw",
                "column_added",
                f"Source column {field.name!r} was added at position {field.position}",
                policies["added_columns"],
            )
        )
    for field in removed:
        events.append(
            DriftEvent(
                "raw",
                "column_removed",
                f"Source column {field.name!r} was removed from position {field.position}",
                policies["removed_columns"],
            )
        )
    for name in old.keys() & new.keys():
        if old[name].datatype != new[name].datatype:
            events.append(
                DriftEvent(
                    "raw",
                    "datatype_change",
                    f"Source column {name!r} representation changed from "
                    f"{old[name].datatype!r} to {new[name].datatype!r}",
                    policies["datatype_change"],
                )
            )
    old_order = [field.name for field in previous.fields if field.name in new]
    new_order = [field.name for field in current.fields if field.name in old]
    if old_order != new_order:
        events.append(
            DriftEvent(
                "raw",
                "column_order_change",
                "The order of existing source columns changed",
                policies["raw_structure_change"],
            )
        )
    if len(added) == 1 and len(removed) == 1 and added[0].position == removed[0].position:
        events.append(
            DriftEvent(
                "raw",
                "possible_column_rename",
                f"Source column {removed[0].name!r} may have been renamed to {added[0].name!r}",
                policies["removed_columns"],
            )
        )
    return events


def detect_schema_drift(
    previous_raw: SchemaFingerprint | None,
    current_raw: SchemaFingerprint,
    previous_canonical: SchemaFingerprint | None,
    current_canonical: SchemaFingerprint,
    policies: dict[str, str],
) -> DriftResult:
    """Compare the current schemas with the latest successful-run baseline."""
    events: list[DriftEvent] = []
    if previous_raw is not None and previous_raw.hash != current_raw.hash:
        events.extend(_raw_events(previous_raw, current_raw, policies))
    if previous_canonical is not None and previous_canonical.hash != current_canonical.hash:
        events.append(
            DriftEvent(
                "canonical",
                "canonical_change",
                "Approved canonical names, datatypes, nullability, order, or formats changed",
                policies["canonical_change"],
            )
        )
    return DriftResult(tuple(events))
