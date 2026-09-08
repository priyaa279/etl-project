from __future__ import annotations

import hashlib
import re
import shutil
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import yaml
from fastapi import UploadFile

from metadata_etl.config import load_config
from metadata_etl.errors import ConfigError, ProfilingError
from metadata_etl.onboarding import generate_starter_config, profile_file
from metadata_etl.transformations.operators.base import SUPPORTED_TYPES
from metadata_etl_api.catalog import DatasetCatalog
from metadata_etl_api.onboarding_operations import (
    OnboardingCollisionError,
    OnboardingRepository,
)
from metadata_etl_api.settings import APISettings

DATASET_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
SOURCE_EXTENSIONS = {
    "csv": {".csv"},
    "json": {".json", ".jsonl", ".ndjson"},
    "parquet": {".parquet"},
}
DATE_FORMATS = {
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
}
NESTED_REASON = "nested_structure_requires_explicit_normalization"


class OnboardingError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class OnboardingService:
    def __init__(
        self,
        settings: APISettings,
        repository: OnboardingRepository,
        catalog: DatasetCatalog,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.catalog = catalog

    @classmethod
    def from_settings(cls, settings: APISettings) -> OnboardingService:
        return cls(
            settings,
            OnboardingRepository(settings.database_dsn),
            DatasetCatalog(settings.config_dir),
        )

    def capability(self) -> dict[str, Any]:
        return {
            "enabled": self.settings.onboarding_enabled,
            "source_types": sorted(SOURCE_EXTENSIONS),
            "supported_datatypes": sorted(SUPPORTED_TYPES),
            "reason": (
                None
                if self.settings.onboarding_enabled
                else "New-dataset onboarding is disabled in this environment."
            ),
        }

    def _require_enabled(self) -> None:
        if not self.settings.onboarding_enabled:
            raise OnboardingError(403, "New-dataset onboarding is disabled in this environment.")

    async def create(self, dataset_name: str, source_type: str, file: UploadFile) -> dict[str, Any]:
        self._require_enabled()
        dataset = dataset_name.strip()
        source_type = source_type.strip().lower()
        if not DATASET_NAME.fullmatch(dataset):
            raise OnboardingError(
                422,
                "Dataset name must start with a lowercase letter and contain only lowercase "
                "letters, digits, and underscores (maximum 63 characters).",
            )
        if source_type not in SOURCE_EXTENSIONS:
            raise OnboardingError(415, "New-dataset onboarding supports CSV, JSON, and Parquet.")
        if self.catalog.contains(dataset):
            raise OnboardingError(
                409,
                "This dataset already has an approved configuration. Use Upload new data instead.",
            )

        self.repository.ensure_schema()
        if self.repository.find_by_dataset(dataset) is not None:
            raise OnboardingError(409, "An onboarding draft already exists for this dataset.")

        supplied_name = (file.filename or "source").replace("\x00", "")[:512]
        original_filename = Path(supplied_name.replace("\\", "/")).name
        extension = Path(original_filename).suffix.lower()
        if extension not in SOURCE_EXTENSIONS[source_type]:
            expected = ", ".join(sorted(SOURCE_EXTENSIONS[source_type]))
            raise OnboardingError(415, f"Expected a {source_type.upper()} file ({expected}).")

        onboarding_id = str(uuid.uuid4())
        landing_key = PurePosixPath(onboarding_id, f"source{extension}").as_posix()
        root = self.settings.onboarding_root.resolve()
        destination = (root / Path(landing_key)).resolve()
        if not destination.is_relative_to(root):
            raise OnboardingError(400, "Invalid onboarding destination.")
        destination.parent.mkdir(parents=True, exist_ok=False)
        temporary = destination.with_suffix(destination.suffix + ".part")
        digest = hashlib.sha256()
        size = 0
        try:
            with temporary.open("xb") as handle:
                while chunk := await file.read(1024 * 1024):
                    size += len(chunk)
                    if size > self.settings.upload_max_bytes:
                        raise OnboardingError(413, "Uploaded file exceeds the configured limit.")
                    digest.update(chunk)
                    handle.write(chunk)
            temporary.replace(destination)
            record = self.repository.create(
                {
                    "onboarding_id": onboarding_id,
                    "proposed_dataset_name": dataset,
                    "source_type": source_type,
                    "original_filename": original_filename,
                    "size_bytes": size,
                    "sha256": digest.hexdigest(),
                    "landing_key": landing_key,
                    "uploaded_at": datetime.now(UTC),
                }
            )
        except OnboardingCollisionError as exc:
            temporary.unlink(missing_ok=True)
            destination.unlink(missing_ok=True)
            shutil.rmtree(destination.parent, ignore_errors=True)
            raise OnboardingError(
                409, "An onboarding draft already exists for this dataset."
            ) from exc
        except Exception:
            temporary.unlink(missing_ok=True)
            destination.unlink(missing_ok=True)
            shutil.rmtree(destination.parent, ignore_errors=True)
            raise
        finally:
            await file.close()
        return self._public(record)

    def profile(self, onboarding_id: str) -> dict[str, Any]:
        self._require_enabled()
        record = self._record(onboarding_id)
        if record["status"] in {"NEEDS_REVIEW", "REVIEW_IN_PROGRESS", "REVIEW_COMPLETE"}:
            return self.get(onboarding_id)
        if record["status"] == "PROFILING":
            raise OnboardingError(409, "Profiling is already in progress.")
        now = datetime.now(UTC)
        self.repository.begin_profile(onboarding_id, now)
        draft_path: Path | None = None
        try:
            source = self._landing_path(record["landing_key"])
            profile = replace(
                profile_file(source, record["source_type"]),
                dataset_name=record["proposed_dataset_name"],
            )
            draft = generate_starter_config(profile)
            draft["source"]["path"] = (
                PurePosixPath("data/onboarding") / record["landing_key"]
            ).as_posix()
            draft["orchestration"] = {
                "enabled": False,
                "schedule": None,
                "retries": 0,
                "retry_delay_minutes": 5,
            }
            draft_key = f"{onboarding_id}.yaml"
            draft_path = self._draft_path(draft_key, require_exists=False)
            self._write_validated_yaml(draft_path, draft, replace_existing=False)
            profile_result = profile.to_dict()
            profile_result.pop("source_path", None)
            model = self._review_model(record, draft, profile_result, {})
            status = "NEEDS_REVIEW" if model["progress"]["remaining"] else "REVIEW_COMPLETE"
            updated = self.repository.set_profile(
                onboarding_id,
                status=status,
                draft_key=draft_key,
                profile_result=profile_result,
                profiled_at=datetime.now(UTC),
            )
            return self._public(updated)
        except (ConfigError, ProfilingError, OSError, ValueError, yaml.YAMLError) as exc:
            if draft_path is not None:
                draft_path.unlink(missing_ok=True)
            self.repository.set_failed(
                onboarding_id,
                safe_error="The source could not be profiled or converted into a draft configuration.",
                updated_at=datetime.now(UTC),
            )
            raise OnboardingError(
                422, "The source could not be profiled or converted into a draft configuration."
            ) from exc

    def get(self, onboarding_id: str) -> dict[str, Any]:
        return self._public(self._record(onboarding_id))

    def review(self, onboarding_id: str) -> dict[str, Any]:
        record = self._record(onboarding_id)
        draft = self._draft(record)
        return self._review_model(
            record,
            draft,
            record.get("profile_result") or {},
            record.get("key_decisions") or {},
        )

    def yaml_content(self, onboarding_id: str) -> dict[str, str]:
        record = self._record(onboarding_id)
        path = self._draft_path_for_record(record)
        try:
            content = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise OnboardingError(409, "Draft configuration is unavailable.") from exc
        return {"onboarding_id": onboarding_id, "yaml": content}

    def update_schema(
        self, onboarding_id: str, field: str, decision: dict[str, Any]
    ) -> dict[str, Any]:
        self._require_enabled()
        record = self._record(onboarding_id)
        draft = self._draft(record)
        columns = draft.get("columns")
        if not isinstance(columns, dict) or field not in columns:
            raise OnboardingError(404, "Schema field not found.")
        column = columns[field]
        if not isinstance(column, dict):
            raise OnboardingError(409, "Draft schema is invalid.")
        reasons = column.get("review", {}).get("reasons", [])
        if NESTED_REASON in reasons:
            raise OnboardingError(
                409, "Nested JSON normalization is configured in the next onboarding phase."
            )

        canonical_name = str(decision.get("canonical_name", "")).strip()
        if not DATASET_NAME.fullmatch(canonical_name):
            raise OnboardingError(422, "Canonical name must be a safe lowercase identifier.")
        if canonical_name != field and canonical_name in columns:
            raise OnboardingError(409, "Canonical column name already exists.")
        datatype = str(decision.get("datatype", "")).lower()
        if datatype not in SUPPORTED_TYPES:
            raise OnboardingError(422, "Datatype is not supported by the ETL framework.")
        nullable = decision.get("nullable")
        if not isinstance(nullable, bool):
            raise OnboardingError(422, "Nullable must be true or false.")
        date_format = decision.get("format")
        if datatype in {"date", "timestamp"}:
            if date_format not in DATE_FORMATS:
                raise OnboardingError(422, "Choose a supported date or timestamp format.")
            self._validate_examples(column, str(date_format))
            column["format"] = date_format
        else:
            column.pop("format", None)
        column["type"] = datatype
        column["nullable"] = nullable
        if isinstance(column.get("review"), dict) and column["review"].get("required") is True:
            column["review"]["approved"] = True
            column["review"]["approved_by"] = "review_center"
        if canonical_name != field:
            draft["columns"] = {
                (canonical_name if name == field else name): value
                for name, value in columns.items()
            }
        key_decisions = dict(record.get("key_decisions") or {})
        if canonical_name != field and field in key_decisions:
            key_decisions[canonical_name] = key_decisions.pop(field)
        self._force_unapproved(draft)
        path = self._draft_path_for_record(record)
        self._write_validated_yaml(path, draft, replace_existing=True)
        model = self._review_model(
            record,
            draft,
            record.get("profile_result") or {},
            key_decisions,
        )
        self.repository.set_review(
            onboarding_id,
            status=self._status_for(model),
            key_decisions=key_decisions,
            updated_at=datetime.now(UTC),
        )
        return self.review(onboarding_id)

    def update_key_decision(self, onboarding_id: str, field: str, decision: str) -> dict[str, Any]:
        self._require_enabled()
        if decision not in {"accepted", "rejected"}:
            raise OnboardingError(422, "Key decision must be accepted or rejected.")
        record = self._record(onboarding_id)
        draft = self._draft(record)
        columns = draft.get("columns", {})
        column = columns.get(field) if isinstance(columns, dict) else None
        if not isinstance(column, dict) or not column.get("profile", {}).get(
            "possible_key_candidate"
        ):
            raise OnboardingError(404, "Key candidate not found.")
        decisions = dict(record.get("key_decisions") or {})
        decisions[field] = decision
        model = self._review_model(record, draft, record.get("profile_result") or {}, decisions)
        status = self._status_for(model)
        self.repository.set_review(
            onboarding_id,
            status=status,
            key_decisions=decisions,
            updated_at=datetime.now(UTC),
        )
        return self.review(onboarding_id)

    @staticmethod
    def _status_for(model: dict[str, Any]) -> str:
        progress = model["progress"]
        if progress["remaining"] == 0:
            return "REVIEW_COMPLETE"
        if progress["reviewed"]:
            return "REVIEW_IN_PROGRESS"
        return "NEEDS_REVIEW"

    @staticmethod
    def _validate_examples(column: dict[str, Any], date_format: str) -> None:
        examples = column.get("profile", {}).get("observed_examples", [])
        try:
            for value in examples:
                datetime.strptime(str(value), date_format).replace(tzinfo=UTC)
        except ValueError as exc:
            raise OnboardingError(
                422, "The selected format does not parse observed values."
            ) from exc

    @staticmethod
    def _force_unapproved(draft: dict[str, Any]) -> None:
        review = draft.setdefault("review", {})
        review.update(required=True, approved=False, approved_by=None)

    @staticmethod
    def _validate_draft(path: Path) -> None:
        config = load_config(path, require_source=False, allow_unapproved=True)
        if config.raw.get("review", {}).get("approved") is not False:
            raise OnboardingError(409, "Draft configuration must remain unapproved.")

    def _review_model(
        self,
        record: dict[str, Any],
        draft: dict[str, Any],
        profile: dict[str, Any],
        key_decisions: dict[str, str],
    ) -> dict[str, Any]:
        fields: list[dict[str, Any]] = []
        unresolved: list[dict[str, str]] = []
        reviewed = 0
        total = 0
        for canonical, value in draft.get("columns", {}).items():
            review = value.get("review") or {}
            reasons = list(review.get("reasons") or [])
            required = review.get("required") is True
            resolved = review.get("approved") is True
            if required:
                total += 1
                if resolved:
                    reviewed += 1
                else:
                    unresolved.append(
                        {
                            "kind": "schema",
                            "field": canonical,
                            "reason": reasons[0] if reasons else "schema_decision_required",
                        }
                    )
            key_candidate = bool(value.get("profile", {}).get("possible_key_candidate"))
            key_decision = key_decisions.get(canonical)
            if key_candidate:
                total += 1
                if key_decision in {"accepted", "rejected"}:
                    reviewed += 1
                else:
                    unresolved.append(
                        {"kind": "key", "field": canonical, "reason": "possible_key_candidate"}
                    )
            fields.append(
                {
                    "source_name": value.get("source", canonical),
                    "canonical_name": canonical,
                    "datatype": value.get("type"),
                    "nullable": bool(value.get("nullable", True)),
                    "format": value.get("format"),
                    "confidence": value.get("inference", {}).get("confidence"),
                    "reason": value.get("inference", {}).get("reason"),
                    "review_required": required,
                    "review_resolved": resolved,
                    "review_reasons": reasons,
                    "profile": value.get("profile", {}),
                    "key_candidate": key_candidate,
                    "key_decision": key_decision,
                    "editable": NESTED_REASON not in reasons,
                }
            )
        return {
            "onboarding_id": record["onboarding_id"],
            "dataset": record["proposed_dataset_name"],
            "source_type": record["source_type"],
            "original_filename": record["original_filename"],
            "size_bytes": record["size_bytes"],
            "status": record["status"],
            "rows_scanned": profile.get("rows_scanned"),
            "rows_profiled": profile.get("rows_profiled"),
            "exact_duplicate_count": profile.get("exact_duplicate_count"),
            "column_count": len(fields),
            "nested_fields": profile.get("nested_fields", []),
            "fields": fields,
            "unresolved": unresolved,
            "progress": {"reviewed": reviewed, "total": total, "remaining": total - reviewed},
            "final_approved": False,
        }

    def _public(self, record: dict[str, Any]) -> dict[str, Any]:
        profile = record.get("profile_result") or {}
        return {
            key: record.get(key)
            for key in (
                "onboarding_id",
                "proposed_dataset_name",
                "source_type",
                "original_filename",
                "size_bytes",
                "sha256",
                "status",
                "uploaded_at",
                "profiled_at",
                "updated_at",
                "safe_error",
            )
        } | {
            "profile_summary": (
                {
                    "rows_scanned": profile.get("rows_scanned"),
                    "rows_profiled": profile.get("rows_profiled"),
                    "exact_duplicate_count": profile.get("exact_duplicate_count"),
                    "column_count": len(profile.get("columns", [])),
                    "possible_key_candidates": sum(
                        1
                        for column in profile.get("columns", [])
                        if column.get("possible_key_candidate")
                    ),
                    "required_decisions": sum(
                        1 for column in profile.get("columns", []) if column.get("review_required")
                    ),
                    "nested_structure_detected": bool(profile.get("nested_fields")),
                }
                if profile
                else None
            )
        }

    def _record(self, onboarding_id: str) -> dict[str, Any]:
        record = self.repository.get(onboarding_id)
        if record is None:
            raise OnboardingError(404, "Onboarding session not found.")
        return record

    def _landing_path(self, landing_key: str) -> Path:
        root = self.settings.onboarding_root.resolve()
        path = (root / Path(landing_key)).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise OnboardingError(409, "Onboarding source is unavailable.")
        return path

    def _draft_path(self, draft_key: str, *, require_exists: bool = True) -> Path:
        root = self.settings.draft_config_dir.resolve()
        path = (root / Path(draft_key)).resolve()
        if not path.is_relative_to(root) or (require_exists and not path.is_file()):
            raise OnboardingError(409, "Draft configuration is unavailable.")
        return path

    def _draft_path_for_record(self, record: dict[str, Any]) -> Path:
        draft_key = record.get("draft_key")
        if not draft_key:
            raise OnboardingError(409, "This onboarding session has not been profiled.")
        return self._draft_path(draft_key)

    def _draft(self, record: dict[str, Any]) -> dict[str, Any]:
        try:
            value = yaml.safe_load(self._draft_path_for_record(record).read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            raise OnboardingError(409, "Draft configuration is unavailable.") from exc
        if not isinstance(value, dict):
            raise OnboardingError(409, "Draft configuration is invalid.")
        return value

    def _write_validated_yaml(
        self, path: Path, value: dict[str, Any], *, replace_existing: bool
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and not replace_existing:
            raise OnboardingError(409, "Draft configuration already exists.")
        temporary = path.with_suffix(".yaml.part")
        try:
            temporary.write_text(
                yaml.safe_dump(value, sort_keys=False, allow_unicode=True), encoding="utf-8"
            )
            self._validate_draft(temporary)
            temporary.replace(path)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
