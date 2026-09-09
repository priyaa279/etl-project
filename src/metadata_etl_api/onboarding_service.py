from __future__ import annotations

import hashlib
import re
import shutil
import time
import uuid
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import yaml
from fastapi import UploadFile

from metadata_etl.config import ETLConfig, load_config
from metadata_etl.errors import ConfigError, ETLError, ProfilingError
from metadata_etl.onboarding import generate_starter_config, profile_file
from metadata_etl.orchestration import dag_id_for_dataset
from metadata_etl.transformations.operators.base import SUPPORTED_TYPES
from metadata_etl.transformations.registry import default_registry
from metadata_etl.validation import validate_plan
from metadata_etl_api.airflow_client import AirflowClient, AirflowError
from metadata_etl_api.catalog import DatasetCatalog
from metadata_etl_api.git_adapter import GitConfigAdapter, GitPromotionError
from metadata_etl_api.onboarding_operations import (
    OnboardingCollisionError,
    OnboardingRepository,
    OnboardingRepositoryError,
)
from metadata_etl_api.repository import ObservabilityRepository
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
DRIFT_SETTINGS = {
    "added_columns",
    "removed_columns",
    "datatype_change",
    "canonical_change",
    "raw_structure_change",
}


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
        airflow: AirflowClient | None = None,
        observability: ObservabilityRepository | None = None,
        git: GitConfigAdapter | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.catalog = catalog
        self.airflow = airflow or AirflowClient(
            settings.airflow_api_url,
            username=settings.airflow_username,
            password=settings.airflow_password,
            token=settings.airflow_token,
            password_file=settings.airflow_password_file,
        )
        self.observability = observability or ObservabilityRepository(settings.database_dsn)
        self.git = git or GitConfigAdapter(
            settings.repository_root,
            executable=settings.git_executable,
            push_enabled=settings.git_push_enabled,
            remote=settings.git_remote,
            branch=settings.git_branch,
        )
        self._schema_ready = False

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

        self._ensure_schema()
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
        if record["status"] in {
            "NEEDS_REVIEW",
            "REVIEW_IN_PROGRESS",
            "REVIEW_COMPLETE",
            "CONFIGURING",
            "READY_FOR_VALIDATION",
            "VALIDATION_FAILED",
            "READY_FOR_APPROVAL",
            "APPROVING",
            "APPROVED",
            "ACTIVATING",
            "WAITING_FOR_DAG",
            "ACTIVATION_FAILED",
            "READY_FOR_FIRST_RUN",
            "TRIGGERING",
            "QUEUED",
            "RUNNING",
            "SUCCEEDED",
            "FIRST_RUN_FAILED",
        }:
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
            draft["source"]["path"] = self._stable_source_config_path(record)
            draft["orchestration"] = {
                "enabled": True,
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
        self._require_editable(record)
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
        self._require_editable(record)
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

    def configuration(self, onboarding_id: str) -> dict[str, Any]:
        record = self._record(onboarding_id)
        draft = self._draft(record)
        review = self._review_model(
            record,
            draft,
            record.get("profile_result") or {},
            record.get("key_decisions") or {},
        )
        path = self._draft_path_for_record(record)
        current_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        validated_hash = record.get("validation_hash")
        final_approved = record.get("approved_at") is not None
        validation_current = bool(validated_hash) and (
            validated_hash == current_hash or final_approved
        )
        if final_approved or (validation_current and record["status"] == "READY_FOR_APPROVAL"):
            result = "VALID"
        elif validation_current and record["status"] == "VALIDATION_FAILED":
            result = "INVALID"
        else:
            result = "NOT_VALIDATED"
        post_schema = self._post_transformation_schema(path, draft)
        columns = [
            {
                "name": name,
                "source": value.get("source", name),
                "datatype": value.get("type"),
                "nullable": bool(value.get("nullable", True)),
                "format": value.get("format"),
                "classification": value.get("classification"),
                "quarantine_value": value.get("quarantine", {}).get("value", "none"),
            }
            for name, value in draft.get("columns", {}).items()
        ]
        nested = bool((record.get("profile_result") or {}).get("nested_fields"))
        json_normalization = draft.get("normalization", {}).get("json")
        activation_ready = final_approved or self._activation_ready(record, draft)
        return {
            "onboarding_id": onboarding_id,
            "dataset": record["proposed_dataset_name"],
            "source_type": record["source_type"],
            "status": record["status"],
            "review_complete": review["progress"]["remaining"] == 0,
            "normalization_complete": not nested or bool(json_normalization),
            "activation_ready": activation_ready,
            "columns": columns,
            "post_transformation_columns": [
                {"name": name, "datatype": datatype} for name, datatype in post_schema.items()
            ],
            "accepted_key_candidates": sorted(
                field
                for field, decision in (record.get("key_decisions") or {}).items()
                if decision == "accepted"
            ),
            "normalization": deepcopy(json_normalization),
            "transformations": deepcopy(draft.get("transformations", [])),
            "contracts": deepcopy(draft.get("contracts", [])),
            "load": deepcopy(draft.get("load", {})),
            "schema_drift": deepcopy(draft.get("schema_drift", {})),
            "orchestration": deepcopy(draft.get("orchestration", {})),
            "validation": {
                "result": result,
                "draft_hash": current_hash,
                "validated_hash": validated_hash if validation_current else None,
                "validated_at": record.get("validated_at") if validation_current else None,
                "errors": deepcopy(record.get("validation_errors") or [])
                if validation_current
                else [],
            },
            "final_approved": final_approved,
            "approval": self._approval_metadata(record),
        }

    def update_normalization(self, onboarding_id: str, value: dict[str, Any]) -> dict[str, Any]:
        self._require_enabled()
        record = self._record(onboarding_id)
        self._require_editable(record)
        if record["source_type"] != "json":
            raise OnboardingError(409, "JSON normalization applies only to JSON onboarding.")
        draft = self._draft(record)
        fields = value.get("fields")
        columns = value.get("columns")
        if not isinstance(fields, dict) or not isinstance(columns, dict) or not columns:
            raise OnboardingError(422, "JSON normalization requires field mappings and columns.")
        json_config: dict[str, Any] = {"fields": deepcopy(fields)}
        if value.get("root_path"):
            json_config["root_path"] = value["root_path"]
        explode = value.get("explode")
        if explode is not None:
            if not isinstance(explode, dict):
                raise OnboardingError(422, "JSON explode configuration must be a mapping.")
            json_config["explode"] = deepcopy(explode)
        configured_columns: dict[str, dict[str, Any]] = {}
        for name, settings in columns.items():
            if not isinstance(settings, dict):
                raise OnboardingError(422, "Every normalized column must be a mapping.")
            column = {
                "source": name,
                "type": settings.get("type"),
                "nullable": settings.get("nullable", True),
            }
            if settings.get("format") is not None:
                column["format"] = settings["format"]
            if settings.get("classification") is not None:
                column["classification"] = settings["classification"]
            column["quarantine"] = {"value": settings.get("quarantine_value", "none")}
            configured_columns[str(name)] = column
        draft.setdefault("normalization", {})["json"] = json_config
        draft["columns"] = configured_columns
        return self._persist_configuration(record, draft)

    def add_transformation(self, onboarding_id: str, value: dict[str, Any]) -> dict[str, Any]:
        record, draft = self._advanced_draft(onboarding_id)
        transformations = draft.setdefault("transformations", [])
        if not isinstance(transformations, list):
            raise OnboardingError(409, "Draft transformations are invalid.")
        transformations.append(self._transformation_value(value))
        return self._persist_configuration(record, draft)

    def edit_transformation(
        self, onboarding_id: str, item_id: str, value: dict[str, Any]
    ) -> dict[str, Any]:
        record, draft = self._advanced_draft(onboarding_id)
        index = self._item_index(draft.get("transformations"), item_id, "Transformation")
        if value.get("id") not in {None, item_id}:
            raise OnboardingError(422, "Transformation ID cannot be changed.")
        replacement = self._transformation_value(value)
        replacement["id"] = item_id
        draft["transformations"][index] = replacement
        return self._persist_configuration(record, draft)

    def delete_transformation(self, onboarding_id: str, item_id: str) -> dict[str, Any]:
        record, draft = self._advanced_draft(onboarding_id)
        index = self._item_index(draft.get("transformations"), item_id, "Transformation")
        del draft["transformations"][index]
        return self._persist_configuration(record, draft)

    def move_transformation(
        self, onboarding_id: str, item_id: str, direction: str
    ) -> dict[str, Any]:
        record, draft = self._advanced_draft(onboarding_id)
        items = draft.get("transformations")
        index = self._item_index(items, item_id, "Transformation")
        target = index - 1 if direction == "up" else index + 1
        if target < 0 or target >= len(items):
            raise OnboardingError(409, "Transformation is already at that boundary.")
        items[index], items[target] = items[target], items[index]
        return self._persist_configuration(record, draft)

    def add_contract(self, onboarding_id: str, value: dict[str, Any]) -> dict[str, Any]:
        record, draft = self._advanced_draft(onboarding_id)
        contracts = draft.setdefault("contracts", [])
        if not isinstance(contracts, list):
            raise OnboardingError(409, "Draft contracts are invalid.")
        contracts.append(deepcopy(value))
        return self._persist_configuration(record, draft)

    def edit_contract(
        self, onboarding_id: str, item_id: str, value: dict[str, Any]
    ) -> dict[str, Any]:
        record, draft = self._advanced_draft(onboarding_id)
        index = self._item_index(draft.get("contracts"), item_id, "Contract")
        if value.get("id") not in {None, item_id}:
            raise OnboardingError(422, "Contract ID cannot be changed.")
        replacement = deepcopy(value)
        replacement["id"] = item_id
        draft["contracts"][index] = replacement
        return self._persist_configuration(record, draft)

    def delete_contract(self, onboarding_id: str, item_id: str) -> dict[str, Any]:
        record, draft = self._advanced_draft(onboarding_id)
        index = self._item_index(draft.get("contracts"), item_id, "Contract")
        del draft["contracts"][index]
        return self._persist_configuration(record, draft)

    def update_column_privacy(
        self, onboarding_id: str, field: str, decision: dict[str, Any]
    ) -> dict[str, Any]:
        record, draft = self._advanced_draft(onboarding_id)
        column = draft.get("columns", {}).get(field)
        if not isinstance(column, dict):
            raise OnboardingError(404, "Column not found.")
        classification = decision.get("classification")
        if classification is None:
            column.pop("classification", None)
        else:
            column["classification"] = classification
        column["quarantine"] = {"value": decision.get("quarantine_value")}
        return self._persist_configuration(record, draft)

    def update_load(self, onboarding_id: str, value: dict[str, Any]) -> dict[str, Any]:
        record, draft = self._advanced_draft(onboarding_id)
        strategy = str(value.get("strategy", "")).lower()
        section_fields = {
            "full": set(),
            "incremental": {"watermark"},
            "upsert": {"keys"},
            "scd2": {"keys", "tracked_columns", "effective_timestamp", "history_columns"},
        }
        if strategy not in section_fields:
            raise OnboardingError(422, "Load strategy is not supported.")
        unknown = set(value) - {"strategy"} - section_fields[strategy]
        if unknown:
            raise OnboardingError(422, f"Unsupported load settings: {sorted(unknown)}")
        current = draft.get("load", {})
        load = {
            "strategy": strategy,
            "connection_env": current.get("connection_env", "ETL_POSTGRES_DSN"),
            "schema": current.get("schema", "public"),
            "staging_table": current.get("staging_table"),
            "target_table": current.get("target_table"),
        }
        for key in section_fields[strategy]:
            if key in value:
                load[key] = deepcopy(value[key])
        draft["load"] = load
        return self._persist_configuration(record, draft)

    def update_schema_drift(self, onboarding_id: str, value: dict[str, str]) -> dict[str, Any]:
        record, draft = self._advanced_draft(onboarding_id)
        if set(value) - DRIFT_SETTINGS:
            raise OnboardingError(422, "Schema-drift setting is not supported.")
        merged = dict(draft.get("schema_drift", {}))
        merged.update(value)
        draft["schema_drift"] = merged
        return self._persist_configuration(record, draft)

    def validate_configuration(self, onboarding_id: str) -> dict[str, Any]:
        self._require_enabled()
        record = self._record(onboarding_id)
        self._require_editable(record)
        draft = self._draft(record)
        path = self._draft_path_for_record(record)
        expected_source = self._stable_source_config_path(record)
        expected_orchestration = {
            "enabled": True,
            "schedule": None,
            "retries": 0,
            "retry_delay_minutes": 5,
        }
        if (
            draft.get("source", {}).get("path") != expected_source
            or draft.get("orchestration") != expected_orchestration
        ):
            draft.setdefault("source", {})["path"] = expected_source
            draft["orchestration"] = expected_orchestration
            self._force_unapproved(draft)
            self._write_validated_yaml(path, draft, replace_existing=True, runtime_record=record)
            self.repository.set_configuration(
                onboarding_id, status="READY_FOR_VALIDATION", updated_at=datetime.now(UTC)
            )
            record = self._record(onboarding_id)
        current_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        review = self._review_model(
            record,
            draft,
            record.get("profile_result") or {},
            record.get("key_decisions") or {},
        )
        errors: list[dict[str, str]] = []
        for item in review["unresolved"]:
            errors.append(
                {
                    "section": "schema_review",
                    "field": item["field"],
                    "message": f"Resolve {item['reason']} before validation.",
                }
            )
        nested = bool((record.get("profile_result") or {}).get("nested_fields"))
        if nested and not draft.get("normalization", {}).get("json"):
            errors.append(
                {
                    "section": "normalization",
                    "message": "Nested JSON requires explicit normalization.",
                }
            )
        if not errors:
            try:
                config = self._validate_draft(path)
                validate_plan(config, source_override=self._landing_path(record["landing_key"]))
            except (ETLError, OSError, ValueError) as exc:
                errors.append({"section": "configuration", "message": self._safe_detail(exc)})
        now = datetime.now(UTC)
        status = "VALIDATION_FAILED" if errors else "READY_FOR_APPROVAL"
        self.repository.set_validation(
            onboarding_id,
            status=status,
            validation_hash=current_hash,
            validation_errors=errors,
            validated_at=now,
        )
        return self.configuration(onboarding_id)

    def approve(
        self,
        onboarding_id: str,
        *,
        expected_hash: str,
        approved_by: str,
        acknowledged: bool,
    ) -> dict[str, Any]:
        """Approve exactly the validated draft, promote it, and request DAG discovery."""
        self._require_enabled()
        approver = approved_by.strip()
        if not acknowledged:
            raise OnboardingError(422, "Approval acknowledgment is required.")
        if not approver or len(approver) > 120:
            raise OnboardingError(422, "Approved by is required (maximum 120 characters).")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._@+-]*", approver):
            raise OnboardingError(422, "Approved by contains unsupported characters.")
        if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
            raise OnboardingError(422, "Expected draft hash must be a SHA-256 value.")

        record = self._record(onboarding_id)
        if record.get("approved_at") is not None:
            return self.completion(onboarding_id)
        draft_path = self._draft_path_for_record(record)
        if record["status"] == "APPROVING":
            if record.get("validation_hash") != expected_hash:
                raise OnboardingError(409, "Validated configuration hash does not match.")
            if self._resume_approval(record, draft_path):
                return self.activate(onboarding_id)
            return self.completion(onboarding_id)
        current_hash = hashlib.sha256(draft_path.read_bytes()).hexdigest()
        if (
            record["status"] != "READY_FOR_APPROVAL"
            or record.get("validation_hash") != expected_hash
            or current_hash != expected_hash
        ):
            if record["status"] == "READY_FOR_APPROVAL" and current_hash != expected_hash:
                self.repository.invalidate_approval(onboarding_id, updated_at=datetime.now(UTC))
            raise OnboardingError(
                409, "The draft no longer matches its validated version. Validate it again."
            )
        draft = self._draft(record)
        if not self._activation_ready(record, draft):
            self.repository.invalidate_approval(onboarding_id, updated_at=datetime.now(UTC))
            raise OnboardingError(
                409,
                "The draft predates activation support. Validate it again before approval.",
            )
        review = self._review_model(
            record,
            draft,
            record.get("profile_result") or {},
            record.get("key_decisions") or {},
        )
        if review["progress"]["remaining"]:
            raise OnboardingError(409, "Human review is incomplete.")

        try:
            claimed, should_promote = self.repository.claim_approval(
                onboarding_id,
                expected_hash=expected_hash,
                approved_by=approver,
                updated_at=datetime.now(UTC),
            )
        except OnboardingRepositoryError as exc:
            raise OnboardingError(409, str(exc)) from exc
        if not should_promote:
            return self.completion(onboarding_id)

        config_path = self._approved_config_path(claimed)
        source_path = self._approved_source_path(claimed)
        source_existed = source_path.exists()
        config_created = False
        committed = False
        temporary_source = source_path.with_suffix(source_path.suffix + ".part")
        temporary_config = config_path.with_suffix(".yaml.part")
        try:
            if config_path.exists() or self.catalog.contains(claimed["proposed_dataset_name"]):
                raise OnboardingError(
                    409, "An approved configuration already exists for this dataset."
                )
            self.git.ensure_clean()
            if source_existed:
                raise OnboardingError(
                    409, "A stable source artifact already exists for this dataset."
                )
            source_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(self._landing_path(claimed["landing_key"]), temporary_source)
            if hashlib.sha256(temporary_source.read_bytes()).hexdigest() != claimed["sha256"]:
                raise OnboardingError(409, "Source materialization checksum did not match.")
            temporary_source.replace(source_path)

            approved = deepcopy(draft)
            approved["review"] = {
                "required": True,
                "approved": True,
                "approved_by": approver,
            }
            temporary_config.write_text(
                yaml.safe_dump(approved, sort_keys=False, allow_unicode=True), encoding="utf-8"
            )
            config = load_config(temporary_config, require_source=False)
            validate_plan(config, source_override=source_path)
            approved_hash = hashlib.sha256(temporary_config.read_bytes()).hexdigest()
            temporary_config.replace(config_path)
            config_created = True

            version = self.git.commit_config(config_path, claimed["proposed_dataset_name"])
            committed = True
            now = datetime.now(UTC)
            self.repository.finish_approval(
                onboarding_id,
                approved_at=now,
                approved_by=approver,
                approved_config_hash=approved_hash,
                approved_config_key=config_path.name,
                source_key=self._stable_source_key(claimed),
                git_commit_sha=version.commit_sha,
                git_push_status=version.push_status,
                dag_id=dag_id_for_dataset(claimed["proposed_dataset_name"]),
            )
            try:
                self._archive_draft(draft_path, onboarding_id)
            except OSError:
                # The approved top-level config is canonical; a stale ignored draft is harmless.
                pass
        except (GitPromotionError, ETLError, OSError, ValueError, yaml.YAMLError) as exc:
            temporary_source.unlink(missing_ok=True)
            temporary_config.unlink(missing_ok=True)
            if not committed:
                if config_created:
                    config_path.unlink(missing_ok=True)
                if not source_existed:
                    source_path.unlink(missing_ok=True)
            self.repository.fail_approval(
                onboarding_id,
                safe_error="Configuration approval could not be completed safely.",
                updated_at=datetime.now(UTC),
            )
            raise OnboardingError(409, self._safe_detail(exc)) from exc
        except OnboardingError as exc:
            temporary_source.unlink(missing_ok=True)
            temporary_config.unlink(missing_ok=True)
            if not committed:
                if config_created:
                    config_path.unlink(missing_ok=True)
                if not source_existed:
                    source_path.unlink(missing_ok=True)
            self.repository.fail_approval(
                onboarding_id, safe_error=exc.detail, updated_at=datetime.now(UTC)
            )
            raise
        return self.activate(onboarding_id)

    def _resume_approval(self, record: dict[str, Any], draft_path: Path) -> bool:
        """Finalize durable approval metadata without creating a duplicate Git commit."""
        config_path = self._approved_config_path(record)
        source_path = self._approved_source_path(record)
        if not config_path.is_file() or not source_path.is_file():
            return False
        if hashlib.sha256(source_path.read_bytes()).hexdigest() != record["sha256"]:
            raise OnboardingError(409, "Approved source checksum does not match the upload.")
        approved = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        if not isinstance(approved, dict) or approved.get("review") != {
            "required": True,
            "approved": True,
            "approved_by": record["approved_by"],
        }:
            raise OnboardingError(409, "Approved configuration evidence is inconsistent.")
        config = load_config(config_path, require_source=False)
        validate_plan(config, source_override=source_path)
        version = self.git.recover_config_commit(config_path, record["proposed_dataset_name"])
        if version is None:
            return False
        approved_hash = hashlib.sha256(config_path.read_bytes()).hexdigest()
        now = datetime.now(UTC)
        self.repository.finish_approval(
            record["onboarding_id"],
            approved_at=now,
            approved_by=record["approved_by"],
            approved_config_hash=approved_hash,
            approved_config_key=config_path.name,
            source_key=self._stable_source_key(record),
            git_commit_sha=version.commit_sha,
            git_push_status=version.push_status,
            dag_id=dag_id_for_dataset(record["proposed_dataset_name"]),
        )
        try:
            self._archive_draft(draft_path, record["onboarding_id"])
        except OSError:
            pass
        return True

    def activate(self, onboarding_id: str) -> dict[str, Any]:
        """Poll only for the already-approved generic DAG; never trigger ETL."""
        self._require_enabled()
        record = self._record(onboarding_id)
        if record.get("approved_at") is None:
            raise OnboardingError(409, "Configuration must be approved before activation.")
        if record["status"] in {
            "READY_FOR_FIRST_RUN",
            "TRIGGERING",
            "QUEUED",
            "RUNNING",
            "SUCCEEDED",
            "FIRST_RUN_FAILED",
        }:
            return self.completion(onboarding_id)
        self.repository.begin_activation(onboarding_id, datetime.now(UTC))
        deadline = time.monotonic() + max(0, self.settings.airflow_discovery_timeout_seconds)
        discovered = False
        while True:
            try:
                response = self.airflow.dag(record["dag_id"])
                discovered = response.get("dag_id") == record["dag_id"]
            except AirflowError:
                discovered = False
            if discovered or time.monotonic() >= deadline:
                break
            time.sleep(max(0.05, self.settings.airflow_discovery_poll_seconds))
        self.repository.finish_activation(
            onboarding_id, discovered=discovered, updated_at=datetime.now(UTC)
        )
        return self.completion(onboarding_id)

    def run_first(self, onboarding_id: str, *, retry: bool = False) -> dict[str, Any]:
        """Explicitly trigger the first run with one exact durable correlation identifier."""
        self._require_enabled()
        record = self._record(onboarding_id)
        next_attempt = int(record.get("first_run_attempt") or 0) + 1
        correlation_id = f"onboarding:{onboarding_id}:first-run:{next_attempt}"
        airflow_run_id = f"onboarding__{onboarding_id.replace('-', '')}__{next_attempt}"
        try:
            claimed, should_trigger = self.repository.claim_first_run(
                onboarding_id,
                correlation_id=correlation_id,
                airflow_run_id=airflow_run_id,
                retry=retry,
                updated_at=datetime.now(UTC),
            )
        except OnboardingRepositoryError as exc:
            raise OnboardingError(409, str(exc)) from exc
        if not should_trigger:
            return self.completion(onboarding_id)
        source_override = (
            PurePosixPath(self.settings.airflow_source_root) / claimed["source_key"]
        ).as_posix()
        try:
            response = self.airflow.trigger(
                claimed["dag_id"],
                claimed["airflow_run_id"],
                source_override=source_override,
                correlation_id=claimed["first_run_correlation_id"],
                git_commit_sha=claimed["git_commit_sha"],
            )
            state = str(response.get("state") or "queued").upper()
            self.repository.mark_first_run_triggered(
                onboarding_id, airflow_state=state, updated_at=datetime.now(UTC)
            )
        except AirflowError as exc:
            self.repository.sync_first_run(
                onboarding_id,
                status="FIRST_RUN_FAILED",
                airflow_state="FAILED",
                etl_run_id=None,
                completed_at=datetime.now(UTC),
                safe_error="Airflow could not accept the first-run request.",
                updated_at=datetime.now(UTC),
            )
            raise OnboardingError(502, "Airflow could not accept the first-run request.") from exc
        return self.completion(onboarding_id)

    def completion(self, onboarding_id: str) -> dict[str, Any]:
        record = self._record(onboarding_id)
        etl_run = None
        correlation_id = record.get("first_run_correlation_id")
        if correlation_id:
            etl_run = self.observability.run_by_correlation(correlation_id)
        airflow_state = record.get("airflow_state")
        if record.get("dag_id") and record.get("airflow_run_id"):
            try:
                response = self.airflow.run(record["dag_id"], record["airflow_run_id"])
                airflow_state = str(response.get("state") or airflow_state or "queued").upper()
            except AirflowError:
                airflow_state = airflow_state or "UNKNOWN"

        status = record["status"]
        completed_at = record.get("completed_at")
        safe_error = record.get("safe_error")
        if etl_run is not None:
            if etl_run["status"] == "SUCCEEDED":
                status = "SUCCEEDED"
                safe_error = None
            elif etl_run["status"] == "FAILED":
                status = "FIRST_RUN_FAILED"
                safe_error = "The first ETL run failed."
            else:
                status = "RUNNING"
                safe_error = None
            completed_at = etl_run.get("completed_at")
        elif airflow_state == "RUNNING":
            status = "RUNNING"
        elif airflow_state in {"FAILED", "UPSTREAM_FAILED"}:
            status = "FIRST_RUN_FAILED"
            completed_at = completed_at or datetime.now(UTC)
            safe_error = "The first Airflow run failed."
        elif airflow_state == "SUCCESS" and correlation_id:
            status = "FIRST_RUN_FAILED"
            completed_at = completed_at or datetime.now(UTC)
            safe_error = "Airflow completed without a correlated ETL ledger row."
        elif airflow_state in {"QUEUED", "SCHEDULED"}:
            status = "QUEUED"
        if correlation_id and (
            status != record["status"]
            or airflow_state != record.get("airflow_state")
            or (etl_run and record.get("etl_run_id") != etl_run["run_id"])
        ):
            record = self.repository.sync_first_run(
                onboarding_id,
                status=status,
                airflow_state=airflow_state,
                etl_run_id=etl_run["run_id"] if etl_run else None,
                completed_at=completed_at,
                safe_error=safe_error,
                updated_at=datetime.now(UTC),
            )
        return self._completion_model(record, etl_run)

    def _advanced_draft(self, onboarding_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        self._require_enabled()
        record = self._record(onboarding_id)
        self._require_editable(record)
        draft = self._draft(record)
        review = self._review_model(
            record,
            draft,
            record.get("profile_result") or {},
            record.get("key_decisions") or {},
        )
        if review["progress"]["remaining"]:
            raise OnboardingError(409, "Complete schema and profiler review before configuration.")
        return record, draft

    def _persist_configuration(
        self, record: dict[str, Any], draft: dict[str, Any]
    ) -> dict[str, Any]:
        self._force_unapproved(draft)
        try:
            self._write_validated_yaml(
                self._draft_path_for_record(record),
                draft,
                replace_existing=True,
                runtime_record=record,
            )
        except (ETLError, OSError, ValueError, yaml.YAMLError) as exc:
            raise OnboardingError(422, self._safe_detail(exc)) from exc
        self.repository.set_configuration(
            record["onboarding_id"], status="CONFIGURING", updated_at=datetime.now(UTC)
        )
        return self.configuration(record["onboarding_id"])

    @staticmethod
    def _item_index(items: Any, item_id: str, label: str) -> int:
        if not isinstance(items, list):
            raise OnboardingError(409, f"Draft {label.lower()} list is invalid.")
        for index, item in enumerate(items):
            if isinstance(item, dict) and item.get("id") == item_id:
                return index
        raise OnboardingError(404, f"{label} not found.")

    @staticmethod
    def _transformation_value(value: dict[str, Any]) -> dict[str, Any]:
        result = deepcopy(value)
        if result.get("type") != "map" or not isinstance(result.get("mappings"), list):
            return result
        configured: dict[Any, Any] = {}
        for index, row in enumerate(result["mappings"]):
            if not isinstance(row, dict) or set(row) != {"source", "result"}:
                raise OnboardingError(
                    422, f"Map row {index + 1} requires source and result values."
                )
            source = row["source"]
            if isinstance(source, dict | list) or source in configured:
                raise OnboardingError(422, "Map source values must be scalar and unique.")
            configured[source] = row["result"]
        result["mappings"] = configured
        return result

    @staticmethod
    def _post_transformation_schema(path: Path, draft: dict[str, Any]) -> dict[str, str]:
        config = load_config(path, require_source=False, allow_unapproved=True)
        schema = {column.name: column.datatype for column in config.columns}
        registry = default_registry()
        for index, item in enumerate(draft.get("transformations", [])):
            registry.validate(item.get("type", ""), item, f"transformations[{index}]", schema)
        return schema

    def _safe_detail(self, exc: Exception) -> str:
        detail = str(exc) or "Configuration validation failed."
        for path in (
            self.settings.onboarding_root.resolve(),
            self.settings.draft_config_dir.resolve(),
            Path.cwd().resolve(),
        ):
            detail = detail.replace(str(path), "<internal>")
            detail = detail.replace(path.as_posix(), "<internal>")
        return detail[:600]

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
    def _validate_draft(path: Path) -> ETLConfig:
        config = load_config(path, require_source=False, allow_unapproved=True)
        if config.raw.get("review", {}).get("approved") is not False:
            raise OnboardingError(409, "Draft configuration must remain unapproved.")
        return config

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
                    "editable": record.get("approved_at") is None and NESTED_REASON not in reasons,
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
            "final_approved": record.get("approved_at") is not None,
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
                "approved_at",
                "approved_by",
                "git_commit_sha",
                "dag_id",
                "first_run_correlation_id",
                "airflow_run_id",
                "etl_run_id",
                "completed_at",
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

    @staticmethod
    def _require_editable(record: dict[str, Any]) -> None:
        if record.get("approved_at") is not None or record["status"] in {
            "APPROVING",
            "APPROVED",
            "ACTIVATING",
            "WAITING_FOR_DAG",
            "ACTIVATION_FAILED",
            "READY_FOR_FIRST_RUN",
            "TRIGGERING",
            "QUEUED",
            "RUNNING",
            "SUCCEEDED",
            "FIRST_RUN_FAILED",
        }:
            raise OnboardingError(409, "Approved configuration is read-only.")

    def _stable_source_key(self, record: dict[str, Any]) -> str:
        extension = Path(record["original_filename"]).suffix.lower()
        return PurePosixPath(record["proposed_dataset_name"], f"source{extension}").as_posix()

    def _activation_ready(self, record: dict[str, Any], draft: dict[str, Any]) -> bool:
        return draft.get("source", {}).get("path") == self._stable_source_config_path(
            record
        ) and draft.get("orchestration") == {
            "enabled": True,
            "schedule": None,
            "retries": 0,
            "retry_delay_minutes": 5,
        }

    def _stable_source_config_path(self, record: dict[str, Any]) -> str:
        root = PurePosixPath(self.settings.approved_source_config_root)
        if root.is_absolute() or ".." in root.parts:
            raise OnboardingError(409, "Approved source config root must be a safe relative path.")
        return (root / self._stable_source_key(record)).as_posix()

    def _approved_source_path(self, record: dict[str, Any]) -> Path:
        root = self.settings.approved_source_root.resolve()
        path = (root / Path(self._stable_source_key(record))).resolve()
        if not path.is_relative_to(root):
            raise OnboardingError(409, "Invalid approved source destination.")
        return path

    def _approved_config_path(self, record: dict[str, Any]) -> Path:
        root = self.settings.config_dir.resolve()
        path = (root / f"{record['proposed_dataset_name']}.yaml").resolve()
        if path.parent != root:
            raise OnboardingError(409, "Invalid approved configuration destination.")
        return path

    def _archive_draft(self, draft_path: Path, onboarding_id: str) -> None:
        archive_root = (self.settings.draft_config_dir / "archive").resolve()
        archive_root.mkdir(parents=True, exist_ok=True)
        destination = archive_root / f"{onboarding_id}.yaml"
        draft_path.replace(destination)

    @staticmethod
    def _approval_metadata(record: dict[str, Any]) -> dict[str, Any]:
        return {
            key: record.get(key)
            for key in (
                "approved_at",
                "approved_by",
                "validation_hash",
                "approved_config_hash",
                "git_commit_sha",
                "git_push_status",
                "dag_id",
                "activation_checked_at",
            )
        }

    def _completion_model(
        self, record: dict[str, Any], etl_run: dict[str, Any] | None
    ) -> dict[str, Any]:
        run = (
            {
                key: etl_run.get(key)
                for key in (
                    "run_id",
                    "status",
                    "started_at",
                    "completed_at",
                    "duration_seconds",
                    "rows_extracted",
                    "rows_transformed",
                    "rows_contract_passed",
                    "rows_quarantined",
                    "rows_loaded",
                    "drift_status",
                )
            }
            if etl_run
            else None
        )
        return {
            "onboarding_id": record["onboarding_id"],
            "dataset": record["proposed_dataset_name"],
            "status": record["status"],
            "safe_error": record.get("safe_error"),
            "approval": self._approval_metadata(record),
            "first_run": {
                "attempt": record.get("first_run_attempt") or 0,
                "correlation_id": record.get("first_run_correlation_id"),
                "airflow_dag_id": record.get("dag_id"),
                "airflow_run_id": record.get("airflow_run_id"),
                "airflow_state": record.get("airflow_state"),
                "etl_run_id": record.get("etl_run_id"),
                "completed_at": record.get("completed_at"),
                "etl_run": run,
            },
        }

    def _record(self, onboarding_id: str) -> dict[str, Any]:
        self._ensure_schema()
        record = self.repository.get(onboarding_id)
        if record is None:
            raise OnboardingError(404, "Onboarding session not found.")
        return record

    def _ensure_schema(self) -> None:
        if not self._schema_ready:
            self.repository.ensure_schema()
            self._schema_ready = True

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
        if record.get("approved_at") is not None and record.get("approved_config_key"):
            root = self.settings.config_dir.resolve()
            path = (root / Path(record["approved_config_key"])).resolve()
            if path.parent != root or not path.is_file():
                raise OnboardingError(409, "Approved configuration is unavailable.")
            return path
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
        self,
        path: Path,
        value: dict[str, Any],
        *,
        replace_existing: bool,
        runtime_record: dict[str, Any] | None = None,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and not replace_existing:
            raise OnboardingError(409, "Draft configuration already exists.")
        temporary = path.with_suffix(".yaml.part")
        try:
            temporary.write_text(
                yaml.safe_dump(value, sort_keys=False, allow_unicode=True), encoding="utf-8"
            )
            config = self._validate_draft(temporary)
            if runtime_record is not None:
                validate_plan(
                    config,
                    source_override=self._landing_path(runtime_record["landing_key"]),
                )
            temporary.replace(path)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
