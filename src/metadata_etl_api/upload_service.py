from __future__ import annotations

import hashlib
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from fastapi import UploadFile

from metadata_etl.errors import ETLError
from metadata_etl.schema import SchemaFingerprint
from metadata_etl.validation import preflight_source
from metadata_etl_api.airflow_client import AirflowClient, AirflowError
from metadata_etl_api.catalog import CatalogError, DatasetCatalog
from metadata_etl_api.operations import OperationsError, UploadRepository
from metadata_etl_api.repository import ObservabilityRepository
from metadata_etl_api.settings import APISettings


class UploadOperationError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


EXTENSIONS = {
    "csv": {".csv"},
    "json": {".json", ".jsonl", ".ndjson"},
    "parquet": {".parquet"},
}


class UploadService:
    def __init__(
        self,
        settings: APISettings,
        operations: UploadRepository,
        observability: ObservabilityRepository,
        catalog: DatasetCatalog,
        airflow: AirflowClient,
    ) -> None:
        self.settings = settings
        self.operations = operations
        self.observability = observability
        self.catalog = catalog
        self.airflow = airflow

    @classmethod
    def from_settings(cls, settings: APISettings) -> UploadService:
        operations = UploadRepository(settings.database_dsn)
        return cls(
            settings,
            operations,
            ObservabilityRepository(settings.database_dsn),
            DatasetCatalog(settings.config_dir),
            AirflowClient(
                settings.airflow_api_url,
                username=settings.airflow_username,
                password=settings.airflow_password,
                token=settings.airflow_token,
                password_file=settings.airflow_password_file,
            ),
        )

    def _require_enabled(self) -> None:
        if not self.settings.operations_enabled:
            raise UploadOperationError(
                403,
                "Operational actions are disabled in this environment.",
            )

    def capability(self, dataset: str) -> dict[str, Any]:
        try:
            capability = self.catalog.capability(dataset).__dict__
        except CatalogError as exc:
            raise UploadOperationError(404, str(exc)) from exc
        if capability["upload_eligible"] and not self.settings.operations_enabled:
            capability = {
                **capability,
                "upload_eligible": False,
                "reason": "Operational upload and run actions are disabled.",
            }
        return capability

    async def create_upload(self, dataset: str, file: UploadFile) -> dict[str, Any]:
        self._require_enabled()
        try:
            capability = self.catalog.capability(dataset)
            config = self.catalog.get(dataset)
        except CatalogError as exc:
            raise UploadOperationError(404, str(exc)) from exc
        if not capability.upload_eligible:
            raise UploadOperationError(409, capability.reason or "File upload is not applicable.")

        original_filename = (file.filename or "upload").replace("\x00", "")[:512]
        extension = Path(original_filename.replace("\\", "/")).suffix.lower()
        if extension not in EXTENSIONS[config.source_type]:
            expected = ", ".join(sorted(EXTENSIONS[config.source_type]))
            raise UploadOperationError(
                415, f"Expected a {config.source_type.upper()} file ({expected})."
            )

        upload_id = str(uuid.uuid4())
        landing_key = PurePosixPath(dataset, upload_id, f"artifact{extension}").as_posix()
        root = self.settings.upload_root.resolve()
        destination = (root / Path(landing_key)).resolve()
        if not destination.is_relative_to(root):
            raise UploadOperationError(400, "Invalid upload destination.")
        destination.parent.mkdir(parents=True, exist_ok=False)
        temporary = destination.with_suffix(destination.suffix + ".part")
        digest = hashlib.sha256()
        size = 0
        try:
            with temporary.open("xb") as handle:
                while chunk := await file.read(1024 * 1024):
                    size += len(chunk)
                    if size > self.settings.upload_max_bytes:
                        raise UploadOperationError(
                            413, "Uploaded file exceeds the configured limit."
                        )
                    digest.update(chunk)
                    handle.write(chunk)
            temporary.replace(destination)
            self.operations.ensure_schema()
            record = self.operations.create(
                {
                    "upload_id": upload_id,
                    "correlation_id": upload_id,
                    "dataset": dataset,
                    "original_filename": original_filename,
                    "source_type": config.source_type,
                    "size_bytes": size,
                    "sha256": digest.hexdigest(),
                    "landing_key": landing_key,
                    "uploaded_at": datetime.now(UTC),
                }
            )
        except Exception:
            temporary.unlink(missing_ok=True)
            destination.unlink(missing_ok=True)
            shutil.rmtree(destination.parent, ignore_errors=True)
            raise
        finally:
            await file.close()
        return self._public(record)

    def validate(self, upload_id: str) -> dict[str, Any]:
        self._require_enabled()
        record = self._record(upload_id)
        if record["status"] in {"TRIGGERING", "QUEUED", "RUNNING", "SUCCEEDED"}:
            return self.get(upload_id)
        try:
            config = self.catalog.get(record["dataset"])
            if config.source_type != record["source_type"]:
                raise ETLError("Uploaded source type does not match the approved configuration")
            source = self._landing_path(record["landing_key"])
            old_raw_json, old_canonical_json = self.observability.schema_baseline(config.dataset)
            previous_raw = SchemaFingerprint.from_json(old_raw_json) if old_raw_json else None
            previous_canonical = (
                SchemaFingerprint.from_json(old_canonical_json) if old_canonical_json else None
            )
            preview = preflight_source(
                config,
                source,
                previous_raw=previous_raw,
                previous_canonical=previous_canonical,
            )
            status = (
                "BLOCKED"
                if preview.drift.failed or not preview.canonical_compatible
                else "WARNING"
                if preview.drift.status == "WARN"
                else "READY"
            )
            result = {
                "status": status,
                "config_approved": True,
                "source_valid": True,
                "canonical_compatible": preview.canonical_compatible,
                "schema": {
                    "raw_hash": preview.raw_schema.hash,
                    "canonical_hash": preview.canonical_schema.hash,
                    "fields": [
                        {"name": field.name, "datatype": field.datatype}
                        for field in preview.raw_schema.fields
                    ],
                },
                "drift": {
                    "detected": bool(preview.drift.events),
                    "status": preview.drift.status,
                    "events": [
                        {
                            "type": event.drift_type,
                            "description": event.description,
                            "policy": event.policy,
                            "action": event.action_taken,
                        }
                        for event in preview.drift.events
                    ],
                },
            }
        except (CatalogError, ETLError, OSError, ValueError) as exc:
            status = "BLOCKED"
            result = {
                "status": status,
                "config_approved": not isinstance(exc, CatalogError),
                "source_valid": False,
                "canonical_compatible": False,
                "message": "The uploaded file could not be parsed or does not match the approved structure.",
                "drift": {"detected": False, "status": "UNKNOWN", "events": []},
            }
        updated = self.operations.set_preflight(upload_id, status, result, datetime.now(UTC))
        return self._public(updated)

    def run(self, upload_id: str) -> dict[str, Any]:
        self._require_enabled()
        record = self._record(upload_id)
        try:
            config = self.catalog.get(record["dataset"])
            dag_id = self.catalog.dag_id(config)
        except CatalogError as exc:
            raise UploadOperationError(409, str(exc)) from exc
        airflow_run_id = f"upload__{upload_id}"
        try:
            claimed, should_trigger = self.operations.claim_trigger(
                upload_id, dag_id, airflow_run_id, datetime.now(UTC)
            )
        except OperationsError as exc:
            raise UploadOperationError(409, str(exc)) from exc
        if not should_trigger:
            return self.get(upload_id)

        source_override = (
            PurePosixPath(self.settings.airflow_upload_root) / claimed["landing_key"]
        ).as_posix()
        try:
            response = self.airflow.trigger(
                dag_id,
                airflow_run_id,
                source_override=source_override,
                correlation_id=claimed["correlation_id"],
            )
            airflow_state = str(response.get("state") or "queued").upper()
            updated = self.operations.mark_triggered(upload_id, airflow_run_id, airflow_state)
        except AirflowError as exc:
            self.operations.mark_failed(
                upload_id,
                "Airflow could not accept the run request.",
                datetime.now(UTC),
            )
            raise UploadOperationError(502, "Airflow could not accept the run request.") from exc
        return self._public(updated)

    def get(self, upload_id: str) -> dict[str, Any]:
        record = self._record(upload_id)
        etl_run = self.observability.run_by_correlation(record["correlation_id"])
        airflow_state = record.get("airflow_state")
        if record.get("airflow_dag_id") and record.get("airflow_run_id"):
            try:
                airflow = self.airflow.run(record["airflow_dag_id"], record["airflow_run_id"])
                airflow_state = str(airflow.get("state") or airflow_state or "queued").upper()
            except AirflowError:
                airflow_state = airflow_state or "UNKNOWN"

        status = record["status"]
        completed_at = record.get("completed_at")
        if etl_run is not None:
            status = etl_run["status"]
            completed_at = etl_run.get("completed_at")
        elif airflow_state in {"RUNNING"}:
            status = "RUNNING"
        elif airflow_state in {"FAILED", "UPSTREAM_FAILED"}:
            status = "FAILED"
            completed_at = completed_at or datetime.now(UTC)
        elif airflow_state == "SUCCESS":
            status = "FAILED"
            completed_at = completed_at or datetime.now(UTC)
            if not record.get("safe_error"):
                record = self.operations.mark_failed(
                    upload_id,
                    "Airflow completed without a correlated ETL result.",
                    completed_at,
                )
        elif airflow_state in {"QUEUED", "SCHEDULED"}:
            status = "QUEUED"
        if (
            status != record["status"]
            or airflow_state != record.get("airflow_state")
            or (etl_run and record.get("etl_run_id") != etl_run["run_id"])
        ):
            record = self.operations.sync(
                upload_id,
                status=status,
                airflow_state=airflow_state,
                etl_run_id=etl_run["run_id"] if etl_run else None,
                completed_at=completed_at,
            )
        return self._public(record, etl_run)

    def _record(self, upload_id: str) -> dict[str, Any]:
        record = self.operations.get(upload_id)
        if record is None:
            raise UploadOperationError(404, "Upload session not found.")
        return record

    def _landing_path(self, landing_key: str) -> Path:
        root = self.settings.upload_root.resolve()
        path = (root / Path(landing_key)).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise UploadOperationError(409, "Uploaded artifact is unavailable.")
        return path

    def _public(
        self, record: dict[str, Any], etl_run: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        result = {
            key: record.get(key)
            for key in (
                "upload_id",
                "dataset",
                "original_filename",
                "source_type",
                "size_bytes",
                "sha256",
                "status",
                "uploaded_at",
                "validated_at",
                "triggered_at",
                "airflow_dag_id",
                "airflow_run_id",
                "airflow_state",
                "etl_run_id",
                "completed_at",
                "safe_error",
                "preflight_result",
            )
        }
        result["etl_run"] = (
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
        return result
