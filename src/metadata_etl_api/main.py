from __future__ import annotations

from typing import Annotated, Literal

from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from metadata_etl_api.models import (
    DatasetSummary,
    DatasetUploadCapability,
    DatasetWatermarkResponse,
    HealthResponse,
    OverviewResponse,
    QualityOverview,
    QualitySummary,
    RunDetail,
    SchemaDriftEvent,
    UploadOperation,
    WatermarkState,
)
from metadata_etl_api.operations import OperationsError
from metadata_etl_api.repository import ObservabilityRepository, RepositoryError
from metadata_etl_api.settings import APISettings
from metadata_etl_api.upload_service import UploadOperationError, UploadService


def _repository(request: Request) -> ObservabilityRepository:
    configured = getattr(request.app.state, "repository", None)
    if configured is not None:
        return configured
    settings = APISettings.from_environment()
    return ObservabilityRepository(settings.database_dsn)


Repository = Annotated[ObservabilityRepository, Depends(_repository)]


def _upload_service(request: Request) -> UploadService:
    configured = getattr(request.app.state, "upload_service", None)
    if configured is not None:
        return configured
    return UploadService.from_settings(request.app.state.settings)


Uploads = Annotated[UploadService, Depends(_upload_service)]
RunStatusFilter = Literal["SUCCEEDED", "FAILED", "RUNNING"]
LoadStrategyFilter = Literal["full", "incremental", "upsert", "scd2"]


def create_app() -> FastAPI:
    settings = APISettings.from_environment()
    application = FastAPI(
        title="ETL Control Center API",
        description="ETL monitoring plus gated existing-dataset upload operations.",
        version="10.1.0",
    )
    application.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.allowed_origins),
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["Accept", "Content-Type"],
    )
    application.state.settings = settings
    application.state.upload_service = UploadService.from_settings(settings)

    @application.exception_handler(RepositoryError)
    async def repository_error_handler(_request: Request, _exc: RepositoryError) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"detail": "Observability data is temporarily unavailable."},
        )

    @application.exception_handler(UploadOperationError)
    async def upload_error_handler(_request: Request, exc: UploadOperationError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @application.exception_handler(OperationsError)
    async def operations_error_handler(_request: Request, _exc: OperationsError) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"detail": "Operational state is temporarily unavailable."},
        )

    @application.get("/api/health", response_model=HealthResponse, tags=["system"])
    def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @application.get("/api/overview", response_model=OverviewResponse, tags=["overview"])
    def overview(repository: Repository) -> dict[str, object]:
        return repository.overview(hours=24)

    @application.get("/api/datasets", response_model=list[DatasetSummary], tags=["datasets"])
    def datasets(repository: Repository) -> list[dict[str, object]]:
        return repository.datasets()

    @application.get("/api/datasets/{dataset}", response_model=DatasetSummary, tags=["datasets"])
    def dataset_detail(dataset: str, repository: Repository) -> dict[str, object]:
        result = repository.dataset(dataset)
        if result is None:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        return result

    @application.get(
        "/api/datasets/{dataset}/upload-capability",
        response_model=DatasetUploadCapability,
        tags=["uploads"],
    )
    def dataset_upload_capability(dataset: str, uploads: Uploads) -> dict[str, object]:
        return uploads.capability(dataset)

    @application.post(
        "/api/datasets/{dataset}/uploads",
        response_model=UploadOperation,
        status_code=201,
        tags=["uploads"],
    )
    async def create_upload(
        dataset: str,
        uploads: Uploads,
        file: Annotated[UploadFile, File(...)],
    ) -> dict[str, object]:
        return await uploads.create_upload(dataset, file)

    @application.post(
        "/api/uploads/{upload_id}/validate",
        response_model=UploadOperation,
        tags=["uploads"],
    )
    def validate_upload(upload_id: str, uploads: Uploads) -> dict[str, object]:
        return uploads.validate(upload_id)

    @application.post(
        "/api/uploads/{upload_id}/run",
        response_model=UploadOperation,
        tags=["uploads"],
    )
    def run_upload(upload_id: str, uploads: Uploads) -> dict[str, object]:
        return uploads.run(upload_id)

    @application.get(
        "/api/uploads/{upload_id}",
        response_model=UploadOperation,
        tags=["uploads"],
    )
    def upload_status(upload_id: str, uploads: Uploads) -> dict[str, object]:
        return uploads.get(upload_id)

    @application.get(
        "/api/datasets/{dataset}/runs", response_model=list[RunDetail], tags=["datasets"]
    )
    def dataset_runs(
        dataset: str,
        repository: Repository,
        limit: Annotated[int, Query(ge=1, le=200)] = 20,
    ) -> list[dict[str, object]]:
        if repository.dataset(dataset) is None:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        return repository.runs(limit=limit, dataset=dataset)

    @application.get(
        "/api/datasets/{dataset}/quality",
        response_model=list[QualitySummary],
        tags=["quality"],
    )
    def dataset_quality(
        dataset: str,
        repository: Repository,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> list[dict[str, object]]:
        if repository.dataset(dataset) is None:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        return repository.dataset_quality(dataset, limit=limit)

    @application.get(
        "/api/datasets/{dataset}/schema-drift",
        response_model=list[SchemaDriftEvent],
        tags=["schema drift"],
    )
    def dataset_schema_drift(
        dataset: str,
        repository: Repository,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> list[dict[str, object]]:
        if repository.dataset(dataset) is None:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        return repository.schema_drift(limit=limit, dataset=dataset)

    @application.get(
        "/api/datasets/{dataset}/watermark",
        response_model=DatasetWatermarkResponse,
        tags=["watermarks"],
    )
    def dataset_watermark(dataset: str, repository: Repository) -> dict[str, object]:
        if repository.dataset(dataset) is None:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        return {"dataset": dataset, "watermark": repository.watermark(dataset)}

    @application.get("/api/runs", response_model=list[RunDetail], tags=["runs"])
    def runs(
        repository: Repository,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        dataset: str | None = None,
        status: RunStatusFilter | None = None,
        load_strategy: LoadStrategyFilter | None = None,
    ) -> list[dict[str, object]]:
        return repository.runs(
            limit=limit,
            dataset=dataset,
            status=status,
            load_strategy=load_strategy,
        )

    @application.get("/api/runs/{run_id}", response_model=RunDetail, tags=["runs"])
    def run_detail(run_id: str, repository: Repository) -> dict[str, object]:
        result = repository.run(run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Run not found.")
        return result

    @application.get(
        "/api/runs/{run_id}/quality",
        response_model=list[QualitySummary],
        tags=["quality"],
    )
    def run_quality(run_id: str, repository: Repository) -> list[dict[str, object]]:
        if repository.run(run_id) is None:
            raise HTTPException(status_code=404, detail="Run not found.")
        return repository.run_quality(run_id)

    @application.get("/api/quality", response_model=QualityOverview, tags=["quality"])
    def quality_overview(
        repository: Repository,
        days: Annotated[int, Query(ge=1, le=365)] = 30,
    ) -> dict[str, object]:
        return repository.quality_overview(days=days)

    @application.get(
        "/api/schema-drift", response_model=list[SchemaDriftEvent], tags=["schema drift"]
    )
    def schema_drift(
        repository: Repository,
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
        dataset: str | None = None,
    ) -> list[dict[str, object]]:
        return repository.schema_drift(limit=limit, dataset=dataset)

    @application.get("/api/watermarks", response_model=list[WatermarkState], tags=["watermarks"])
    def watermarks(repository: Repository) -> list[dict[str, object]]:
        return repository.watermarks()

    return application


app = create_app()
