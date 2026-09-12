from __future__ import annotations

from typing import Annotated, Literal

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from metadata_etl_api.models import (
    ApprovalRequest,
    ColumnPrivacyDecision,
    DatasetSummary,
    DatasetUploadCapability,
    DatasetWatermarkResponse,
    DraftYAML,
    FirstRunRequest,
    HealthResponse,
    KeyCandidateDecision,
    MoveDirection,
    OnboardingCapability,
    OnboardingCompletion,
    OnboardingConfiguration,
    OnboardingReview,
    OnboardingSession,
    OverviewResponse,
    PipelineSummary,
    QualityOverview,
    QualitySummary,
    RunDetail,
    SchemaDecision,
    SchemaDriftEvent,
    TrustedDataPreview,
    UploadOperation,
    WatermarkState,
)
from metadata_etl_api.onboarding_operations import OnboardingRepositoryError
from metadata_etl_api.onboarding_service import OnboardingError, OnboardingService
from metadata_etl_api.operations import OperationsError
from metadata_etl_api.repository import ObservabilityRepository, RepositoryError
from metadata_etl_api.settings import APISettings
from metadata_etl_api.trusted_data import (
    DEFAULT_PREVIEW_LIMIT,
    MAX_PREVIEW_LIMIT,
    TrustedDataError,
    TrustedDataService,
)
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


def _onboarding_service(request: Request) -> OnboardingService:
    configured = getattr(request.app.state, "onboarding_service", None)
    if configured is not None:
        return configured
    return OnboardingService.from_settings(request.app.state.settings)


Onboarding = Annotated[OnboardingService, Depends(_onboarding_service)]


def _trusted_data_service(request: Request) -> TrustedDataService:
    configured = getattr(request.app.state, "trusted_data_service", None)
    if configured is not None:
        return configured
    return TrustedDataService.from_settings(request.app.state.settings)


TrustedData = Annotated[TrustedDataService, Depends(_trusted_data_service)]
RunStatusFilter = Literal["SUCCEEDED", "FAILED", "RUNNING"]
LoadStrategyFilter = Literal["full", "incremental", "upsert", "scd2"]


def create_app() -> FastAPI:
    settings = APISettings.from_environment()
    application = FastAPI(
        title="ETL Control Center API",
        description="ETL monitoring plus gated data operations and onboarding review.",
        version="10.2.2",
    )
    application.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.allowed_origins),
        allow_credentials=False,
        allow_methods=["GET", "POST", "PATCH", "DELETE"],
        allow_headers=["Accept", "Content-Type"],
    )
    application.state.settings = settings
    application.state.upload_service = UploadService.from_settings(settings)
    application.state.onboarding_service = OnboardingService.from_settings(settings)
    application.state.trusted_data_service = TrustedDataService.from_settings(settings)

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

    @application.exception_handler(OnboardingError)
    async def onboarding_error_handler(_request: Request, exc: OnboardingError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @application.exception_handler(OnboardingRepositoryError)
    async def onboarding_repository_error_handler(
        _request: Request, _exc: OnboardingRepositoryError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"detail": "Onboarding state is temporarily unavailable."},
        )

    @application.exception_handler(TrustedDataError)
    async def trusted_data_error_handler(_request: Request, exc: TrustedDataError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

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
        "/api/datasets/{dataset}/trusted-data",
        response_model=TrustedDataPreview,
        tags=["datasets"],
    )
    def trusted_data_preview(
        dataset: str,
        trusted_data: TrustedData,
        limit: Annotated[int, Query(ge=1, le=MAX_PREVIEW_LIMIT)] = DEFAULT_PREVIEW_LIMIT,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict[str, object]:
        return trusted_data.preview(dataset, limit=limit, offset=offset)

    @application.get(
        "/api/datasets/{dataset}/pipeline-summary",
        response_model=PipelineSummary,
        tags=["datasets"],
    )
    def pipeline_summary(dataset: str, trusted_data: TrustedData) -> dict[str, object]:
        return trusted_data.pipeline_summary(dataset)

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
        "/api/onboarding/capability",
        response_model=OnboardingCapability,
        tags=["onboarding"],
    )
    def onboarding_capability(onboarding: Onboarding) -> dict[str, object]:
        return onboarding.capability()

    @application.post(
        "/api/onboarding",
        response_model=OnboardingSession,
        status_code=201,
        tags=["onboarding"],
    )
    async def create_onboarding(
        onboarding: Onboarding,
        dataset_name: Annotated[str, Form(...)],
        source_type: Annotated[str, Form(...)],
        file: Annotated[UploadFile, File(...)],
    ) -> dict[str, object]:
        return await onboarding.create(dataset_name, source_type, file)

    @application.post(
        "/api/onboarding/{onboarding_id}/profile",
        response_model=OnboardingSession,
        tags=["onboarding"],
    )
    def profile_onboarding(onboarding_id: str, onboarding: Onboarding) -> dict[str, object]:
        return onboarding.profile(onboarding_id)

    @application.get(
        "/api/onboarding/{onboarding_id}",
        response_model=OnboardingSession,
        tags=["onboarding"],
    )
    def onboarding_status(onboarding_id: str, onboarding: Onboarding) -> dict[str, object]:
        return onboarding.get(onboarding_id)

    @application.get(
        "/api/onboarding/{onboarding_id}/review",
        response_model=OnboardingReview,
        tags=["onboarding"],
    )
    def onboarding_review(onboarding_id: str, onboarding: Onboarding) -> dict[str, object]:
        return onboarding.review(onboarding_id)

    @application.patch(
        "/api/onboarding/{onboarding_id}/schema/{field}",
        response_model=OnboardingReview,
        tags=["onboarding"],
    )
    def update_onboarding_schema(
        onboarding_id: str,
        field: str,
        decision: SchemaDecision,
        onboarding: Onboarding,
    ) -> dict[str, object]:
        return onboarding.update_schema(onboarding_id, field, decision.model_dump())

    @application.post(
        "/api/onboarding/{onboarding_id}/key-decisions",
        response_model=OnboardingReview,
        tags=["onboarding"],
    )
    def update_onboarding_key(
        onboarding_id: str,
        decision: KeyCandidateDecision,
        onboarding: Onboarding,
    ) -> dict[str, object]:
        return onboarding.update_key_decision(onboarding_id, decision.field, decision.decision)

    @application.get(
        "/api/onboarding/{onboarding_id}/yaml",
        response_model=DraftYAML,
        tags=["onboarding"],
    )
    def onboarding_yaml(onboarding_id: str, onboarding: Onboarding) -> dict[str, str]:
        return onboarding.yaml_content(onboarding_id)

    @application.get(
        "/api/onboarding/{onboarding_id}/configuration",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def onboarding_configuration(onboarding_id: str, onboarding: Onboarding) -> dict[str, object]:
        return onboarding.configuration(onboarding_id)

    @application.patch(
        "/api/onboarding/{onboarding_id}/normalization",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def update_onboarding_normalization(
        onboarding_id: str, value: dict[str, object], onboarding: Onboarding
    ) -> dict[str, object]:
        return onboarding.update_normalization(onboarding_id, value)

    @application.post(
        "/api/onboarding/{onboarding_id}/transformations",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def add_onboarding_transformation(
        onboarding_id: str, value: dict[str, object], onboarding: Onboarding
    ) -> dict[str, object]:
        return onboarding.add_transformation(onboarding_id, value)

    @application.patch(
        "/api/onboarding/{onboarding_id}/transformations/{item_id}",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def edit_onboarding_transformation(
        onboarding_id: str,
        item_id: str,
        value: dict[str, object],
        onboarding: Onboarding,
    ) -> dict[str, object]:
        return onboarding.edit_transformation(onboarding_id, item_id, value)

    @application.delete(
        "/api/onboarding/{onboarding_id}/transformations/{item_id}",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def delete_onboarding_transformation(
        onboarding_id: str, item_id: str, onboarding: Onboarding
    ) -> dict[str, object]:
        return onboarding.delete_transformation(onboarding_id, item_id)

    @application.post(
        "/api/onboarding/{onboarding_id}/transformations/{item_id}/move",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def move_onboarding_transformation(
        onboarding_id: str,
        item_id: str,
        move: MoveDirection,
        onboarding: Onboarding,
    ) -> dict[str, object]:
        return onboarding.move_transformation(onboarding_id, item_id, move.direction)

    @application.post(
        "/api/onboarding/{onboarding_id}/contracts",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def add_onboarding_contract(
        onboarding_id: str, value: dict[str, object], onboarding: Onboarding
    ) -> dict[str, object]:
        return onboarding.add_contract(onboarding_id, value)

    @application.patch(
        "/api/onboarding/{onboarding_id}/contracts/{item_id}",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def edit_onboarding_contract(
        onboarding_id: str,
        item_id: str,
        value: dict[str, object],
        onboarding: Onboarding,
    ) -> dict[str, object]:
        return onboarding.edit_contract(onboarding_id, item_id, value)

    @application.delete(
        "/api/onboarding/{onboarding_id}/contracts/{item_id}",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def delete_onboarding_contract(
        onboarding_id: str, item_id: str, onboarding: Onboarding
    ) -> dict[str, object]:
        return onboarding.delete_contract(onboarding_id, item_id)

    @application.patch(
        "/api/onboarding/{onboarding_id}/columns/{field}/privacy",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def update_onboarding_privacy(
        onboarding_id: str,
        field: str,
        decision: ColumnPrivacyDecision,
        onboarding: Onboarding,
    ) -> dict[str, object]:
        return onboarding.update_column_privacy(onboarding_id, field, decision.model_dump())

    @application.patch(
        "/api/onboarding/{onboarding_id}/load",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def update_onboarding_load(
        onboarding_id: str, value: dict[str, object], onboarding: Onboarding
    ) -> dict[str, object]:
        return onboarding.update_load(onboarding_id, value)

    @application.patch(
        "/api/onboarding/{onboarding_id}/schema-drift",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def update_onboarding_drift(
        onboarding_id: str, value: dict[str, str], onboarding: Onboarding
    ) -> dict[str, object]:
        return onboarding.update_schema_drift(onboarding_id, value)

    @application.post(
        "/api/onboarding/{onboarding_id}/validate",
        response_model=OnboardingConfiguration,
        tags=["onboarding"],
    )
    def validate_onboarding_configuration(
        onboarding_id: str, onboarding: Onboarding
    ) -> dict[str, object]:
        return onboarding.validate_configuration(onboarding_id)

    @application.post(
        "/api/onboarding/{onboarding_id}/approve",
        response_model=OnboardingCompletion,
        tags=["onboarding"],
    )
    def approve_onboarding(
        onboarding_id: str, value: ApprovalRequest, onboarding: Onboarding
    ) -> dict[str, object]:
        return onboarding.approve(
            onboarding_id,
            expected_hash=value.expected_hash,
            approved_by=value.approved_by,
            acknowledged=value.acknowledged,
        )

    @application.post(
        "/api/onboarding/{onboarding_id}/activate",
        response_model=OnboardingCompletion,
        tags=["onboarding"],
    )
    def activate_onboarding(onboarding_id: str, onboarding: Onboarding) -> dict[str, object]:
        return onboarding.activate(onboarding_id)

    @application.post(
        "/api/onboarding/{onboarding_id}/first-run",
        response_model=OnboardingCompletion,
        tags=["onboarding"],
    )
    def first_onboarding_run(
        onboarding_id: str, value: FirstRunRequest, onboarding: Onboarding
    ) -> dict[str, object]:
        return onboarding.run_first(onboarding_id, retry=value.retry)

    @application.get(
        "/api/onboarding/{onboarding_id}/completion",
        response_model=OnboardingCompletion,
        tags=["onboarding"],
    )
    def onboarding_completion(onboarding_id: str, onboarding: Onboarding) -> dict[str, object]:
        return onboarding.completion(onboarding_id)

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
