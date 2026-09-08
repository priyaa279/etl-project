from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _boolean(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class APISettings:
    database_dsn: str | None
    allowed_origins: tuple[str, ...]
    operations_enabled: bool
    config_dir: Path
    upload_root: Path
    airflow_upload_root: str
    upload_max_bytes: int
    airflow_api_url: str
    airflow_username: str | None
    airflow_password: str | None
    airflow_token: str | None
    onboarding_enabled: bool = False
    onboarding_root: Path = Path("data/onboarding")
    draft_config_dir: Path = Path("configs/drafts")
    repository_root: Path = Path(".")
    approved_source_root: Path = Path("data/sources")
    approved_source_config_root: str = "data/sources"
    airflow_source_root: str = "/opt/airflow/data/sources"
    airflow_discovery_timeout_seconds: float = 20.0
    airflow_discovery_poll_seconds: float = 1.0
    git_executable: str = "git"
    git_push_enabled: bool = False
    git_remote: str = "origin"
    git_branch: str = "main"

    @classmethod
    def from_environment(cls) -> APISettings:
        origins = tuple(
            origin.strip()
            for origin in os.getenv(
                "ETL_API_CORS_ORIGINS",
                "http://localhost:5173,http://localhost:4173",
            ).split(",")
            if origin.strip()
        )
        max_bytes_value = os.getenv("ETL_UPLOAD_MAX_BYTES", str(100 * 1024 * 1024))
        try:
            max_bytes = int(max_bytes_value)
        except ValueError:
            max_bytes = 100 * 1024 * 1024
        return cls(
            database_dsn=os.getenv("ETL_POSTGRES_DSN"),
            allowed_origins=origins,
            operations_enabled=_boolean("ETL_CONTROL_OPERATIONS_ENABLED"),
            config_dir=Path(os.getenv("ETL_CONFIG_DIR", "configs")),
            upload_root=Path(os.getenv("ETL_UPLOAD_ROOT", "data/uploads")),
            airflow_upload_root=os.getenv("ETL_AIRFLOW_UPLOAD_ROOT", "/opt/airflow/data/uploads"),
            upload_max_bytes=max(1, max_bytes),
            airflow_api_url=os.getenv("AIRFLOW_API_BASE_URL", "http://localhost:8080"),
            airflow_username=os.getenv("AIRFLOW_API_USERNAME"),
            airflow_password=os.getenv("AIRFLOW_API_PASSWORD"),
            airflow_token=os.getenv("AIRFLOW_API_TOKEN"),
            onboarding_enabled=_boolean("ETL_CONTROL_ONBOARDING_ENABLED"),
            onboarding_root=Path(os.getenv("ETL_ONBOARDING_ROOT", "data/onboarding")),
            draft_config_dir=Path(os.getenv("ETL_DRAFT_CONFIG_DIR", "configs/drafts")),
            repository_root=Path(os.getenv("ETL_REPOSITORY_ROOT", ".")),
            approved_source_root=Path(os.getenv("ETL_APPROVED_SOURCE_ROOT", "data/sources")),
            approved_source_config_root=os.getenv(
                "ETL_APPROVED_SOURCE_CONFIG_ROOT", "data/sources"
            ).strip("/"),
            airflow_source_root=os.getenv("ETL_AIRFLOW_SOURCE_ROOT", "/opt/airflow/data/sources"),
            airflow_discovery_timeout_seconds=float(
                os.getenv("ETL_AIRFLOW_DISCOVERY_TIMEOUT_SECONDS", "20")
            ),
            airflow_discovery_poll_seconds=float(
                os.getenv("ETL_AIRFLOW_DISCOVERY_POLL_SECONDS", "1")
            ),
            git_executable=os.getenv("ETL_GIT_EXECUTABLE", "git"),
            git_push_enabled=_boolean("ETL_CONTROL_GIT_PUSH_ENABLED"),
            git_remote=os.getenv("ETL_GIT_REMOTE", "origin"),
            git_branch=os.getenv("ETL_GIT_BRANCH", "main"),
        )
