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
        )
