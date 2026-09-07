from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class APISettings:
    database_dsn: str | None
    allowed_origins: tuple[str, ...]

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
        return cls(os.getenv("ETL_POSTGRES_DSN"), origins)
