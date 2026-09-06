"""Dataset onboarding through profiling and starter-config generation."""

from metadata_etl.onboarding.config_generator import (
    generate_starter_config,
    render_starter_yaml,
    write_starter_config,
)
from metadata_etl.onboarding.profiler import DatasetProfile, profile_csv

__all__ = [
    "DatasetProfile",
    "generate_starter_config",
    "profile_csv",
    "render_starter_yaml",
    "write_starter_config",
]
