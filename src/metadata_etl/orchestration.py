from __future__ import annotations

import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from metadata_etl.config import ETLConfig, load_config
from metadata_etl.errors import ConfigError
from metadata_etl.structured_logging import emit_event
from metadata_etl.validation import validate_plan


@dataclass(frozen=True)
class ScheduledConfig:
    path: Path
    dataset: str
    dag_id: str
    schedule: str
    retries: int
    retry_delay_minutes: int


@dataclass(frozen=True)
class ConfigValidation:
    path: Path
    dataset: str
    mode: str


def _config_paths(config_dir: str | Path) -> tuple[Path, ...]:
    directory = Path(config_dir).resolve()
    if not directory.is_dir():
        raise ConfigError(f"Config directory does not exist: {directory}")
    paths = tuple(sorted(directory.glob("*.yaml")))
    if not paths:
        raise ConfigError(f"No runnable YAML configs found in {directory}")
    return paths


def discover_scheduled_configs(config_dir: str | Path) -> tuple[ScheduledConfig, ...]:
    """Discover approved, valid, explicitly enabled top-level dataset configs."""
    scheduled: list[ScheduledConfig] = []
    for path in _config_paths(config_dir):
        try:
            config = load_config(path)
        except ConfigError:
            emit_event(
                "ORCHESTRATION_CONFIG_SKIPPED",
                level=logging.WARNING,
                config_path=str(path),
                reason="invalid_or_unapproved",
            )
            continue
        orchestration = config.orchestration
        if not orchestration.enabled:
            continue
        assert orchestration.schedule is not None
        scheduled.append(
            ScheduledConfig(
                path=path,
                dataset=config.dataset,
                dag_id=f"etl_{config.dataset}",
                schedule=orchestration.schedule,
                retries=orchestration.retries,
                retry_delay_minutes=orchestration.retry_delay_minutes,
            )
        )
    dag_ids = [item.dag_id for item in scheduled]
    if len(set(dag_ids)) != len(dag_ids):
        raise ConfigError("Enabled orchestration configs must have unique dataset names")
    return tuple(scheduled)


def validate_all_configs(
    config_dir: str | Path, *, require_source_bindings: bool = False
) -> tuple[ConfigValidation, ...]:
    """Validate every top-level runnable config; drafts belong under configs/drafts."""
    validated: list[ConfigValidation] = []
    failures: list[str] = []
    for path in _config_paths(config_dir):
        try:
            config: ETLConfig = load_config(path)
            source_credentials_available = bool(
                config.source_connection_env and os.getenv(config.source_connection_env)
            )
            if config.source_type == "postgres" and not source_credentials_available:
                if require_source_bindings:
                    _ = config.source_dsn
                mode = "static"
            else:
                validate_plan(config)
                mode = "bound"
            validated.append(ConfigValidation(path, config.dataset, mode))
        except ConfigError as exc:
            failures.append(f"{path.name}: {exc}")
    if failures:
        raise ConfigError("Config validation failed: " + "; ".join(failures))
    return tuple(validated)


def run_cli_stage(
    stage: str,
    config_path: str | Path,
    source_override: str | None = None,
    correlation_id: str | None = None,
) -> None:
    """Run the same CLI entry point used outside Airflow and propagate its exit status."""
    if stage not in {"validate", "run"}:
        raise ValueError(f"Unsupported orchestration stage: {stage}")
    command = [sys.executable, "-m", "metadata_etl.cli", stage, str(Path(config_path).resolve())]
    if stage == "run" and source_override:
        command.extend(["--source-override", source_override])
    if stage == "run" and correlation_id:
        command.extend(["--correlation-id", correlation_id])
    subprocess.run(command, check=True)
