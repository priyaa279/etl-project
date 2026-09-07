from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from metadata_etl.config import ETLConfig, load_config
from metadata_etl.errors import ConfigError


class CatalogError(RuntimeError):
    """Raised when an approved dataset configuration cannot be selected safely."""


@dataclass(frozen=True)
class DatasetCapability:
    dataset: str
    source_type: str
    load_strategy: str
    config_approved: bool
    upload_eligible: bool
    reason: str | None


class DatasetCatalog:
    def __init__(self, config_dir: Path) -> None:
        self.config_dir = config_dir.resolve()

    def get(self, dataset: str) -> ETLConfig:
        matches: list[ETLConfig] = []
        for path in sorted(self.config_dir.glob("*.yaml")):
            try:
                config = load_config(path, require_source=False)
            except ConfigError:
                continue
            if config.dataset == dataset:
                matches.append(config)
        if not matches:
            raise CatalogError("Dataset does not have an approved configuration.")
        orchestrated = [config for config in matches if config.orchestration.enabled]
        if len(orchestrated) == 1:
            return orchestrated[0]
        if len(matches) == 1:
            return matches[0]
        raise CatalogError("Dataset configuration is ambiguous.")

    def capability(self, dataset: str) -> DatasetCapability:
        config = self.get(dataset)
        if config.source_type == "postgres":
            reason = "This dataset reads from PostgreSQL and does not accept file uploads."
            eligible = False
        elif not config.orchestration.enabled:
            reason = "Control Center execution is not enabled for this dataset."
            eligible = False
        else:
            reason = None
            eligible = True
        return DatasetCapability(
            dataset=config.dataset,
            source_type=config.source_type,
            load_strategy=config.load_strategy,
            config_approved=True,
            upload_eligible=eligible,
            reason=reason,
        )

    @staticmethod
    def dag_id(config: ETLConfig) -> str:
        if not config.orchestration.enabled:
            raise CatalogError("Control Center execution is not enabled for this dataset.")
        return f"etl_{config.dataset}"
