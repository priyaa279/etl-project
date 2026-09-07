from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

from metadata_etl.cli import main
from metadata_etl.config import load_config
from metadata_etl.orchestration import (
    discover_scheduled_configs,
    run_cli_stage,
    validate_all_configs,
)

ROOT = Path(__file__).parents[1]


def _write_config(
    tmp_path: Path,
    *,
    dataset: str,
    enabled: bool,
    approved: bool = True,
) -> Path:
    config = yaml.safe_load((ROOT / "configs" / "customers.yaml").read_text(encoding="utf-8"))
    config["dataset"]["name"] = dataset
    config["review"]["approved"] = approved
    config["source"]["path"] = str(ROOT / "data" / "incoming" / "customers.csv")
    config["runtime"]["raw_root"] = str(tmp_path / "raw")
    config["load"]["staging_table"] = f"{dataset}_staging"
    config["load"]["target_table"] = dataset
    config["orchestration"] = {
        "enabled": enabled,
        "schedule": "0 4 * * *",
        "retries": 3,
        "retry_delay_minutes": 7,
    }
    path = tmp_path / f"{dataset}.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def test_orchestration_metadata_is_optional_and_parsed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    enabled = load_config(_write_config(tmp_path, dataset="enabled_data", enabled=True))
    config = yaml.safe_load((ROOT / "configs" / "customers.yaml").read_text(encoding="utf-8"))
    config.pop("orchestration")
    config["source"]["path"] = str(ROOT / "data" / "incoming" / "customers.csv")
    without_metadata = tmp_path / "local_only.yaml"
    without_metadata.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    local_only = load_config(without_metadata)

    assert enabled.orchestration.enabled is True
    assert enabled.orchestration.schedule == "0 4 * * *"
    assert enabled.orchestration.retries == 3
    assert enabled.orchestration.retry_delay_minutes == 7
    assert local_only.orchestration.enabled is False


def test_disabled_and_unapproved_configs_are_not_scheduled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    _write_config(tmp_path, dataset="disabled_data", enabled=False)
    _write_config(tmp_path, dataset="unapproved_data", enabled=True, approved=False)

    assert discover_scheduled_configs(tmp_path) == ()


def test_generic_discovery_finds_unrelated_enabled_configs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(ROOT)

    discovered = discover_scheduled_configs(ROOT / "configs")

    assert {item.dag_id for item in discovered} == {"etl_customers", "etl_sensor_readings"}
    assert all(item.retries == 2 for item in discovered)


def test_new_enabled_config_is_discovered_without_dag_code_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    _write_config(tmp_path, dataset="new_domain", enabled=True)

    discovered = discover_scheduled_configs(tmp_path)

    assert [item.dag_id for item in discovered] == ["etl_new_domain"]


def test_airflow_cli_runner_propagates_nonzero_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _write_config(tmp_path, dataset="failure_status", enabled=True)

    def fail(*args: object, **kwargs: object) -> None:
        raise subprocess.CalledProcessError(2, "etl")

    monkeypatch.setattr(subprocess, "run", fail)

    with pytest.raises(subprocess.CalledProcessError) as failure:
        run_cli_stage("run", config)

    assert failure.value.returncode == 2


def test_validate_all_checks_top_level_configs_and_ignores_drafts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    _write_config(tmp_path, dataset="runnable", enabled=False)
    drafts = tmp_path / "drafts"
    drafts.mkdir()
    (drafts / "unapproved.yaml").write_text("review:\n  approved: false\n", encoding="utf-8")

    results = validate_all_configs(tmp_path)

    assert [(item.dataset, item.mode) for item in results] == [("runnable", "bound")]


def test_validate_all_cli_returns_nonzero_for_unapproved_runnable_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    _write_config(tmp_path, dataset="unapproved", enabled=False, approved=False)

    assert main(["validate-all", str(tmp_path)]) == 2


def test_generic_dag_has_no_dataset_specific_implementation() -> None:
    dag_path = ROOT / "airflow" / "dags" / "generic_etl.py"
    source = dag_path.read_text(encoding="utf-8")

    compile(source, str(dag_path), "exec")
    assert "discover_scheduled_configs" in source
    assert "run_cli_stage" in source
    assert "customers" not in source
    assert "sensor_readings" not in source
