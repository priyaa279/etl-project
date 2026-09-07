from pathlib import Path

import pytest

from metadata_etl.config import load_config
from metadata_etl.errors import ConfigError

ROOT = Path(__file__).parents[1]
CONFIG = ROOT / "configs" / "customers.yaml"


def test_sample_config_is_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)
    config = load_config(CONFIG)

    assert config.dataset == "customers"
    assert config.columns[0].name == "customer_id"
    assert config.columns[0].datatype == "string"
    assert [item.type for item in config.transformations] == ["filter", "derive"]
    assert len(config.config_hash) == 64


@pytest.mark.parametrize(
    "name",
    ["customers.yaml", "sensors.yaml", "reliability_incremental.yaml", "reliability_upsert.yaml"],
)
def test_full_and_reliability_demo_configs_are_valid(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)

    config = load_config(ROOT / "configs" / name)

    assert config.load_strategy in {"full", "incremental", "upsert"}


def test_unapproved_config_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)
    text = CONFIG.read_text(encoding="utf-8").replace("approved: true", "approved: false", 1)
    config_path = tmp_path / "unapproved.yaml"
    config_path.write_text(text, encoding="utf-8")

    with pytest.raises(ConfigError, match="requires explicit approval"):
        load_config(config_path)


def test_forbidden_sql_token_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)
    text = CONFIG.read_text(encoding="utf-8").replace(
        "status = 'ACTIVE'", "status = 'ACTIVE'; DROP TABLE customers"
    )
    config_path = tmp_path / "unsafe.yaml"
    config_path.write_text(text, encoding="utf-8")

    with pytest.raises(ConfigError, match="forbidden SQL token"):
        load_config(config_path)


def test_scd2_is_not_accepted_yet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)
    text = CONFIG.read_text(encoding="utf-8").replace("strategy: full", "strategy: scd2")
    config_path = tmp_path / "scd2.yaml"
    config_path.write_text(text, encoding="utf-8")

    with pytest.raises(ConfigError, match="full, incremental, or upsert"):
        load_config(config_path)
