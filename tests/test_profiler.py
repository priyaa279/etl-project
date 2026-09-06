from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from metadata_etl.cli import main
from metadata_etl.config import load_config
from metadata_etl.errors import ConfigError
from metadata_etl.onboarding import profile_csv, write_starter_config
from metadata_etl.pipeline import run_pipeline

ROOT = Path(__file__).parents[1]
STUDENTS = ROOT / "data" / "incoming" / "students.csv"


def _column(profile, name: str):
    return next(column for column in profile.columns if column.canonical_name == name)


def test_profiler_detects_leading_zero_identifier_and_key_candidate() -> None:
    profile = profile_csv(STUDENTS)
    student_id = _column(profile, "student_id")
    credits = _column(profile, "credits")

    assert student_id.inferred_type == "string"
    assert student_id.inference.confidence == "high"
    assert student_id.inference.reason == "leading_zeros"
    assert student_id.leading_zeros_detected is True
    assert student_id.possible_key_candidate is True
    assert student_id.string_length_min == 5
    assert student_id.string_length_max == 5
    assert credits.numeric_min == "12"
    assert credits.numeric_max == "18"


def test_profiler_distinguishes_clear_and_ambiguous_dates() -> None:
    profile = profile_csv(STUDENTS)
    enrolled = _column(profile, "enrolled_on")
    birth_date = _column(profile, "birth_date")

    assert enrolled.inferred_type == "date"
    assert enrolled.inferred_format == "%Y-%m-%d"
    assert enrolled.inference.confidence == "high"
    assert enrolled.review_required is False

    assert birth_date.inferred_type == "date"
    assert birth_date.inferred_format is None
    assert birth_date.inference.reason == "ambiguous_date_format"
    assert birth_date.review_required is True


def test_profiler_calculates_null_percentage_and_suggests_tokens() -> None:
    profile = profile_csv(STUDENTS)
    advisor = _column(profile, "advisor_email")

    assert advisor.null_count == 2
    assert advisor.null_percentage == pytest.approx(66.6667)
    assert advisor.possible_null_tokens == ("NULL", "")
    assert advisor.review_required is True
    assert "non_empty_null_token" in advisor.review_reasons


def test_exact_duplicates_are_counted_across_rows_beyond_sample(tmp_path: Path) -> None:
    source = tmp_path / "duplicates.csv"
    source.write_text("id,value\n1,a\n2,b\n1,a\n1,a\n", encoding="utf-8")

    profile = profile_csv(source, sample_size=2)

    assert profile.rows_scanned == 4
    assert profile.rows_profiled == 2
    assert profile.exact_duplicate_count == 2


def test_starter_yaml_contains_proposals_but_no_automatic_deduplication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    profile = profile_csv(STUDENTS)
    output = write_starter_config(profile, tmp_path / "students.yaml")
    generated = yaml.safe_load(output.read_text(encoding="utf-8"))

    assert generated["review"] == {
        "required": True,
        "approved": False,
        "approved_by": None,
    }
    assert generated["columns"]["student_id"]["inference"] == {
        "confidence": "high",
        "reason": "leading_zeros",
    }
    assert generated["columns"]["birth_date"]["format"] is None
    assert generated["columns"]["birth_date"]["review"]["approved"] is False
    assert generated["onboarding"]["exact_duplicate_count"] == 0
    assert generated["transformations"] == []


def test_generated_config_is_rejected_until_all_reviews_are_completed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    output = write_starter_config(profile_csv(STUDENTS), tmp_path / "students.yaml")

    with pytest.raises(ConfigError, match="requires explicit approval"):
        load_config(output)
    with pytest.raises(ConfigError, match="requires explicit approval"):
        run_pipeline(output)

    generated = yaml.safe_load(output.read_text(encoding="utf-8"))
    generated["review"].update(approved=True, approved_by="reviewer")
    generated["columns"]["birth_date"]["format"] = "%d/%m/%Y"
    for column in generated["columns"].values():
        if "review" in column:
            column["review"].update(approved=True, approved_by="reviewer")
    output.write_text(yaml.safe_dump(generated, sort_keys=False), encoding="utf-8")

    approved = load_config(output)
    assert approved.dataset == "students"
    assert (
        next(column for column in approved.columns if column.name == "birth_date").date_format
        == "%d/%m/%Y"
    )


def test_profile_cli_writes_starter_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(ROOT)
    output = tmp_path / "generated" / "students.yaml"

    exit_code = main(["profile", str(STUDENTS), "--output", str(output)])

    assert exit_code == 0
    assert output.is_file()
    assert '"exact_duplicate_count": 0' in capsys.readouterr().out
