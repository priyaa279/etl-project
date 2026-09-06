from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from metadata_etl.errors import ProfilingError
from metadata_etl.onboarding.profiler import DatasetProfile


def _source_display_path(source_path: Path) -> str:
    try:
        return source_path.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return source_path.as_posix()


def generate_starter_config(profile: DatasetProfile) -> dict[str, Any]:
    """Build an intentionally unapproved runtime config from structural proposals."""
    observed_null_tokens: list[str] = []
    for column in profile.columns:
        for token in column.possible_null_tokens:
            if token not in observed_null_tokens:
                observed_null_tokens.append(token)

    columns: dict[str, dict[str, Any]] = {}
    for column in profile.columns:
        proposal: dict[str, Any] = {
            "source": column.source_name,
            "type": column.inferred_type,
            "nullable": column.null_count > 0,
            "inference": {
                "confidence": column.inference.confidence,
                "reason": column.inference.reason,
            },
            "profile": {
                "null_percentage": column.null_percentage,
                "possible_null_tokens": list(column.possible_null_tokens),
                "distinct_count": column.distinct_count,
                "cardinality_ratio": column.cardinality_ratio,
                "leading_zeros_detected": column.leading_zeros_detected,
                "possible_key_candidate": column.possible_key_candidate,
            },
        }
        if column.inferred_type in {"date", "timestamp"}:
            proposal["format"] = column.inferred_format
        if column.numeric_min is not None:
            proposal["profile"]["numeric_min"] = column.numeric_min
            proposal["profile"]["numeric_max"] = column.numeric_max
        if column.string_length_min is not None:
            proposal["profile"]["string_length"] = {
                "min": column.string_length_min,
                "max": column.string_length_max,
                "average": column.string_length_average,
            }
        if column.review_required:
            proposal["review"] = {
                "required": True,
                "approved": False,
                "reasons": list(column.review_reasons),
            }
        columns[column.canonical_name] = proposal

    dataset = profile.dataset_name
    return {
        "config_schema_version": "1.0",
        "dataset": {"name": dataset},
        "review": {
            "required": True,
            "approved": False,
            "approved_by": None,
        },
        "onboarding": {
            "generated_at": datetime.now(UTC).isoformat(),
            "rows_scanned": profile.rows_scanned,
            "rows_profiled": profile.rows_profiled,
            "sample_size_requested": profile.sample_size_requested,
            "exact_duplicate_count": profile.exact_duplicate_count,
            "note": "Duplicates are reported only; no deduplication is configured automatically.",
        },
        "source": {
            "type": "csv",
            "path": _source_display_path(profile.source_path),
            "options": {"delimiter": profile.delimiter},
        },
        "runtime": {"raw_root": "data/raw"},
        "normalization": {
            "trim_strings": True,
            "null_tokens": observed_null_tokens,
        },
        "columns": columns,
        "transformations": [],
        "load": {
            "strategy": "full",
            "connection_env": "ETL_POSTGRES_DSN",
            "schema": "public",
            "staging_table": f"{dataset}_staging",
            "target_table": dataset,
        },
    }


def render_starter_yaml(profile: DatasetProfile) -> str:
    return yaml.safe_dump(
        generate_starter_config(profile),
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
    )


def write_starter_config(
    profile: DatasetProfile, output_path: str | Path, *, force: bool = False
) -> Path:
    path = Path(output_path).resolve()
    if path.exists() and not force:
        raise ProfilingError(f"Output config already exists: {path}; pass --force to replace it")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_starter_yaml(profile), encoding="utf-8")
    except OSError as exc:
        raise ProfilingError(f"Could not write starter config {path}: {exc}") from exc
    return path
