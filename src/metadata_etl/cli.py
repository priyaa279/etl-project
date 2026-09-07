from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from metadata_etl.config import load_config
from metadata_etl.errors import ETLError
from metadata_etl.sql_compiler import compile_transform_sql
from metadata_etl.structured_logging import configure_logging, emit_event, redact_text


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _display(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, Decimal):
        return f"{value:.2f}"
    return str(value)


def _print_table(headers: tuple[str, ...], rows: list[tuple[Any, ...]]) -> None:
    rendered = [[_display(value) for value in row] for row in rows]
    widths = [
        max(len(header), *(len(row[index]) for row in rendered)) if rendered else len(header)
        for index, header in enumerate(headers)
    ]
    print("  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("  ".join("-" * width for width in widths))
    for row in rendered:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="etl",
        description="Metadata-driven ETL: new dataset = new config, not new pipeline.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate an approved YAML configuration")
    validate.add_argument("config", type=Path)
    validate.add_argument("--show-sql", action="store_true", help="Print the compiled DuckDB SQL")

    validate_all = subparsers.add_parser(
        "validate-all", help="Validate every top-level runnable YAML config in a directory"
    )
    validate_all.add_argument("config_dir", type=Path)
    validate_all.add_argument(
        "--require-source-bindings",
        action="store_true",
        help="Require credentials and live source binding for PostgreSQL source configs",
    )

    run = subparsers.add_parser("run", help="Execute an approved YAML configuration")
    run.add_argument("config", type=Path)
    run.add_argument("--from", dest="backfill_from", help="Exclusive backfill lower boundary")
    run.add_argument("--to", dest="backfill_to", help="Inclusive backfill upper boundary")
    run.add_argument(
        "--source-override",
        type=Path,
        help="Use one controlled file artifact without changing the approved YAML",
    )
    run.add_argument(
        "--correlation-id",
        help="Associate this execution with an external operation request",
    )

    profile = subparsers.add_parser("profile", help="Profile a CSV and propose a starter config")
    profile.add_argument("source", type=Path)
    profile.add_argument("--output", type=Path, help="Write the proposed YAML configuration")
    profile.add_argument("--sample-size", type=int, default=10_000)
    profile.add_argument("--delimiter", default=",")
    profile.add_argument("--force", action="store_true", help="Replace an existing output file")

    subparsers.add_parser("operators", help="List registered transformation operators")

    status = subparsers.add_parser("status", help="Show current dataset health")
    status.add_argument("--dataset", help="Show detailed health for one dataset")

    runs = subparsers.add_parser("runs", help="Show recent ETL runs")
    runs.add_argument("--dataset", help="Restrict recent runs to one dataset")
    runs.add_argument("--limit", type=_positive_int, default=10)

    subparsers.add_parser(
        "observability-install",
        help="Install or refresh the PostgreSQL observability schema and views",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = _parser().parse_args(argv)
    try:
        if args.command == "profile":
            from metadata_etl.onboarding import profile_csv, write_starter_config

            profile = profile_csv(
                args.source, sample_size=args.sample_size, delimiter=args.delimiter
            )
            output = profile.to_dict()
            if args.output:
                output["starter_config"] = str(
                    write_starter_config(profile, args.output, force=args.force)
                )
            print(json.dumps(output, indent=2))
            return 0

        if args.command == "operators":
            from metadata_etl.transformations.registry import default_registry

            print(json.dumps({"operators": default_registry().names}, indent=2))
            return 0

        if args.command in {"status", "runs", "observability-install"}:
            from metadata_etl.observability import MonitoringStore, monitoring_dsn

            dsn = monitoring_dsn()
            if args.command == "observability-install":
                from metadata_etl.postgres import PostgresStore

                with PostgresStore(dsn) as store:
                    store.ensure_metadata_tables()
                print("Observability schema and views installed.")
                return 0
            with MonitoringStore(dsn) as monitoring:
                if args.command == "status":
                    health = monitoring.status(args.dataset)
                    if args.dataset:
                        item = health[0]
                        fields = (
                            ("dataset", item["dataset"]),
                            ("health", item["health_status"]),
                            ("latest_run_id", item["latest_run_id"]),
                            ("latest_status", item["latest_run_status"]),
                            ("latest_run_time", item["latest_run_time"]),
                            ("duration_seconds", item["latest_duration_seconds"]),
                            ("rows_extracted", item["latest_rows_extracted"]),
                            ("rows_loaded", item["latest_rows_loaded"]),
                            ("rows_quarantined", item["latest_rows_quarantined"]),
                            ("drift_status", item["latest_drift_status"]),
                            ("latest_watermark", item["latest_watermark"]),
                            ("last_successful_run", item["last_successful_run"]),
                            ("consecutive_failures", item["consecutive_failure_count"]),
                            ("hours_since_success", item["hours_since_last_success"]),
                        )
                        _print_table(("field", "value"), list(fields))
                    else:
                        _print_table(
                            (
                                "dataset",
                                "health",
                                "latest_status",
                                "loaded",
                                "quarantined",
                                "drift",
                            ),
                            [
                                (
                                    item["dataset"],
                                    item["health_status"],
                                    item["latest_run_status"],
                                    item["latest_rows_loaded"],
                                    item["latest_rows_quarantined"],
                                    item["latest_drift_status"],
                                )
                                for item in health
                            ],
                        )
                    return 0
                recent = monitoring.runs(dataset=args.dataset, limit=args.limit)
                _print_table(
                    (
                        "run_id",
                        "dataset",
                        "status",
                        "started_at",
                        "duration",
                        "loaded",
                        "quarantined",
                        "strategy",
                        "mode",
                    ),
                    [
                        (
                            item["run_id"],
                            item["dataset"],
                            item["status"],
                            item["started_at"],
                            item["duration_seconds"],
                            item["rows_loaded"],
                            item["rows_quarantined"],
                            item["load_strategy"],
                            item["run_mode"],
                        )
                        for item in recent
                    ],
                )
                return 0

        if args.command == "validate":
            config = load_config(args.config)
            from metadata_etl.validation import validate_plan

            validate_plan(config)
            emit_event(
                "CONFIG_VALIDATED",
                dataset=config.dataset,
                stage="CONFIG",
                source_type=config.source_type,
                load_strategy=config.load_strategy,
            )
            output: dict[str, object] = {
                "status": "VALID",
                "dataset": config.dataset,
                "config_schema_version": config.schema_version,
                "config_hash": config.config_hash,
                "transformations": len(config.transformations),
                "contracts": len(config.contracts),
            }
            print(json.dumps(output, indent=2))
            if args.show_sql:
                print("\n-- Compiled SQL --")
                print(compile_transform_sql(config))
            return 0

        if args.command == "validate-all":
            from metadata_etl.orchestration import validate_all_configs

            validations = validate_all_configs(
                args.config_dir,
                require_source_bindings=args.require_source_bindings,
            )
            print(
                json.dumps(
                    {
                        "status": "VALID",
                        "configs": [
                            {
                                "path": str(item.path),
                                "dataset": item.dataset,
                                "mode": item.mode,
                            }
                            for item in validations
                        ],
                    },
                    indent=2,
                )
            )
            return 0

        if args.command == "run":
            from metadata_etl.pipeline import run_pipeline

            result = run_pipeline(
                args.config,
                backfill_from=args.backfill_from,
                backfill_to=args.backfill_to,
                source_override=args.source_override,
                correlation_id=args.correlation_id,
            )
            print(json.dumps(result.to_dict(), indent=2))
            return 0
    except ETLError as exc:
        print(f"ERROR: {redact_text(exc)}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001  # pragma: no cover - safe process boundary
        print(f"ERROR: unexpected {type(exc).__name__}", file=sys.stderr)
        return 3
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
