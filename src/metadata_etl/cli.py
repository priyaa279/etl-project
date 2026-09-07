from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from metadata_etl.config import load_config
from metadata_etl.errors import ETLError
from metadata_etl.sql_compiler import compile_transform_sql


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="etl",
        description="Metadata-driven ETL: new dataset = new config, not new pipeline.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate an approved YAML configuration")
    validate.add_argument("config", type=Path)
    validate.add_argument("--show-sql", action="store_true", help="Print the compiled DuckDB SQL")

    run = subparsers.add_parser("run", help="Execute an approved YAML configuration")
    run.add_argument("config", type=Path)
    run.add_argument("--from", dest="backfill_from", help="Exclusive backfill lower boundary")
    run.add_argument("--to", dest="backfill_to", help="Inclusive backfill upper boundary")

    profile = subparsers.add_parser("profile", help="Profile a CSV and propose a starter config")
    profile.add_argument("source", type=Path)
    profile.add_argument("--output", type=Path, help="Write the proposed YAML configuration")
    profile.add_argument("--sample-size", type=int, default=10_000)
    profile.add_argument("--delimiter", default=",")
    profile.add_argument("--force", action="store_true", help="Replace an existing output file")

    subparsers.add_parser("operators", help="List registered transformation operators")
    return parser


def main(argv: list[str] | None = None) -> int:
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

        if args.command == "validate":
            config = load_config(args.config)
            from metadata_etl.validation import validate_plan

            validate_plan(config)
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

        if args.command == "run":
            from metadata_etl.pipeline import run_pipeline

            result = run_pipeline(
                args.config,
                backfill_from=args.backfill_from,
                backfill_to=args.backfill_to,
            )
            print(json.dumps(result.to_dict(), indent=2))
            return 0
    except ETLError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
