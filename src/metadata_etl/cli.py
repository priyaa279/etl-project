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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
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
            }
            print(json.dumps(output, indent=2))
            if args.show_sql:
                print("\n-- Compiled SQL --")
                print(compile_transform_sql(config))
            return 0

        if args.command == "run":
            from metadata_etl.pipeline import run_pipeline

            result = run_pipeline(args.config)
            print(json.dumps(result.to_dict(), indent=2))
            return 0
    except ETLError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
