# Metadata-Driven ETL Framework

> The framework code is dataset-agnostic. New datasets are onboarded through profiled,
> human-approved, version-controlled configuration rather than new pipeline code.

**New dataset = new config, not new pipeline.**

This repository contains the first complete vertical slice: an approved YAML configuration drives
a CSV source through raw preservation, structural normalization, SQL transformations, PostgreSQL
staging, atomic trusted-table publishing, and a run ledger. There is no customer-specific Python.

## Milestone 1 flow

```text
customers.csv
  -> validate approved customers.yaml
  -> create run ID and RUNNING ledger entry
  -> preserve byte-identical raw copy
  -> normalize configured columns and types
  -> compile filter/derive operators into DuckDB SQL
  -> load a run-scoped PostgreSQL staging table
  -> atomically replace the trusted table
  -> record SUCCEEDED/FAILED metrics in the ledger
```

The engine reads and transforms the preserved raw artifact rather than rereading a potentially
changing incoming file. The config hash is SHA-256 over the exact YAML bytes. If the folder is a Git
repository, the current commit SHA is recorded; otherwise the ledger uses `UNAVAILABLE`.

## What this milestone supports

- CSV sources with configurable delimiters
- explicit source-to-canonical column mapping
- string, integer, decimal, date, timestamp, and boolean types
- configured trimming and null tokens
- required human approval, including nested review flags
- SQL `filter` and `derive` transformations
- PostgreSQL full loads through transaction-scoped staging
- immutable-by-convention raw run directories and SHA-256 checksums
- run status, counts, timing, config identity, and errors in `etl_meta.etl_run_ledger`

Not implemented yet: profiling, quality contracts/quarantine, schema drift, incremental/upsert/SCD2,
JSON/Parquet/PostgreSQL sources, Airflow, Power BI, or the broader transformation registry. Those
belong to later milestones after this path is running reliably.

## Quick start

Requirements: Python 3.11+ and PostgreSQL. Docker Compose is optional but the easiest way to start
the included local database.

```bash
python -m venv .venv
```

Activate the environment, then install the project:

```bash
python -m pip install -e ".[dev]"
docker compose up -d postgres
```

Set the destination connection. PowerShell:

```powershell
$env:ETL_POSTGRES_DSN = "postgresql://etl:etl@localhost:5432/etl"
```

Validate the config and compiled plan without writing to PostgreSQL:

```bash
etl validate configs/customers.yaml --show-sql
```

Run the end-to-end pipeline:

```bash
etl run configs/customers.yaml
```

Expected sample result: four rows extracted, the `ACTIVE` filter leaves three rows, and three rows
are atomically published to `public.customers`. The trusted table includes the derived
`customer_label` column. Inspect the ledger with:

```sql
SELECT *
FROM etl_meta.etl_run_ledger
ORDER BY started_at DESC;
```

## Configuration contract

The runnable example is [`configs/customers.yaml`](configs/customers.yaml). Important rules:

- `config_schema_version` must currently be `"1.0"`.
- A required review must have `approved: true` and a non-empty `approved_by`.
- Date columns require an explicit `format`; the engine does not guess ambiguous dates.
- Relative source and raw paths are resolved from the command's working directory. Run the CLI from
  the repository root for the sample.
- PostgreSQL credentials are named by `load.connection_env`; secrets are not stored in YAML.
- SQL expressions reject statement separators, comments, and data-changing/DDL keywords. DuckDB
  binds the complete plan during `etl validate`, catching missing columns and invalid expressions.
- Milestone 1 accepts only `source.type: csv` and `load.strategy: full`.

## Atomicity and reruns

Every run gets a unique staging table. Staging creation, row copy, trusted-table truncate/insert, and
staging cleanup happen in one PostgreSQL transaction. A failure rolls the transaction back and
leaves the existing trusted table unchanged. A successful full rerun replaces the trusted contents,
so it does not append duplicates.

## Tests

```bash
pytest
ruff check .
```

The tests cover configuration approval and safety, leading-zero preservation, SQL transformation
behavior, and byte-identical raw copying. A live PostgreSQL instance is needed for the end-to-end
CLI run, but not for these unit tests.

## Repository map

```text
configs/customers.yaml              approved dataset metadata
data/incoming/customers.csv         example input
data/raw/                            run-scoped untouched copies (Git-ignored)
src/metadata_etl/config.py          parsing and validation
src/metadata_etl/sql_compiler.py    generic SQL compilation
src/metadata_etl/source.py          raw preservation
src/metadata_etl/postgres.py        ledger, staging, atomic publish
src/metadata_etl/pipeline.py        dataset-agnostic execution sequence
src/metadata_etl/cli.py             etl validate / etl run
sql/metadata_tables.sql             ledger DDL reference
tests/                              unit tests
```

