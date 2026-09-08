# Metadata-Driven ETL Framework

> The framework code is dataset-agnostic. New datasets are onboarded through profiled,
> human-approved, version-controlled configuration rather than new pipeline code.

**New dataset = new config, not new pipeline.**

This project is a complete local data-engineering platform built with Python, DuckDB, PostgreSQL,
YAML, Docker, and Airflow. Approved metadata drives extraction, raw preservation, SQL
transformations, quality gates, privacy-safe quarantine, reliable publishing, and observability.
Airflow schedules the same CLI used locally; it contains no ETL business logic.

**Sources:** CSV, JSON, Parquet, PostgreSQL · **Reliability:** drift, contracts, quarantine,
watermarks, full/incremental/upsert/SCD2, backfills · **Production:** Docker, Airflow, CI,
structured logs · **Operations:** PostgreSQL health views and monitoring CLI

Milestones 1–9 are implemented. The final dataset-independence proof runs unrelated e-commerce,
higher-education, and IoT inputs through the same engine without adding domain-specific Python.

## Quick start

Install the local CLI, configure `.env` from `.env.example`, and start the stack:

```powershell
python -m pip install -e ".[dev]"
docker compose up -d
$env:ETL_POSTGRES_DSN = "postgresql://etl:YOUR_PASSWORD@localhost:5432/etl"
etl validate-all configs
etl run configs/portfolio_orders.yaml
etl status
```

Local commands invoke the framework directly. Airflow is optional orchestration around those same
commands; its separate setup and smoke-test instructions appear below.

## Application UI

Milestone 10C.2B preserves the read-only monitoring application and existing-dataset operations
while completing the separately gated new-dataset onboarding lifecycle:

```text
Existing approved dataset -> upload -> preflight -> explicit run
        React controls -> FastAPI operation layer -> Airflow orchestrates
        -> existing ETL CLI processes -> observability reports the exact run
```

```text
Completely new dataset -> upload -> profile -> draft YAML -> human schema review
  -> explicit normalization -> transformations -> quality -> load and drift policy
  -> validate exact draft -> explicit human approval -> Git-versioned config
  -> generic Airflow discovery -> explicit first run -> correlated observability
```

The **ETL Control Center** includes Overview, Datasets, Runs, Data Quality, Schema Drift, and
Watermarks pages, plus dataset and run detail views. Eligible Dataset Detail pages now link to
**Upload new data**. PostgreSQL-source datasets clearly show that file upload is not applicable.
When enabled, **Onboard Dataset** is a separate action that creates and validates only an
unapproved draft. Approval and the first ETL run remain separate, explicit actions.

Operational writes are disabled by default. Set `ETL_CONTROL_OPERATIONS_ENABLED=true` only in a
trusted local environment, configure server-side Airflow authentication, and restart the API.
The browser never receives PostgreSQL or Airflow credentials, server paths, raw records, or
row-level quarantine values.

New-dataset onboarding has its own `ETL_CONTROL_ONBOARDING_ENABLED=false` gate. Enable it only in a
local/trusted environment until authentication and authorization are added. Uploads are stored in
Git-ignored `data/onboarding`; generated YAML is stored in Git-ignored `configs/drafts`. Neither is
served by the frontend. YAML remains the source of truth, while React provides the human-friendly
review and configuration interface. Every edit is validated server-side and atomically written to
the actual draft YAML; refreshing the browser reloads that persisted source of truth.
Final approval checks the current bytes against the successful validation SHA-256, records the
human approver, materializes the source under Git-ignored `data/sources`, promotes only
`configs/<dataset>.yaml`, and creates a local Git commit. The repository must otherwise be clean.
All configuration editing endpoints reject changes after approval.

`ETL_CONTROL_GIT_PUSH_ENABLED` is a separate gate and defaults to false. Local versioning is the
default; when explicitly enabled, the restricted adapter can push only `HEAD` to the configured
fixed remote and branch. Browser input can never choose a repository, path, remote, branch, or Git
command.

The upload landing area is temporary application input, not ETL evidence. Once Airflow invokes the
CLI with `--source-override`, the normal connector preserves a separate immutable raw artifact.
Preflight is also only a preview: the real ETL run recalculates schema drift and enforces the
approved policy before publishing.

Start the integrated application using the root `.env` configuration:

```powershell
docker compose up -d --build
```

- Frontend: `http://localhost:4173`
- Application API: `http://localhost:8000`
- API health: `http://localhost:8000/api/health`
- Airflow: `http://localhost:8080`

For separate frontend/API development commands, endpoint details, privacy boundaries, and the
10A, 10B, 10C.1, 10C.2A, and 10C.2B boundaries, see
[frontend architecture](docs/frontend_architecture.md).

## Portfolio proof

| Domain | Source | Generic processing | Quality | Load |
| --- | --- | --- | --- | --- |
| E-commerce order lines | nested JSON | explicit explode, cast, map, filter, derive | not-null, unique, range | upsert |
| Higher-education students | CSV | derive | not-null, unique, range, regex | full |
| IoT telemetry | Parquet | cast, map, filter, deduplicate, derive | not-null, unique, range | incremental |

Run the reproducible proof against the local stack:

```powershell
docker compose up -d
$env:ETL_POSTGRES_DSN = "postgresql://etl:etl@localhost:5432/etl"
python scripts/run_portfolio_demo.py
```

The demonstration validates all configs, runs and reruns the three domains, exercises schema
drift, SCD2, and bounded-backfill behavior, and displays trusted counts plus operational health.
See [dataset-independence evidence](docs/dataset_independence.md), the
[architecture guide](docs/architecture.md), and [portfolio talking points](docs/portfolio_talking_points.md).

## Runtime flow

```text
input source
  -> validate approved dataset YAML
  -> create run ID and RUNNING ledger entry
  -> select CSV/JSON/Parquet/PostgreSQL connector
  -> preserve the extracted source representation
  -> fingerprint raw schema and apply drift policy
  -> explicitly normalize source structure and configured types
  -> fingerprint and validate canonical schema
  -> validate operators through the transformation registry
  -> compile cast/filter/derive/map/deduplicate operators into DuckDB SQL
  -> apply the previous successful watermark for incremental runs
  -> evaluate not-null/unique/range/regex contracts
  -> write failed rows as privacy-protected quarantine details
  -> send only contract-passing rows forward
  -> load a run-scoped PostgreSQL staging table
  -> atomically full-publish, append, merge, backfill, or maintain SCD2 history
  -> advance the watermark only after successful publish
  -> record SUCCEEDED/FAILED metrics in the ledger and structured execution logs
```

The engine reads and transforms the preserved raw artifact rather than rereading a potentially
changing incoming file. The config hash is SHA-256 over the exact YAML bytes. If the folder is a Git
repository, the current commit SHA is recorded; otherwise the ledger uses `UNAVAILABLE`.

## Implemented capabilities

- CSV sources with configurable delimiters
- JSON arrays/objects and newline-delimited JSON with explicit nested normalization and explosion
- Parquet sources with embedded-schema fingerprinting
- table-based PostgreSQL sources with environment-only credentials and CSV raw snapshots
- one connector registry and shared downstream pipeline for every source type
- explicit source-to-canonical column mapping
- string, integer, decimal, date, timestamp, and boolean types
- configured trimming and null tokens
- required human approval, including nested review flags
- a generic transformation registry with operator-owned validation and SQL compilation
- SQL `cast`, `filter`, `derive`, `map`, and `deduplicate` transformations
- sampled CSV profiling with datatype, null, cardinality, range, length, and date-pattern proposals
- exact duplicate-row reporting across the scanned CSV
- starter YAML generation with explicit human-review gates
- post-transformation `not_null`, `unique`, `range`, and `regex` contracts
- row-level quarantine with `full`, `masked`, `hashed`, and `none` value policies
- per-rule results in `etl_meta.data_quality_results`
- unique quarantined-row counts in the run ledger
- deterministic raw and canonical schema fingerprints
- configurable `allow`, `warn`, and `fail` schema-drift actions
- drift history in `etl_meta.schema_drift_history`
- PostgreSQL full, incremental, and business-key upsert loads through transaction-scoped staging
- config-driven SCD Type 2 history with atomic expiry/current-version insertion
- bounded, idempotent backfills that do not advance the production watermark
- centrally persisted successful watermarks in `etl_meta.etl_watermarks`
- insert/update counts and before/after watermarks in the run ledger
- idempotent full reruns, successful incremental reruns, and business-key upserts
- immutable-by-convention raw run directories and SHA-256 checksums
- run status, counts, timing, config identity, and errors in `etl_meta.etl_run_ledger`
- opt-in, config-discovered Airflow DAGs that validate and invoke the existing ETL CLI
- JSON structured logs with run, dataset, stage, source, load, count, and duration context
- pinned ETL/Airflow Docker images with PostgreSQL and service health checks
- GitHub Actions checks for tests, lint, formatting, runnable configs, compilation, and PostgreSQL
- an `etl_observability` PostgreSQL schema with health, trend, quality, drift, and watermark views
- read-only `etl status` and `etl runs` operational monitoring commands
- a React/TypeScript ETL Control Center backed by a read-only FastAPI application boundary
- three unrelated portfolio datasets executed through one dataset-agnostic engine
- a rerunnable CLI-driven demonstration covering quality, drift, and load idempotency evidence
- architecture, dataset-independence, and interview walkthrough documentation

Not implemented yet: Power BI dashboards, cloud services, alerting integrations, Kafka/streaming,
Spark, dbt, Kubernetes, Terraform, automatic schema migration, or automatic business-rule
inference. Those belong to later milestones.

## Dataset onboarding flow

The Control Center implements profiled, human-reviewed configuration for CSV, JSON, and Parquet files:

```text
Upload -> existing profiler -> existing starter-config generator -> unapproved draft YAML
       -> Understanding View / read-only YAML View -> persisted schema decisions
       -> explicit JSON normalization where needed
       -> transformations -> contracts and quarantine privacy -> load -> drift
       -> validate exact draft and uploaded source -> READY_FOR_APPROVAL
       -> acknowledge exact hash -> promote approved YAML -> Git commit
       -> wait for generic DAG -> explicit first run -> completion
```

The profiler proposes. The human approves. The engine executes only approved configuration.
The approval request carries the browser-displayed expected hash and a self-declared approver. The
server accepts it only when that hash equals both the persisted successful-validation hash and the
current draft hash. It does not silently revalidate a changed draft. Older pre-10C.2B drafts are
upgraded to the stable source/on-demand orchestration shape only during an explicit validation,
which produces a new hash that must be reviewed.

Approved configs enable the existing generic DAG factory with `schedule: null`, so they are
available on demand without silently creating a recurring schedule. Airflow discovery may be
retried without revoking approval. The first run is a separate button and carries one exact
correlation ID through Airflow, the CLI, the ledger, and the completion view. A failed first run
does not revoke the approved configuration and can be retried explicitly.

The existing CLI onboarding flow remains available:

```text
new CSV
  -> profile a bounded sample
  -> report exact duplicates from the full CSV scan
  -> infer safe structural proposals
  -> flag ambiguous or meaning-sensitive proposals
  -> generate an unapproved starter YAML
  -> human reviews, completes, and approves YAML
  -> etl validate
  -> etl run
```

Profile the included higher-education example and create a starter config:

```bash
etl profile data/incoming/students.csv --output configs/students.yaml
```

The default profiling sample is 10,000 rows. It can be changed without changing pipeline code:

```bash
etl profile data/incoming/students.csv --sample-size 50000
```

The report includes source and canonical column names, proposed types, null percentages, observed
possible null tokens, distinct counts and cardinality, numeric bounds, string lengths, date
patterns, leading-zero detection, possible key candidates, and ambiguous inferences. Column
statistics are sample-based; exact duplicate counting scans all rows. Duplicates are reported only
and never create an automatic `deduplicate` transformation.

A generated proposal intentionally starts like this:

```yaml
config_schema_version: '1.0'
dataset:
  name: students
review:
  required: true
  approved: false
  approved_by: null
columns:
  student_id:
    source: Student ID
    type: string
    inference:
      confidence: high
      reason: leading_zeros
  birth_date:
    source: Birth Date
    type: date
    format: null
    inference:
      confidence: medium
      reason: ambiguous_date_format
    review:
      required: true
      approved: false
      reasons:
        - ambiguous_date_format
transformations: []
contracts: []
```

`format: null` is deliberate: the profiler refuses to choose between day-first and month-first
dates when both interpretations are possible. The reviewer must select the format, approve every
required nested review, and approve the top-level config. Until then, both `etl validate` and
`etl run` reject the proposal.

## Detailed local CLI setup

Requirements: Python 3.11+ and PostgreSQL. Docker Compose is optional for CLI development and is the
supported way to run the complete local PostgreSQL/Airflow stack.

```bash
python -m venv .venv
```

Activate the environment, then install the project:

```bash
python -m pip install -e ".[dev]"
```

Set the destination connection. PowerShell:

```powershell
$env:ETL_POSTGRES_DSN = "postgresql://etl:etl@localhost:5432/etl"
```

Validate the config and compiled plan without writing to PostgreSQL:

```bash
etl validate configs/customers.yaml --show-sql
```

Inspect the active transformation registry:

```bash
etl operators
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

Validate every committed top-level runnable config with:

```bash
etl validate-all configs
```

PostgreSQL source configs receive static validation when their source credential environment
variable is unavailable. Use `--require-source-bindings` when every configured source must also be
connected and bound. Draft profiler output belongs under `configs/drafts/`; that directory is not
scanned by `validate-all` or Airflow discovery.

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
- Supported source types are `csv`, `json`, `parquet`, and table-based `postgres`; load strategies
  are `full`, `incremental`, `upsert`, and `scd2`.
- Starter YAML is a proposal, not executable approval. Required review blocks must include
  `approved: true` and a non-empty `approved_by` before runtime.

## Transformation operators

Operators are resolved by `type` through one registry. The config parser does not contain
operator-specific branches, and the pipeline does not know anything about customers, students,
sensors, or any other business domain.

```yaml
transformations:
  - id: T001
    type: cast
    column: reading
    datatype: decimal

  - id: T002
    type: map
    column: quality
    mappings:
      OK: ACCEPTED
      BAD: REJECTED

  - id: T003
    type: filter
    condition: quality = 'ACCEPTED' AND reading >= 10

  - id: T004
    type: deduplicate
    keys: [device_id]
    order_by:
      observed_at: desc

  - id: T005
    type: derive
    target_column: adjusted_reading
    datatype: decimal
    expression: reading + 0.5
```

`cast` replaces a column with the requested type. `map` replaces configured values and keeps
unmapped values by default; an explicit `default` can override that behavior. `filter` and `derive`
accept approved DuckDB expressions. `deduplicate` retains row number one for each configured key,
using the declared sort precedence. The complete sensor example is
[`configs/sensors.yaml`](configs/sensors.yaml).

## Data-quality contracts and quarantine

Contracts are validated against the schema produced by the transformation plan, so they may safely
reference derived columns. Invalid rule types, missing columns, malformed ranges, reversed bounds,
invalid regular expressions, duplicate rule IDs, and invalid unique keys fail before runtime.

```yaml
columns:
  student_id:
    source: Student ID
    type: string
    classification: restricted
    quarantine:
      value: hashed

  email:
    source: Email
    type: string
    classification: pii
    quarantine:
      value: masked

  gpa:
    source: GPA
    type: decimal
    quarantine:
      value: full

transformations:
  - id: T001
    type: derive
    target_column: total_cost
    datatype: decimal
    expression: credits * cost_per_credit

contracts:
  - id: DQ001
    type: not_null
    column: student_id

  - id: DQ002
    type: unique
    columns: [student_id, term]

  - id: DQ003
    type: range
    column: gpa
    min: 0
    max: 4

  - id: DQ004
    type: regex
    column: email
    pattern: '^[^@]+@[^@]+\.[^@]+$'

  - id: DQ005
    type: range
    column: total_cost
    min: 0
```

A row failing one or more contracts is excluded from staging and the trusted table. Each rule
failure creates a quarantine detail, while `rows_quarantined` counts distinct failed rows. Rule
summaries record checked and failed counts in `etl_meta.data_quality_results`. Row-level failures do
not fail the pipeline; invalid configuration, transformation, metadata persistence, and publishing
remain pipeline-level failures.

Quarantine never stores the full record. It stores a SHA-256 record fingerprint plus the rule,
failure reason, relevant column, and only the failed value permitted by that column's policy:

- `full`: store the scalar value as text.
- `masked`: deterministically retain the first and last character and mask the middle.
- `hashed`: store a stable SHA-256 digest.
- `none`: store no failed value. This is the default, including for derived columns.

For `range` and `regex`, null values pass unless a separate `not_null` rule is configured. A
`unique` rule ignores keys containing null; pair it with `not_null` rules when null keys are invalid.
The runnable failure example is [`configs/students_quality.yaml`](configs/students_quality.yaml). It
extracts and transforms five rows, passes one distinct row, quarantines four distinct rows, and
loads only the passing row. Some rows fail multiple rules, producing six quarantine details.

## Schema fingerprints and drift

Every run fingerprints two deliberately different structures:

- Raw schema: **“Did the physical source change?”** It includes CSV source names, order, and
  observed physical value representations before normalization.
- Canonical schema: **“Does the normalized structure still match what the pipeline expects?”** It
  includes approved canonical names, datatypes, nullability, order, and relevant formats, without
  coupling the canonical hash to raw source names.

Hashes are SHA-256 over deterministic JSON and never include timestamps or run IDs. The current
schemas are compared with the latest successful run for the dataset. This means a failed run never
becomes the next baseline. Each detected event is persisted with its policy and action in
`etl_meta.schema_drift_history` before any trusted-table mutation.

```yaml
schema_drift:
  added_columns: warn
  removed_columns: fail
  datatype_change: fail
  canonical_change: fail
```

`allow` records the event and continues, `warn` records a warning state and continues, and `fail`
records the event then stops before staging/publishing. The two schema-drift demo configs use the
same dataset name: run [`configs/schema_drift_base.yaml`](configs/schema_drift_base.yaml), then
[`configs/schema_drift_added.yaml`](configs/schema_drift_added.yaml). The added raw `Email` column
warns while the approved canonical structure remains unchanged.

## Incremental loads and watermarks

An incremental config names a canonical source column and its approved datatype:

```yaml
load:
  strategy: incremental
  watermark:
    column: updated_at
    type: timestamp
```

The first run processes all eligible rows when no stored watermark exists. An optional
`initial_value` can provide an explicit lower bound. Later runs normalize and process only rows
strictly newer than the previous successful value. `rows_extracted` therefore describes the range
actually processed, not the complete historical CSV.

**Watermark advances only after successful publish.** The trusted append and watermark update are
ordered inside one PostgreSQL transaction. If transformation, quality persistence, staging,
publishing, or the watermark update fails, the transaction does not leave a partial append and the
old watermark remains available for a safe retry. A no-new-data run publishes zero rows and keeps
the watermark unchanged. See
[`configs/reliability_incremental.yaml`](configs/reliability_incremental.yaml).

## Upsert and idempotency

Upsert requires one or more post-transformation business-key columns:

```yaml
load:
  strategy: upsert
  keys: [entity_id]
```

Missing, empty, duplicate, or unknown key definitions fail during config validation. At runtime,
null or duplicate keys in the valid input are rejected before target changes. PostgreSQL stages the
valid rows and performs a locked transactional `MERGE`: existing keys receive current values and
new keys are inserted. Quarantined rows never enter this operation. See
[`configs/reliability_upsert.yaml`](configs/reliability_upsert.yaml).

Idempotency is strategy-specific: full loads atomically replace the trusted snapshot; incremental
loads use the successful watermark and transaction boundary; upserts merge by approved business
keys. Run IDs remain audit identifiers and are not the mechanism preventing duplicate trusted rows.

## Connector architecture and raw preservation

`CSVConnector`, `JSONConnector`, `ParquetConnector`, and `PostgresConnector` implement one source
interface and are selected by `source.type`. Every connector returns a preserved raw artifact, a
physical schema fingerprint, and a DuckDB-readable relation. Normalization, transformations,
contracts, drift handling, and loading are shared after that boundary.

**The raw layer preserves the extracted source representation. The canonical layer represents the
approved relational structure used by the engine.** CSV, JSON, and Parquet inputs are copied
byte-for-byte. A PostgreSQL table source is materialized as a deterministic CSV snapshot containing
the exact rows returned for the run; credentials remain in `SOURCE_POSTGRES_DSN` or another named
environment variable.

Only table-based PostgreSQL extraction is supported in this milestone. Arbitrary source queries are
rejected to keep ingestion read-only and conservative.

## JSON normalization

Nested JSON is never flattened by guesswork:

```text
raw nested payload
  -> raw fingerprint
  -> configured normalization
  -> canonical relational schema
  -> canonical validation
```

```yaml
source:
  type: json
  path: data/incoming/nested_orders.json

normalization:
  json:
    root_path: orders
    fields:
      order_id: order.id
      customer_id: customer.id
      order_date: order.date
    explode:
      path: items
      as: item
      fields:
        product_id: item.product_id
        quantity: item.quantity
```

Arrays of objects and newline-delimited objects are accepted. Nested structures require explicit
field paths, and arrays produce rows only through an explicit `explode`. Missing paths, non-array
explode targets, and selected fields that still resolve to objects/arrays fail before publishing.
See [`configs/nested_orders.yaml`](configs/nested_orders.yaml).

## SCD Type 2

SCD2 tracks history without embedding business meaning in Python:

```yaml
load:
  strategy: scd2
  keys: [entity_id]
  tracked_columns: [region, segment]
  effective_timestamp:
    column: updated_at
  history_columns:
    valid_from: valid_from
    valid_to: valid_to
    is_current: is_current
```

A new key creates a current version. An unchanged tracked value does nothing. A changed value expires
the old current version and inserts the new version in the same PostgreSQL transaction. Exact reruns
find the existing effective version and do not duplicate history. Null keys/effective times, stale
changes, conflicting versions, and malformed configuration fail safely. Only contract-passing rows
participate. Run [`configs/scd2_regions_run1.yaml`](configs/scd2_regions_run1.yaml), followed by
[`configs/scd2_regions_run2.yaml`](configs/scd2_regions_run2.yaml), to demonstrate the transition.

## Backfills

Incremental configs support isolated bounded runs:

```bash
etl run configs/reliability_incremental.yaml \
  --from "2026-09-01 00:00:00" \
  --to "2026-09-03 23:59:59"
```

The lower boundary is exclusive and the upper boundary inclusive. A backfill atomically replaces
only that trusted interval, so repeating the same window is idempotent. It does not read, overwrite,
or advance the normal production watermark. The ledger records `run_mode`, `backfill_from`, and
`backfill_to` along with source type and SCD2 row metrics.

## Production architecture

```text
new dataset
  -> profile
  -> human-approved config in Git
  -> CI validation
  -> Airflow scheduler discovers enabled config
  -> validate_config task
  -> run_etl task invokes the generic ETL CLI
  -> extract / raw / drift / normalize / transform
  -> contracts / quarantine / configured load strategy
  -> atomic publish
  -> PostgreSQL ledger + structured logs

parallel monitoring path:
ETL framework
  -> run ledger / quality / quarantine / drift / watermark metadata
  -> etl_observability SQL views
  -> etl status / etl runs
```

**Airflow = orchestration. ETL framework = data processing.** Airflow owns schedules, dependencies,
task status, and retries. Extraction, normalization, transformations, contracts, quarantine,
schema drift, watermarks, full/incremental/upsert/SCD2 behavior, and publishing remain in
`metadata_etl`. **The same `etl run <config>` execution path is used locally and by Airflow.**

The single DAG factory in `airflow/dags/generic_etl.py` scans approved top-level YAML files. It
creates a `validate_config -> run_etl` DAG for each explicitly enabled config. The execution task
uses the configured retry count and delay; it does not maintain separate retry or watermark state.
A non-zero CLI exit fails the Airflow task, and the existing atomicity/idempotency behavior makes a
retry safe.

Scheduling metadata is optional, so local CLI behavior does not change:

```yaml
orchestration:
  enabled: true
  schedule: "0 2 * * *"
  retries: 2
  retry_delay_minutes: 5
```

Only `enabled: true`, valid, approved configs become DAGs. Adding another scheduled dataset means
adding and approving one YAML file; no DAG Python is added or changed.

## Dockerized local stack

The pinned local stack uses Python 3.12, PostgreSQL 17.6, and Apache Airflow 3.1.7 with
LocalExecutor. It contains PostgreSQL, an optional CLI container, Airflow initialization, scheduler,
DAG processor, and API server. PostgreSQL and Airflow services have dependency/readiness checks.

Copy `.env.example` to `.env`, replace every placeholder, and keep `.env` uncommitted. Generate a
Fernet key and JWT secret with standard Python if needed:

```bash
python -c "import base64,secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
python -c "import secrets; print(secrets.token_urlsafe(48))"
git rev-parse HEAD
```

Put those values in `AIRFLOW_FERNET_KEY`, `AIRFLOW_JWT_SECRET`, and `ETL_GIT_COMMIT_SHA`. Use a
URL-safe local database password because it is interpolated into connection URLs. Then start the
stack:

```bash
docker compose build
docker compose up airflow-init
docker compose up -d postgres airflow-api-server airflow-scheduler airflow-dag-processor
docker compose ps
```

Airflow is available at `http://localhost:8080`. Its development Simple Auth Manager password is
generated in the Git-ignored `airflow/config` directory. Runnable configs are mounted read-only,
while raw data, upload/onboarding landing storage, drafts, and Airflow logs use host-mounted,
Git-ignored runtime directories.
The API writes `data/uploads`; Airflow sees the same files read-only by convention through its
existing `data` mount. Uploaded files are never served by Nginx. Manual containerized CLI
usage remains available through the tools profile:

```bash
docker compose --profile tools run --rm etl validate configs/customers.yaml
docker compose --profile tools run --rm etl run configs/customers.yaml
```

For an orchestration smoke test, list or manually test the generated DAGs. The portfolio order and
telemetry DAGs appear through config discovery; no DAG Python change is required:

```bash
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags test etl_customers 2026-09-07
docker compose exec airflow-scheduler airflow dags test etl_sensor_readings 2026-09-07
docker compose exec airflow-scheduler airflow dags test etl_portfolio_order_lines 2026-09-07
docker compose exec airflow-scheduler airflow dags test etl_portfolio_sensor_telemetry 2026-09-07
```

## Structured logging and failures

CLI execution emits machine-readable JSON to stderr and keeps command results on stdout. Events
include `CONFIG_VALIDATED`, `RUN_STARTED`, `SOURCE_EXTRACTED`, `RAW_PRESERVED`, `SCHEMA_CHECKED`,
`TRANSFORM_COMPLETED`, `QUALITY_COMPLETED`, `PUBLISH_COMPLETED`, `WATERMARK_ADVANCED`,
`RUN_SUCCEEDED`, and `RUN_FAILED`. Context includes timestamp, level, run ID, dataset, stage, source
type, load strategy, row counts, and duration where meaningful. Set `ETL_LOG_FORMAT=text` for a
compact human-readable local format.

Logs never replace the PostgreSQL ledger: logs provide execution detail, while the ledger remains
the durable operational record. Credential-like fields, configured secrets, and credentials inside
URLs are redacted before logging or failure persistence. Expected ETL failures retain meaningful
exception categories and return CLI exit code 2; unexpected safe-boundary failures return code 3.

## Continuous integration

`.github/workflows/ci.yml` runs on pull requests and pushes to `main` using Python 3.12 and a pinned
PostgreSQL service container. It installs the project, runs the complete unit/integration suite,
Ruff lint and formatting checks, validates every top-level runnable config, and compiles application
and DAG Python. CI uses isolated test credentials only and does not require production secrets.

## Backend observability

The observability layer summarizes the durable metadata already produced by ETL runs. It does not
control execution, parse log files, or copy business data:

```text
ETL framework
  -> etl_meta operational tables
  -> etl_observability SQL views
  -> read-only monitoring CLI
```

`PostgresStore.ensure_metadata_tables()` installs or refreshes the views during normal metadata
setup. Existing databases can be upgraded idempotently without deleting history:

```bash
etl observability-install
```

The PostgreSQL objects and their grains are:

| Object | Grain |
|---|---|
| `etl_meta.etl_run_ledger` | one row per ETL run |
| `etl_meta.data_quality_results` | one row per run and quality rule |
| `etl_meta.etl_quarantine` | one row per quarantined rule failure/detail |
| `etl_meta.schema_drift_history` | one row per schema-drift event |
| `etl_meta.etl_watermarks` | current watermark state per dataset |
| `etl_observability.pipeline_runs` | one row per ETL run |
| `etl_observability.dataset_health` | one row per dataset represented in metadata |
| `etl_observability.daily_pipeline_summary` | one row per run date and dataset |
| `etl_observability.quality_summary` | one row per run and quality rule |
| `etl_observability.quality_daily_trend` | one row per date, dataset, rule, and rule type |
| `etl_observability.quarantine_summary` | one aggregate row per date, dataset, rule, and rule type |
| `etl_observability.schema_drift_summary` | one row per schema-drift event |
| `etl_observability.watermark_status` | current watermark state per dataset |

### Health semantics

Run status and health status remain separate. `dataset_health` deterministically assigns:

- `FAILED` when the latest run failed.
- `WARNING` when the latest run succeeded but quarantined rows or `WARN` schema drift exist.
- `HEALTHY` when the latest run succeeded without those warning conditions.
- `UNKNOWN` when metadata identifies a dataset but no run exists, or the latest run is neither a
  completed success nor failure.

Quarantine is therefore a warning, not a pipeline failure. The view also exposes the latest run,
latest watermark, last successful timestamp, hours since success, and consecutive failures. No
dataset-specific thresholds or business assumptions are embedded in this logic.

### Monitoring CLI

Monitoring commands require `ETL_POSTGRES_DSN` and query the SQL views directly:

```bash
etl status
etl status --dataset customers
etl runs --limit 10
etl runs --dataset customers --limit 10
```

`etl status` provides compact current health across datasets. Dataset-specific status adds the
latest run ID/time, duration, row counts, drift, watermark, last success, and failure streak.
`etl runs` lists newest runs first. These commands perform only parameterized `SELECT` queries;
they cannot trigger ETL, change watermarks, retry runs, or modify load state.

### Performance, indexes, and privacy

`pipeline_runs` exposes per-run row counts, duration, and `rows_loaded_per_second`. Throughput is
`NULL` when duration is zero. The daily summary supplies success/failure counts, success rate, row
totals, average duration, and maximum duration. Airflow retry counts are not written to ETL
metadata, so retry analytics are intentionally unavailable.

Indexes are limited to observed monitoring access paths:

- `etl_run_ledger(dataset, started_at DESC)` supports latest-run and per-dataset trends.
- `etl_run_ledger(started_at DESC)` supports global recent-run queries.
- `data_quality_results(dataset, timestamp DESC)` supports quality trends.
- `etl_quarantine(dataset, quarantined_at DESC, rule_id)` supports quarantine aggregation.
- `schema_drift_history(dataset, detected_at DESC)` supports drift history.

Primary keys already cover run IDs and current watermark lookup, so no duplicate indexes are added.

`quarantine_summary` contains only aggregate failure and affected-record counts. It never exposes
failed values, masked values, hashes, record identifiers, failure reasons, source records, or
quarantine detail columns. No observability view includes credentials or environment secrets.

Example direct queries:

```sql
SELECT * FROM etl_observability.dataset_health ORDER BY dataset;
SELECT * FROM etl_observability.pipeline_runs ORDER BY started_at DESC LIMIT 10;
SELECT * FROM etl_observability.daily_pipeline_summary ORDER BY run_date DESC, dataset;
SELECT * FROM etl_observability.quality_summary WHERE records_failed > 0;
SELECT * FROM etl_observability.quarantine_summary ORDER BY date DESC, dataset;
SELECT * FROM etl_observability.schema_drift_summary ORDER BY detected_at DESC;
SELECT * FROM etl_observability.watermark_status ORDER BY dataset;
```

Durable metadata, structured logs, and observability have distinct roles: metadata stores
operational facts, logs support execution debugging, and views provide a stable monitoring model.

## Atomicity and reruns

Every run gets a unique staging table. Staging creation, row copy, trusted-table mutation, staging
cleanup, and—in incremental mode—watermark advancement happen transactionally. A failure rolls the
transaction back and leaves the existing trusted table and successful watermark unchanged. Schema
drift failure occurs before any target mutation, and only rows that pass quality contracts are
given to any load strategy.

## Tests

```bash
pytest
ruff check .
```

The tests cover configuration approval and safety, leading-zero preservation, byte-identical raw
copying, validation failures for every operator, execution of all five operators against unrelated
dataset shapes, profiler inference and statistics, exact duplicate counting, starter YAML output,
runtime blocking before review, contract validation/evaluation, privacy handling, quarantine
persistence, schema hash determinism, drift detection/policies/history, incremental first/later/no-
data runs, failure-safe watermarks and retries, business-key upserts, SCD2 history and rollback,
bounded backfills, flat/nested/NDJSON normalization, Parquet schemas, PostgreSQL source snapshots,
full rerun idempotency, trusted-row exclusion, orchestration discovery, CLI status propagation,
structured-log redaction, observability installation, health derivation, safe rates, privacy-safe
aggregates, monitoring CLI behavior, and row-count accounting. PostgreSQL integration tests are
enabled by `ETL_TEST_POSTGRES_DSN` and otherwise skip locally. A dedicated portfolio proof test
runs JSON orders, CSV students, and Parquet telemetry through the same `run_pipeline` entry point,
checks their expected transformation/quality outcomes, and verifies safe upsert and incremental
reruns.

## Repository map

```text
configs/customers.yaml              approved dataset metadata
configs/sensors.yaml                unrelated dataset using all Milestone 2 operators
configs/students_quality.yaml       contracts, privacy policies, and quality failures
configs/reliability_incremental.yaml incremental/watermark demo
configs/reliability_upsert.yaml     business-key merge demo
configs/schema_drift_*.yaml         raw added-column drift demo
configs/scd2_regions_*.yaml         two-run SCD2 history demo
configs/nested_orders.yaml          configured nested JSON explosion
configs/parquet_weather.yaml        Parquet source demo
configs/postgres_assets.yaml        table-based PostgreSQL source demo
configs/portfolio_orders.yaml       e-commerce JSON/upsert proof
configs/portfolio_sensor_telemetry.yaml IoT Parquet/incremental proof
configs/portfolio_schema_drift_*.yaml controlled drift proof
configs/drafts/                     unapproved profiler drafts; excluded from scheduling
data/incoming/customers.csv         example input
data/incoming/sensor_readings.csv   second example input
data/incoming/students.csv          profiling/onboarding example
data/incoming/student_quality.csv   quality/quarantine example
data/incoming/reliability_*.csv     incremental and upsert examples
data/incoming/schema_drift_*.csv    two-run schema drift example
data/incoming/scd2_regions_*.csv    changing SCD2 source snapshots
data/incoming/nested_orders.json    nested JSON source
data/incoming/weather_readings.parquet embedded-schema source
data/incoming/portfolio_orders.json nested e-commerce source
data/incoming/portfolio_sensor_telemetry.parquet IoT source
data/raw/                            run-scoped untouched copies (Git-ignored)
data/onboarding/                     new-dataset source artifacts (Git-ignored)
scripts/run_portfolio_demo.py       rerunnable public-CLI demonstration
docs/dataset_independence.md        three-domain proof matrix and evidence
docs/architecture.md                system flow, responsibilities, and principles
docs/portfolio_talking_points.md    concise interview walkthrough
docs/frontend_architecture.md       read-only UI/API boundary and future extension points
frontend/                           React, TypeScript, Vite, and Tailwind control center
src/metadata_etl_api/               FastAPI routes, models, settings, and read repository
Dockerfile.api                      pinned FastAPI runtime image
src/metadata_etl/onboarding/        profiler and starter YAML generator
src/metadata_etl/quality/           contract registry, evaluation, and privacy handling
src/metadata_etl/schema/            deterministic fingerprints and drift comparison
src/metadata_etl/connectors/        generic source connector registry
src/metadata_etl/orchestration.py   config discovery, validate-all, and CLI task execution
src/metadata_etl/structured_logging.py structured JSON/text logs and redaction
src/metadata_etl/observability.py   observability installation and read-only queries
src/metadata_etl/sql/observability.sql PostgreSQL monitoring schema, views, and indexes
src/metadata_etl/config.py          parsing and validation
src/metadata_etl/sql_compiler.py    generic SQL compilation
src/metadata_etl/transformations/   registry and reusable operators
src/metadata_etl/source.py          raw preservation
src/metadata_etl/postgres.py        ledger, drift/watermarks, and atomic load strategies
src/metadata_etl/pipeline.py        dataset-agnostic execution sequence
src/metadata_etl/cli.py             processing, validation, and monitoring commands
airflow/dags/generic_etl.py         one config-discovered DAG factory
Dockerfile                          pinned lightweight ETL CLI image
Dockerfile.airflow                  pinned Airflow image extended with the ETL package
docker-compose.yml                  local PostgreSQL and Airflow runtime
.github/workflows/ci.yml            pull-request and main-branch quality gate
sql/metadata_tables.sql             ledger DDL reference
tests/                              unit tests
```
