# Metadata-Driven ETL Framework

> The framework code is dataset-agnostic. New datasets are onboarded through profiled,
> human-approved, version-controlled configuration rather than new pipeline code.

**New dataset = new config, not new pipeline.**

This repository contains Milestones 1–5: the vertical ETL slice, generic transformation engine,
onboarding profiler, data-quality/quarantine layer, and reliability controls. A new CSV is profiled
into a starter YAML proposal, then an approved configuration drives raw preservation, schema
fingerprinting, drift policy, normalization, SQL transformations, quality evaluation,
privacy-safe quarantine, full/incremental/upsert publishing, and operational metadata. There is no
dataset-specific Python.

## Runtime flow

```text
customers.csv
  -> validate approved customers.yaml
  -> create run ID and RUNNING ledger entry
  -> preserve byte-identical raw copy
  -> fingerprint raw schema and apply drift policy
  -> normalize configured columns and types
  -> fingerprint and validate canonical schema
  -> validate operators through the transformation registry
  -> compile cast/filter/derive/map/deduplicate operators into DuckDB SQL
  -> apply the previous successful watermark for incremental runs
  -> evaluate not-null/unique/range/regex contracts
  -> write failed rows as privacy-protected quarantine details
  -> send only contract-passing rows forward
  -> load a run-scoped PostgreSQL staging table
  -> atomically full-publish, append, or merge into the trusted table
  -> advance the watermark only after successful publish
  -> record SUCCEEDED/FAILED metrics in the ledger
```

The engine reads and transforms the preserved raw artifact rather than rereading a potentially
changing incoming file. The config hash is SHA-256 over the exact YAML bytes. If the folder is a Git
repository, the current commit SHA is recorded; otherwise the ledger uses `UNAVAILABLE`.

## Implemented capabilities

- CSV sources with configurable delimiters
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
- centrally persisted successful watermarks in `etl_meta.etl_watermarks`
- insert/update counts and before/after watermarks in the run ledger
- idempotent full reruns, successful incremental reruns, and business-key upserts
- immutable-by-convention raw run directories and SHA-256 checksums
- run status, counts, timing, config identity, and errors in `etl_meta.etl_run_ledger`

Not implemented yet: SCD Type 2, JSON/Parquet/PostgreSQL sources, backfill orchestration, Airflow,
Power BI, or cloud services. Those belong to later milestones.

## Dataset onboarding flow

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
- The current source scope remains `source.type: csv`; load strategies are `full`, `incremental`,
  and `upsert`.
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
data runs, failure-safe watermarks and retries, business-key upserts, full rerun idempotency, trusted-
row exclusion, and row-count accounting. A live PostgreSQL instance is needed for the end-to-end
CLI verification, but not for these unit tests.

## Repository map

```text
configs/customers.yaml              approved dataset metadata
configs/sensors.yaml                unrelated dataset using all Milestone 2 operators
configs/students_quality.yaml       contracts, privacy policies, and quality failures
configs/reliability_incremental.yaml incremental/watermark demo
configs/reliability_upsert.yaml     business-key merge demo
configs/schema_drift_*.yaml         raw added-column drift demo
data/incoming/customers.csv         example input
data/incoming/sensor_readings.csv   second example input
data/incoming/students.csv          profiling/onboarding example
data/incoming/student_quality.csv   quality/quarantine example
data/incoming/reliability_*.csv     incremental and upsert examples
data/incoming/schema_drift_*.csv    two-run schema drift example
data/raw/                            run-scoped untouched copies (Git-ignored)
src/metadata_etl/onboarding/        profiler and starter YAML generator
src/metadata_etl/quality/           contract registry, evaluation, and privacy handling
src/metadata_etl/schema/            deterministic fingerprints and drift comparison
src/metadata_etl/config.py          parsing and validation
src/metadata_etl/sql_compiler.py    generic SQL compilation
src/metadata_etl/transformations/   registry and reusable operators
src/metadata_etl/source.py          raw preservation
src/metadata_etl/postgres.py        ledger, drift/watermarks, and atomic load strategies
src/metadata_etl/pipeline.py        dataset-agnostic execution sequence
src/metadata_etl/cli.py             etl validate / etl run
sql/metadata_tables.sql             ledger DDL reference
tests/                              unit tests
```
