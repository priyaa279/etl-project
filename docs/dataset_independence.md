# Dataset-independence proof

The framework code is dataset-agnostic. New datasets are onboarded through profiled,
human-approved, version-controlled configuration rather than new pipeline code.

Milestone 9 proves that claim with three unrelated domains. Each example uses the same CLI,
connector registry, normalization layer, transformation registry, contract engine, publishing
layer, ledger, and Airflow DAG factory. The only domain-specific assets are input data and YAML.

## Proof matrix

| Domain | Config | Source | Transformations | Quality contracts | Load strategy | Fresh-run proof |
| --- | --- | --- | --- | --- | --- | --- |
| E-commerce order lines | `configs/portfolio_orders.yaml` | Nested JSON | cast, map, filter, derive, explicit array explosion | not-null, unique, range | upsert | 4 extracted, 3 transformed, 1 quarantined, 2 loaded |
| Higher-education students | `configs/students_quality.yaml` | CSV | derive | not-null, unique, range, regex | full | 5 extracted, 5 transformed, 4 quarantined, 1 loaded |
| IoT device telemetry | `configs/portfolio_sensor_telemetry.yaml` | Parquet | cast, map, filter, deduplicate, derive | not-null, unique, range | incremental | 5 extracted, 3 transformed, 1 quarantined, 2 loaded |

The examples deliberately vary their shapes and policies. JSON is flattened and exploded only
where the e-commerce config explicitly requests it. Student identifiers such as `00123` remain
strings. Parquet supplies the physical IoT schema, while the YAML still defines the canonical
contract and processing rules.

## Onboarding proof

The higher-education source can be onboarded from scratch with:

```bash
etl profile data/incoming/students.csv --output configs/drafts/students.yaml
```

The observed proposal identifies leading-zero student IDs as strings, recognizes the unambiguous
ISO enrollment date, leaves the day/month birth-date format unset, reports possible null tokens,
and marks ambiguous decisions for review. The generated top-level approval is false, so both
validation and runtime remain blocked until a human completes and approves the proposal. Profiling
reports duplicate evidence but never inserts a deduplication transformation.

## Reproducible demonstration

Start the local stack and set the destination connection:

```powershell
docker compose up -d
$env:ETL_POSTGRES_DSN = "postgresql://etl:etl@localhost:5432/etl"
python scripts/run_portfolio_demo.py
```

The script first validates every runnable config. It then invokes only the public CLI to run and
rerun all three domains. It also exercises SCD2 change detection, a bounded backfill twice, and a
controlled schema-drift sequence before displaying trusted counts and the read-only observability
commands. It contains orchestration and evidence queries, not ETL business logic.

The script is safe to rerun:

- The order-line upsert retains one trusted row per configured business key.
- The student full load atomically replaces the target with the same valid result.
- The IoT incremental load sees no rows newer than its successful watermark.
- The unchanged SCD2 rerun creates no extra history version.
- Repeating the same bounded backfill replaces that range and leaves the production watermark
  unchanged.

## Reliability evidence

| Behavior | Demonstration | Expected result |
| --- | --- | --- |
| Full-load idempotency | rerun `students_quality.yaml` | one trusted valid row; no failed row published |
| Upsert idempotency | rerun `portfolio_orders.yaml` | two trusted order lines; existing keys updated, not duplicated |
| Incremental no-new-data | rerun `portfolio_sensor_telemetry.yaml` | zero rows extracted and loaded on the repeat |
| SCD2 idempotency | run SCD2 baseline, change, then unchanged change config | one current version per key; unchanged rerun adds no version |
| Backfill idempotency | run the same bounded reliability backfill twice | historical range replaced safely; production watermark unchanged |
| Schema drift | run `portfolio_schema_drift_base.yaml`, then `_added.yaml` | added source column recorded with the configured warning action |
| Quality isolation | inspect the three domain runs | invalid rows appear in summaries/quarantine and never in trusted tables |

## No new pipeline code

There is one generic dispatch path for each extensibility point:

- `source.type` selects a connector from the connector registry.
- transformation `type` selects an operator from the transformation registry.
- contract `type` selects a rule from the contract registry.
- `load.strategy` selects the shared transactional publisher.
- Airflow discovers approved configs with `orchestration.enabled: true` and creates DAGs through
  one generic factory.

There are no conditions on order, student, customer, sensor, or other dataset names under
`src/metadata_etl`. Adding another dataset therefore requires data plus an approved config, not a
new Python pipeline or DAG.

## Observability proof

Every demonstration run records source/load types, status, row counts, duration, config identity,
schema hashes, drift status, and watermark state in the same ledger. Quality summaries,
privacy-safe quarantine details, and schema-drift events use the same metadata tables. The
`etl status` and `etl runs` commands read the backend observability views, so operational health
can be compared across domains without querying business tables.
