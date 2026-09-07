# Architecture

## Dataset onboarding

```text
New source
    |
    v
Profiler -> starter YAML proposal -> human review and approval -> Git -> CI validation
```

The profiler automates bounded structural observations and suggestions. It never grants approval or
silently chooses an ambiguous date, semantic key, duplicate policy, or JSON structure. The approved
YAML is the executable contract and its exact hash and repository SHA identify each run.

## Runtime

```text
Airflow -> same generic CLI -> connector -> raw preservation
                                      |
                                      v
                         raw schema fingerprint / drift
                                      |
                                      v
                        structural normalization
                                      |
                                      v
                  canonical schema / transformation registry
                                      |
                                      v
              quality contracts -----+-----> failures -> quarantine
                                      |
                                 passing rows
                                      |
                                      v
                    full / incremental / upsert / SCD2
                                      |
                                      v
                       atomic PostgreSQL publish
                                      |
                                      v
                         watermark / metadata update
```

## Observability

```text
run ledger + quality metadata + quarantine aggregates + schema drift + watermarks
                                      |
                                      v
                         etl_observability views
                                      |
                                      v
                          etl status / etl runs
```

## Component responsibilities

| Component | Responsibility | Does not decide |
| --- | --- | --- |
| YAML configuration | source binding, canonical columns, approved transformations, contracts, drift and load policies | runtime implementation |
| Profiler | bounded structural statistics and starter-config proposals | approval or business semantics |
| Config loader/validator | syntax, approval gates, references, supported operator semantics | business meaning |
| Connector registry | extract CSV, JSON, Parquet, or PostgreSQL and preserve raw input | dataset-specific parsing guesses |
| Normalization/compiler | apply explicit structure and compile generic operators into DuckDB SQL | ambiguous dates, keys, deduplication, or JSON flattening |
| Contract engine | evaluate not-null, unique, range, and regex rules and split valid/failed rows | unconfigured quality expectations |
| PostgreSQL publisher | stage and atomically execute full, incremental, upsert, backfill, or SCD2 behavior | which strategy a dataset needs |
| Metadata layer | persist run, schema, quality, quarantine, and watermark evidence | business-data analytics |
| Airflow | discover enabled configs and call the same validation/run CLI | ETL business logic |
| PostgreSQL | trusted publication, durable state, and operational metadata | transformation semantics |

## Configuration-driven execution

The framework has a small number of generic dispatch boundaries. Registries map declared source,
transformation, and contract types to reusable implementations. The load strategy is also selected
from YAML. Runtime code never dispatches on a dataset name.

This separates structural mechanics from domain semantics. The profiler may propose a starter
config, but only approved configuration is executable. Ambiguous dates, identifiers with leading
zeros, duplicate handling, nested JSON paths, privacy treatment, and business keys remain explicit
human decisions.

## Data layers

- **Incoming:** supplied file or configured PostgreSQL table.
- **Raw:** untouched file copy or deterministic database snapshot, partitioned by run.
- **Canonical/transformed:** DuckDB relation produced from approved normalization and operators.
- **Quarantine:** rule-level failure metadata with configured full, masked, hashed, or omitted
  values; complete source records are not copied blindly.
- **Staging:** run-scoped PostgreSQL table prepared before publication.
- **Trusted:** atomically published records that passed all contracts.
- **Metadata/observability:** ledger, quality, drift, quarantine, and watermark history plus
  read-only operational views.

## Reliability model

- Raw and canonical schema hashes are deterministic and compared with the prior successful run.
- Drift actions are explicit (`allow`, `warn`, or `fail`) and events are retained.
- A failed run cannot partially replace trusted output because publishing is transactional.
- Incremental watermarks advance in the same successful publish transaction and never during a
  bounded backfill.
- Upserts use configured business keys; SCD2 uses configured keys, tracked columns, and effective
  time.
- Contract-failing rows are removed before every loading strategy.
- Run identity includes the config schema version, exact config hash, and Git commit SHA.

## Orchestration and operations

Airflow scans approved top-level configs and creates one DAG per config that explicitly enables
orchestration. DAG tasks validate and invoke the existing CLI; they do not reimplement processing.
Adding either portfolio config therefore creates a schedulable dataset without editing DAG code.

Structured logs and PostgreSQL observability views expose health, recent runs, row-count trends,
quality failures, drift history, and watermark state. Power BI or another reporting layer can use
these backend views later without changing the ETL engine.

## Design principles

1. **New dataset = new config, not new pipeline.**
2. **The profiler proposes. The human approves. The engine executes approved configuration.**
3. **Raw preservation is immutable evidence, not permission to guess semantics.**
4. **Structural normalization is separate from semantic interpretation.**
5. **Transformations intentionally change data; contracts judge whether the result is acceptable.**
6. **Failed quality rows are quarantined rather than silently discarded.**
7. **Watermarks advance only after successful publication.**
8. **Airflow orchestrates; the ETL framework processes.**
9. **Operational metadata is durable state and remains separate from structured execution logs.**
