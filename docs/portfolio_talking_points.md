# Portfolio talking points

## Thirty-second summary

I built a metadata-driven ETL framework in Python, DuckDB, PostgreSQL, YAML, Docker, and Airflow.
Instead of creating one pipeline per dataset, an approved YAML config selects a generic connector,
SQL transformations, quality rules, drift policy, and loading strategy. I proved the design with
unrelated e-commerce JSON, higher-education CSV, and IoT Parquet datasets, without adding
dataset-specific engine or DAG code.

## What the demonstration proves

- **Dataset independence:** three domains and three file shapes run through one pipeline entry
  point and the same registries.
- **Governed onboarding:** profiling creates proposals; unapproved or ambiguous decisions cannot
  validate or run.
- **Scalable transformations:** generic operators are validated and compiled into DuckDB SQL.
- **Trust boundaries:** contracts run after transformation, failed rows are excluded, and
  quarantine values follow explicit privacy policies.
- **Reliable publication:** staging and transactions protect trusted tables; full, incremental,
  upsert, backfill, and SCD2 reruns have defined idempotency behavior.
- **Auditability:** every run records config/Git identity, hashes, row counts, timing, quality,
  drift, and watermark evidence.
- **Thin orchestration:** Airflow discovers approved configs and calls the same CLI used locally.

## Project questions

### What problem does this solve?

It removes repeated pipeline scaffolding when datasets differ only in source binding, structure,
transformation rules, quality expectations, and publication policy. Those differences become
reviewable metadata while the execution machinery stays shared.

### How is it metadata-driven, and what happens when a new dataset arrives?

A YAML contract selects the source connector, canonical schema, operator sequence, quality rules,
drift actions, privacy policy, load strategy, and optional schedule. A new source is profiled into a
starter proposal, reviewed by a domain owner, validated in CI, and then executed by the same CLI.

### Why not write one Python script per dataset?

Per-dataset scripts duplicate error handling, loading, audit, and orchestration behavior and make
standards harder to enforce. A registry-based engine centralizes those mechanics while keeping
domain choices visible in version-controlled configuration.

### What does the profiler automate, and what remains human-controlled?

It reports names, candidate types, nulls, cardinality, duplicates, ranges, lengths, date patterns,
leading zeros, and possible keys. Humans still approve identifiers, ambiguous dates, null tokens,
deduplication, mappings, business keys, contracts, privacy, drift, and load policy.

### How is bad data kept out of trusted tables?

Contracts evaluate transformed rows before staging. Failed rows are removed from every load path,
rule summaries are persisted, and quarantine stores only configured failure evidence—not complete
source records.

### How do incremental loads remain reliable?

Only values newer than the last successful watermark are selected. Staging publication and
watermark advancement are transactional, so a failure retains the previous watermark for retry.
Backfills replace only their bounded range and never alter the production watermark.

### What is the difference between upsert and SCD2?

Upsert keeps one current row per business key and updates it in place. SCD2 expires the old current
version and inserts a new dated version when configured tracked attributes change, preserving
history.

### What happens when a schema changes?

Deterministic raw and canonical fingerprints are compared with the prior successful run. The
configured policy records and allows, warns, or fails each drift event before trusted publication.

### What does Airflow do?

It discovers approved, scheduling-enabled configs and provides scheduling, dependencies, retries,
and failure handling. Its tasks call the existing validation and run CLI; they do not transform
data themselves.

### How is the framework monitored?

The ledger and related PostgreSQL tables retain runs, counts, durations, quality, quarantine,
schema drift, and watermarks. Read-only views power `etl status` and `etl runs`; Power BI remains a
future presentation layer.

## Useful interview walkthrough

1. Start with the proof matrix in `docs/dataset_independence.md`.
2. Compare the three YAML files and point out that their differences are metadata, not Python.
3. Run `python scripts/run_portfolio_demo.py` and show trusted counts plus warning/healthy states.
4. Open `docs/architecture.md` to explain raw preservation, canonical SQL, quarantine, staging,
   atomic publication, and observability.
5. Show the test that runs all three domains through the same `run_pipeline` function and verifies
   their expected outcomes.

## Tradeoffs and intentional limits

- YAML expressions are constrained to supported, validated operators; arbitrary Python is not
  allowed.
- The local stack prioritizes reproducibility over cloud scale. AWS, Spark, Kafka, dbt, and
  Kubernetes are intentionally outside this project stage.
- Airflow handles scheduling and retries, while the ETL package owns processing. This keeps local
  and orchestrated behavior consistent.
- Operational views are ready for a future Power BI layer, but this milestone proves backend
  observability rather than building a dashboard.

## One-line message

> New dataset = new config, not new pipeline.
