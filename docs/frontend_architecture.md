# ETL Control Center architecture

## Milestone 10A: read-only monitoring

```text
React + TypeScript browser application
                  |
                  v
             FastAPI API
                  |
                  v
      etl_observability SQL views
                  |
                  v
       existing ETL metadata tables
```

The browser never receives PostgreSQL credentials and never opens a database connection. It calls
the FastAPI application using `VITE_API_BASE_URL`; the API obtains its database connection from
`ETL_POSTGRES_DSN`. In Docker, Nginx proxies same-origin `/api` requests to FastAPI.

The monitoring repository is intentionally separate from `metadata_etl`. Its repository opens
read-only PostgreSQL transactions and uses parameterized values for dataset and run identifiers.
Existing observability views remain authoritative for health, run, quality, drift, and watermark
meaning.

## Privacy and failure boundaries

- The API queries `etl_observability.quality_summary` and aggregate observability data, never the
  row-level quarantine table.
- Responses contain no failed values, record identifiers, raw records, source paths, environment
  variables, DSNs, or credentials.
- Schema hashes, config hashes, and Git SHAs are exposed only on the technical run-detail view for
  reproducibility.
- Database failures become a generic unavailable response without exception or connection detail.
- CORS allows only origins listed in `ETL_API_CORS_ORIGINS`; it does not use a wildcard.
- The original 10A routes remain read-only and backward compatible.

## Milestone 10B: existing-dataset operations

```text
React controls
  -> FastAPI validates and stores an upload session
  -> Airflow 3 stable /api/v2 triggers the existing generic DAG
  -> the DAG calls the existing CLI with a source override and correlation ID
  -> the ETL engine preserves raw data, transforms, checks quality, and publishes
  -> observability returns the exact correlated ETL run
```

The API imports only framework configuration and non-destructive validation components for
preflight. It never performs extraction, business transformations, quality handling, loading,
watermark movement, or SCD2 behavior itself.

### Landing, raw, and trusted boundaries

- `data/uploads/<dataset>/<upload_id>/artifact.<ext>` is server-controlled landing storage.
- The client filename is metadata only and never becomes a path.
- A real ETL execution still creates its normal immutable raw copy under `data/raw`.
- Trusted tables change only through the existing atomic ETL load path.

### Preflight boundary

Preflight reuses approved config loading, connectors, schema fingerprinting, drift comparison, and
SQL plan binding inside a temporary workspace. It does not write trusted data, quarantine,
watermarks, ledger rows, or drift history. `READY`, `WARNING`, and `BLOCKED` are previews; the real
run independently enforces schema drift again.

### Application state and correlation

`etl_app.upload_sessions` stores application operation state separately from ETL truth and dataset
health. The deterministic state progression is:

```text
UPLOADED -> READY | WARNING | BLOCKED
READY | WARNING -> TRIGGERING -> QUEUED -> RUNNING -> SUCCEEDED | FAILED
```

Each upload UUID is also a unique nullable `correlation_id` on the ETL ledger. FastAPI passes it in
Airflow DAG-run configuration and the DAG passes it to the CLI. Monitoring queries that exact ID;
it never guesses from the latest dataset run. A deterministic Airflow run ID and transactional
trigger claim make repeated Run requests idempotent.

### Security boundary

`ETL_CONTROL_OPERATIONS_ENABLED` defaults to false. Upload and action endpoints return a safe
rejection while disabled; all 10A GET pages continue to work. Airflow URL, token or username and
password remain server-side. CORS stays allow-listed. This gate is for trusted/local development,
not a replacement for authentication or authorization.

## Milestone 10C.1: profile and review

```text
React onboarding flow
  -> FastAPI onboarding application layer
  -> existing profiler and starter-config generator
  -> configs/drafts/<onboarding_id>.yaml
  -> human-readable Review Center
  -> validated schema decisions written back to the same YAML
```

`etl_app.onboarding_sessions` is separate from operational upload sessions and ETL run truth. Its
states stop at `REVIEW_COMPLETE`; there is no approved, running, or successful state in 10C.1.
Source bytes land under `data/onboarding/<onboarding_id>` using a server-generated UUID. Client
filenames are metadata only, and API responses expose neither landing nor draft filesystem paths.

`ETL_CONTROL_ONBOARDING_ENABLED` defaults to false independently of the 10B operations gate. When
disabled, write endpoints return 403 and the navigation action is hidden. This gate is intended for
local/trusted environments and is not a substitute for authentication or RBAC.

The existing profiler is extended through format readers for CSV, JSON, and Parquet; every source
feeds the same inference logic. Nested JSON is described and marked unresolved. The application
does not guess relational normalization. Generated YAML is canonical and remains top-level
`approved: false`; the API reloads and validates it after schema edits. Key-candidate accept/reject
decisions are onboarding review metadata only and do not create load keys.

The Review Center provides Overview, Review Required, Schema, key-candidate decisions, and a
read-only view of the actual persisted YAML. It allows canonical name, supported datatype,
nullable, and supported date/timestamp format decisions. A browser refresh reloads the session and
draft from PostgreSQL and disk rather than relying on React memory.

The profiler proposes. The human approves. The engine executes only approved configuration.

## Frontend composition

The application uses one responsive shell and six permanent monitoring areas, plus the gated
Onboard Dataset action:

1. **Overview:** 24-hour run metrics, current dataset health, and recent runs.
2. **Datasets:** searchable/filterable inventory and dataset detail.
3. **Runs:** global history and row-flow/technical run detail.
4. **Data Quality:** 30-day totals, trend, and aggregate breakdowns.
5. **Schema Drift:** raw/canonical explanations and policy/action history.
6. **Watermarks:** current successfully published incremental positions.

API response interfaces live in one TypeScript module, requests are centralized in one client, and
pages share loading, empty, error, status, table, metric, and detail components. Future actions can
add separate API methods and UI routes without changing these read-only contracts.

## API surface

| Endpoint | Purpose |
| --- | --- |
| `GET /api/health` | application liveness |
| `GET /api/overview` | current dataset counts and 24-hour run totals |
| `GET /api/datasets` | one current health row per dataset |
| `GET /api/datasets/{dataset}` | current operational dataset detail |
| `GET /api/datasets/{dataset}/runs` | paged recent dataset runs |
| `GET /api/datasets/{dataset}/quality` | recent rule summaries without quarantine values |
| `GET /api/datasets/{dataset}/schema-drift` | recent raw/canonical drift events |
| `GET /api/datasets/{dataset}/watermark` | current incremental state or a clean empty result |
| `GET /api/runs` | filterable global run history |
| `GET /api/runs/{run_id}` | complete recorded run metadata |
| `GET /api/runs/{run_id}/quality` | rule summaries for one run |
| `GET /api/quality` | 30-day quality totals, breakdowns, and daily trend |
| `GET /api/schema-drift` | global drift history |
| `GET /api/watermarks` | all active watermarks |
| `GET /api/datasets/{dataset}/upload-capability` | approved source and operation eligibility |
| `POST /api/datasets/{dataset}/uploads` | stream a file into controlled landing storage |
| `POST /api/uploads/{upload_id}/validate` | run non-destructive preflight |
| `POST /api/uploads/{upload_id}/run` | idempotently request the generic Airflow DAG |
| `GET /api/uploads/{upload_id}` | monitor application, Airflow, and exact ETL state |
| `GET /api/onboarding/capability` | onboarding gate and supported file/type choices |
| `POST /api/onboarding` | create a new-dataset session and safely land one file |
| `POST /api/onboarding/{id}/profile` | run the existing profiler and generate draft YAML |
| `GET /api/onboarding/{id}` | resume durable onboarding state |
| `GET /api/onboarding/{id}/review` | structured view of the authoritative draft |
| `PATCH /api/onboarding/{id}/schema/{field}` | validate and persist one schema decision |
| `POST /api/onboarding/{id}/key-decisions` | persist a non-semantic key suggestion decision |
| `GET /api/onboarding/{id}/yaml` | return only that session's actual draft content |

## Local development

Start PostgreSQL using the existing local configuration, then run the API:

```powershell
$env:ETL_POSTGRES_DSN = "postgresql://etl:YOUR_PASSWORD@localhost:5432/etl"
$env:ETL_API_CORS_ORIGINS = "http://localhost:5173"
python -m uvicorn metadata_etl_api.main:app --reload --port 8000
```

In another terminal, start the frontend:

```powershell
cd frontend
Copy-Item .env.example .env.local
npm install
npm run dev
```

The frontend is available at `http://localhost:5173`; API documentation is available locally at
`http://localhost:8000/docs`.

For the integrated stack, configure root `.env` and run:

```powershell
docker compose up -d --build
```

The Docker frontend is available at `http://localhost:4173` and FastAPI at
`http://localhost:8000/api/health`.

## Milestone 10C.2A: configuration and pre-approval validation

```text
Reviewed draft -> configure transformations, contracts, load and drift policy
               -> validate through the existing config model and execution planner
               -> bind validation to the exact draft SHA-256
               -> READY_FOR_APPROVAL while review.approved remains false
```

The configuration builder is a structured interface over the existing YAML model. Transformation
edits use the current `cast`, `filter`, `derive`, `map`, and `deduplicate` registry; contracts use
the current `not_null`, `unique`, `range`, and `regex` registry. Contracts validate against the
post-transformation schema. JSON fields and one array explosion are expressed through the existing
normalization structure and are tested against the landed source. Load choices use only full,
incremental, upsert, and SCD2. Accepted profiler key candidates are displayed as hints and are never
applied automatically.

Each structured request writes a temporary YAML candidate, runs existing model and plan validation,
and atomically replaces the draft only when valid. The database stores workflow state and the hash,
result, errors, and time of final validation; it does not store a competing configuration model.
Any subsequent schema, normalization, transformation, contract, load, privacy, or drift edit clears
that validation identity and returns the session to configuration.

Additional onboarding endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /api/onboarding/{id}/configuration` | reload the structured view from actual draft YAML |
| `PATCH /api/onboarding/{id}/normalization` | persist explicit existing-model JSON normalization |
| `POST/PATCH/DELETE /api/onboarding/{id}/transformations` | manage registry-backed operators |
| `POST /api/onboarding/{id}/transformations/{rule_id}/move` | change YAML execution order |
| `POST/PATCH/DELETE /api/onboarding/{id}/contracts` | manage post-transformation contracts |
| `PATCH /api/onboarding/{id}/columns/{field}/privacy` | persist existing classification/quarantine policy |
| `PATCH /api/onboarding/{id}/load` | select and validate an existing load strategy |
| `PATCH /api/onboarding/{id}/schema-drift` | persist current drift keys and actions |
| `POST /api/onboarding/{id}/validate` | validate without approving or running |

## Milestone 10C.2B: approval, activation, and first run

```text
READY_FOR_APPROVAL + expected SHA-256 + acknowledgment + approved_by
  -> PostgreSQL approval claim/lock
  -> stable ignored source materialization
  -> strict approved-config validation
  -> atomic configs/<dataset>.yaml promotion
  -> restricted one-file Git commit
  -> generic Airflow DAG discovery
  -> READY_FOR_FIRST_RUN
  -> explicit trigger with exact correlation ID
  -> ledger-backed completion
```

Validation identity and approved-config identity are intentionally separate because adding the
approval metadata changes YAML bytes. PostgreSQL records both hashes, the approver and timestamp,
Git SHA/push result, DAG ID, first-run correlation and Airflow IDs, ETL run ID, and completion
state. The top-level approved config becomes canonical and the ignored draft is archived. Every
draft-edit endpoint becomes read-only after approval.

The Git adapter has a fixed repository root and executes argument arrays with `shell=False`. It
requires a clean project tree and stages only the exact top-level config. Its deterministic commit
message is `Approve dataset configuration: <dataset>`. Push is independently gated off by default
and, if enabled, is restricted to the configured remote and branch. No browser-provided Git
command, path, remote, or branch is accepted.

Approved configs use the existing discovery helper and `etl_<dataset>` naming function. A null
schedule means on-demand activation; it does not create a recurring schedule. Discovery timeout
leaves the config approved and exposes a retry. Approval never starts ETL. The first-run trigger
uses the existing Airflow client and generic CLI path, including the approval commit SHA and one
exact correlation identifier. Status resolution queries that correlation identifier rather than
guessing the latest dataset run. First-run failure preserves approval and allows an explicit retry.

Additional endpoints:

| Endpoint | Purpose |
| --- | --- |
| `POST /api/onboarding/{id}/approve` | hash-bound human approval, promotion, and versioning |
| `POST /api/onboarding/{id}/activate` | retry exact generic-DAG discovery |
| `POST /api/onboarding/{id}/first-run` | explicitly trigger or retry the first run |
| `GET /api/onboarding/{id}/completion` | resume exact approval, Airflow, and ETL state |
