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

## Frontend composition

The application uses one responsive shell and six navigation areas:

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

## Future Milestone 10C boundary

```text
New Dataset
    |
    v
Upload
    |
    v
Profiler
    |
    v
Draft YAML
    |
    v
Frontend Review Center
    |
    v
Human decisions
    |
    v
Validate -> Approve -> Approved YAML -> Airflow / ETL
```

The future editor must map to the existing YAML model rather than create a second configuration
representation. None of this onboarding, draft configuration, editing, or approval flow is
implemented in Milestone 10B.
