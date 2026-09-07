# ETL Control Center architecture

## Current read-only boundary

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

FastAPI is intentionally separate from `metadata_etl`. The API does not import pipeline execution,
the CLI, Airflow, YAML configuration, connectors, or transformation code. Its repository opens
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
- Every operational API route is `GET`. Milestone 10A has no write or trigger endpoint.

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

## Read-only API surface

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

## Future Milestone 10B boundary

Milestone 10B may introduce a separately authorized operational flow:

```text
Existing dataset upload
        |
        v
FastAPI validation boundary
        |
        v
schema check / approved orchestration request
        |
        v
Airflow -> existing ETL CLI -> active-run monitoring
```

That future flow should add explicit write services, validation, audit, and authorization rather
than granting the current read repository mutation access.

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
representation. None of this onboarding or approval flow is implemented in Milestone 10A.
