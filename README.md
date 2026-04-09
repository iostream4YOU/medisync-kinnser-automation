# MediSync Kinnser Automation

Production-style automation pipeline for extracting patient data from Kinnser EHR, normalizing and enriching records, storing synchronized entities, and serving an operational dashboard.

## 1) What This Project Does

The pipeline supports two primary Kinnser workflows:

- Orders workflow: extracts patient orders and related PDFs.
- Patient profile workflow: extracts profile details first and can continue to documents in the same browser session.

For each run, the project can:

- Log in to Kinnser and navigate target workflows.
- Extract profile/order data and download PDF documents.
- Normalize and validate records into a canonical structure.
- Optionally enrich physician data with NPI Registry lookups.
- Sync to one of these targets:
  - Firebase Data Connect
  - Firestore Mongo-compatible endpoint
  - MediSync REST API
- Publish dashboard payloads and serve local/DB-backed document previews.

## 2) End-to-End Workflow

1. Load configuration and create runtime directories.
2. Start extraction:
   - Log in to Kinnser.
   - Execute selected workflow mode from config.
3. Persist extracted artifacts:
   - Excel/CSV exports.
   - Downloaded PDFs.
4. Normalize records in processing layer:
   - Field mapping.
   - Required field checks.
   - Record-level validation and dedupe rules.
5. Optional enrichment:
   - NPI data lookup and cache update.
6. Sync layer writes entities and run status.
7. Output normalized files, sync results, and run summaries.
8. Dashboard reads from DB or local artifacts with fallback behavior.

## 3) Core Components

| Component | File | Responsibility |
|---|---|---|
| Pipeline entrypoint | `main.py` | Orchestrates extraction, processing, enrichment, sync, and report outputs. |
| Selenium extraction | `src/extraction.py` | Kinnser login/navigation, profile/order harvesting, PDF capture, artifact writing. |
| Data normalization | `src/processing.py` | Converts exports into canonical `OrderRecord` items, validates fields, attaches PDF metadata. |
| NPI enrichment | `src/enrichment.py` | Uses NPPES API and local cache for physician enrichment. |
| DataConnect sync | `src/dataconnect_store.py` | Persists entities/run metadata via Data Connect operations and verification. |
| Firestore-Mongo sync | `src/firestore_store.py` | Persists entities/run metadata in Mongo-compatible collections. |
| MediSync API sync | `src/sync.py` | Upserts physicians/patients/episodes/orders/documents using REST endpoints. |
| Dashboard backend | `dashboard/app.py` | FastAPI backend for summary/patient/order/document APIs. |
| Dashboard DataConnect adapter | `dashboard/dataconnect_store.py` | Dashboard read-model integration with Data Connect. |
| Dashboard launcher | `run_dashboard.py` | Starts uvicorn-hosted dashboard app from project config. |
| Shared utilities | `src/utils.py` | Logging, JSON/config IO, date/string helpers, retry wrapper. |

## 4) Repository Layout

```text
Medisync/
  main.py
  run_dashboard.py
  requirements.txt
  config.example.json
  README.md
  dashboard/
    app.py
    dataconnect_store.py
    static/
      index.html
      style.css
      app.js
  src/
    extraction.py
    processing.py
    enrichment.py
    dataconnect_client.py
    dataconnect_store.py
    firestore_store.py
    sync.py
    models.py
    utils.py
  downloads/   (runtime, ignored)
  output/      (runtime, ignored)
  logs/        (runtime, ignored)
  cache/       (runtime, ignored)
  tmp/         (runtime, ignored)
```

## 5) Prerequisites

- Python 3.11+
- Chrome browser installed (for Selenium runs)
- macOS/Linux shell recommended
- Network access to:
  - Kinnser tenant URL
  - NPI Registry API (if enrichment enabled)
  - Chosen sync backend (Data Connect / Mongo / MediSync API)

## 6) Setup

From repository root:

1. Create and activate a virtual environment.
2. Install dependencies.
3. Create your local runtime config from template.

Commands:

- `cd Medisync`
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `pip install -r requirements.txt`
- `cp config.example.json config.json`

Then edit `config.json` with environment-specific values:

- `kinnser.login_url`, `kinnser.username`, `kinnser.password`
- `extraction.workflow_mode`
- `medisync.*` if API sync is enabled
- `firestore.*` or `dataconnect.*` if DB sync is enabled

## 7) Configuration Notes

### Workflow modes

- `orders`
- `patient_profiles`
- `patient_profiles_then_orders` (profiles first, then docs in same browser session)

### Sync modes

- Data Connect: set `dataconnect.enabled=true`
- Firestore Mongo-compatible: set `firestore.enabled=true` and supply URI/database
- MediSync API: set `medisync.sync_enabled=true`

### Environment overrides

- `MEDISYNC_CONFIG`
- `MEDISYNC_DASHBOARD_HOST`
- `MEDISYNC_DASHBOARD_PORT`
- `FIRESTORE_MONGO_URI`
- `FIRESTORE_MONGO_DATABASE`

## 8) Run Guide

### Dry run (recommended first)

- `python main.py --config config.json --dry-run`

This verifies extraction/normalization without backend writes.

### Full run

- `python main.py --config config.json`

### Reprocess existing files only

- `python main.py --config config.json --skip-extraction --excel-path ./downloads/patient_orders_xxx.xlsx --pdf-dir ./downloads/patient_documents`

### Limit run size

- `python main.py --config config.json --limit 50`

## 9) Dashboard

Start dashboard:

- `python run_dashboard.py`

or

- `python -m uvicorn dashboard.app:app --host 127.0.0.1 --port 8787`

Open:

- `http://127.0.0.1:8787`

Behavior:

- Reads DB-backed payload when configured and available.
- Falls back to local extracted artifacts when DB is unavailable.
- Serves local PDF previews/downloads for mapped order documents.

## 10) Output Artifacts

Created in `output/`:

- `normalized_orders_YYYYMMDD_HHMMSS.xlsx`
- `normalized_patient_profiles_YYYYMMDD_HHMMSS.xlsx`
- `sync_results_YYYYMMDD_HHMMSS.xlsx`
- `sync_results_profiles_YYYYMMDD_HHMMSS.xlsx`
- `run_summary_YYYYMMDD_HHMMSS.json`

Created in `downloads/`:

- Patient/order exports from Kinnser
- Patient profile artifacts (`patient_profiles_data/*.json|*.html`)
- Downloaded PDFs (`patient_documents/*.pdf`)

## 11) Data Model Summary

Primary logical entities synchronized by DB stores:

- Agency
- User
- Patient
- Physician
- ImportRun
- Order
- OrderDocument
- OrderRawRow

## 12) Troubleshooting

### Dashboard does not start

- Verify dependencies: `pip install -r requirements.txt`
- Confirm port availability: `lsof -i :8787`

### No records extracted

- Check selectors under `kinnser.selectors` in config.
- Increase wait/timeout values in `extraction` section.

### DB sync failures

- Confirm DB/Data Connect settings and network access.
- Run with `--dry-run` to validate upstream extraction first.

### Missing PDF previews in dashboard

- Confirm `pdf_path` values exist and point inside allowed project/workspace roots.
- Verify PDFs exist in `downloads/patient_documents`.

## 13) Security and Git Hygiene

- Do not commit real credentials.
- Keep `config.json` local only.
- Use `config.example.json` as a template for sharing.
- Runtime folders (`downloads`, `output`, `logs`, `cache`, `tmp`) are intentionally excluded from version control.

## 14) Recommended Demo Flow

1. Run dry-run profile+orders mode for initials `AB`.
2. Show generated normalized outputs and run summary.
3. Start dashboard and show patient cards and order documents.
4. Open a document via dashboard preview endpoint.
5. Show fallback continuity when backend is unavailable.
