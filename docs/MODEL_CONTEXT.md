> Current runtime: **0.7.3**. Read [the 0.7.3 changes](UPGRADE_0.7.3.md) before the historical 0.7.2 baseline below. Saved designs now support DELETE; lifecycle stop ownership and duplicate-stop behavior are fixed. Configuration and database schema remain compatible with 0.7.2.

# Model context: Central Patch Monitor 0.7.2

This is the quick reading guide. Read [the complete technical reference](TECHNICAL_REFERENCE_0.7.2.md) before making architectural or behavioral changes. The documented runtime baseline is clean commit `c2f557f`; documentation commits do not change runtime behavior.

## What the application is

One central, read-only Kubernetes/OpenShift observer. A FastAPI process polls configured cluster APIs, saves an immutable pre-patch baseline and latest successful state in SQLite, and exposes REST plus SSE for image/health monitoring. Remote clusters need credentials/RBAC, not this application's agents. Configuration and examples must remain organization-neutral.

## Read these files first

- `app/central.py`: lifecycle, HTTP routes, background tasks.
- `app/central_config.py`: authoritative configuration defaults and limits.
- `app/namespace_patterns.py`: comma-separated namespace glob grammar.
- `app/central_collect.py`: API pagination, owner resolution, normalized health/image rules.
- `app/central_store.py`: transactions, layers, comparison and retention.
- `app/central_logging.py`: safe structured operational logging.
- `tests/test_central.py`, `tests/test_ui_revision.py`, `tests/test_central_logging.py`: behavior checks.

## Do not assume

- `app.main`, `app.master`, root `openapi.json`, or root-level deployment manifests describe the central app. They belong to the older mode.
- A flow is an arbitrary DAG or Salt protocol. It is a configured fixed monitoring sequence.
- POST session creation initiates collection. The UI additionally calls baseline, which explains the apparent difference.
- Preview proves remote connectivity. It validates configuration-derived settings only.
- Running pod phase means a healthy container.
- A non-target tag is necessarily older than target.
- SSE replays events or transports table data. It repeats current revision/status notifications.
- Current rows retain every intermediate state. There is no events table.
- A successful scan is a single atomic observation across Kubernetes resources/clusters. Only database publication is atomic per cluster.
- A session list or schema describes all response contracts. The reference supplies additional semantics and limits.
- A same-origin check authenticates an API consumer. Ingress authentication is external.
- More Uvicorn workers or replicas are supported. Exactly one process/replica is required.
- Saved YAML scope is a hard boundary for API-created sessions. Use per-cluster regex and actual RBAC.
- Database retention means periodically wiping all data. It deletes eligible closed sessions, keeping saved designs and open sessions.

## Current consumer sequence

POST `/api/v1/flows/preview` (optional) → POST `/api/v1/sessions` → POST `/api/v1/sessions/{sid}/baseline` → poll until BASELINE_READY → POST `/api/v1/sessions/{sid}/start` → read summary/images/targets/changes or subscribe to stream → POST stop or allow expiry.

Keep session ID, revision, cluster freshness/error, and row_type in consumer state. Separate `version_status` from `health_change`; retain legacy `classification` only for compatibility. Coordinate lifecycle writes through one consumer and reconcile timeouts before retrying.

## Current storage model

SQLite tables: designs, sessions, clusters, rows. Rows have stage/current/baseline layers. JSON TEXT stores normalized payload/configuration; selected filter fields are separate SQL columns. Default path /data/patch.db, default closed-session retention 14 days, default page cap 512 MiB. No event archive, separate DB server, or automatic failover.

## Development evidence

The 0.7.2 implementation previously passed 38 unit/integration tests and synthetic HTTP/SSE plus DOM tests. These are not real-cluster, load, image-build, or visual tests. Documentation is checked against source and generated schemas; do not inflate that evidence.

For detailed endpoint inventory, settings, exact classifier precedence, known concurrency limitations, and proposed next steps, use the complete reference. This guide does not change permissions or authorize external actions.
