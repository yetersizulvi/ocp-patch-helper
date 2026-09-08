# Cluster and collector logs — 0.7.1

The central application emits one JSON object per line to stdout (`logger=patch.monitor`). Read it with `oc logs`; Uvicorn access logs may be interleaved. This operational mode is independent of Uvicorn `--log-level`.

## Enable

Requires the 0.7.1 image. `PATCH_LOG_LEVEL` is `INFO` by default; valid values are DEBUG, INFO, WARNING, ERROR. `PATCH_LOG_STATE_INTERVAL_SECONDS` defaults to 60 and accepts 5–3600 seconds. Environment changes restart the pod: stop an active session first, then explicitly resume/create a session after rollout.

```bash
oc set env deployment/patch-monitor -n patch-monitor PATCH_LOG_LEVEL=DEBUG PATCH_LOG_STATE_INTERVAL_SECONDS=30
oc rollout status deployment/patch-monitor -n patch-monitor --timeout=180s
oc logs -f deployment/patch-monitor -n patch-monitor --since=10m
```

Filter one cluster if `grep` is available on the bastion:

```bash
oc logs -f deployment/patch-monitor -n patch-monitor --since=10m | grep --line-buffered '"cluster":"cluster-remote"'
```

Return to normal operational detail:

```bash
oc set env deployment/patch-monitor -n patch-monitor PATCH_LOG_LEVEL=INFO PATCH_LOG_STATE_INTERVAL_SECONDS=60
```

## Read the events

| Event | Level | Meaning |
|---|---|---|
| monitor_started | INFO | Application and selected log level loaded |
| cluster_configured | INFO | Connection is configured, **not checked** |
| cluster_state | INFO | Periodic state: IDLE/CAPTURING/RUNNING/STOPPED, whether a scan is currently in flight, last successful scan age, last error |
| scan_started | INFO | A baseline/live scan started; cluster, session_id and unique scan_id correlate logs |
| api_connected | INFO | First valid HTTP 200 resource-list page in this scan was received, using configured TLS/token policy |
| api_request_started | DEBUG | GET attempted for this namespace/resource path |
| api_page_received | DEBUG | GET returned a valid page; status, item count and duration |
| namespace_scope_selected | DEBUG | Number of namespaces matching administrator + session scope |
| scan_completed | INFO | Entire scan committed: rows, target_rows, unhealthy_rows, pages, objects, duration_ms, published=true |
| api_request_failed | ERROR | Non-200 response with path/namespace/resource and HTTP status |
| scan_failed | ERROR | Failure with last attempted API path, safe error code/type, processed row/page counts; published=false |
| scan_cancelled | INFO | STOP/expiry/shutdown cancelled collection |
| scan_retry_scheduled | WARNING | Failed live scan will retry after retry_seconds; failure count shown |
| scan_scheduled | DEBUG | Successful live scan's next scheduled delay |
| baseline_finished | INFO | BASELINE_READY or DRAFT if not all selected clusters succeeded |
| session_started / session_stopped / session_expired | INFO | Lifecycle transitions |
| config_reloaded | INFO | New validated config version loaded |
| config_or_retention_failed | ERROR | Config reload or retention failed; prior valid config remains |

`api_connected` is evidence for one successful API page, not proof that every resource is authorized. `scan_completed` with `published=true` means all configured collection steps completed. `cluster_state.connection=LAST_SCAN_SUCCEEDED` records historical evidence and has an age; it is **not** an active connection probe. `connection=SCAN_CANCELLED` means the scan was interrupted intentionally; it is not a network failure. `scanning=false` between periodic scans is normal even in RUNNING state. In IDLE there are no API calls until baseline/start; configured endpoints are never labelled connected without an actual successful read. Demo scans carry demo=true and never emit api_connected.

Errors include API_HTTP_401 (token rejected/expired), API_HTTP_403 (RBAC denied), API_TIMEOUT, TLS_ERROR, CONNECTION_ERROR, TOKEN_FILE_UNREADABLE, EMPTY_TOKEN, CA_FILE_UNREADABLE, INVALID_API_RESPONSE, scan/page/object/row limits and storage failures. Generic CONNECTION_ERROR does not claim to distinguish DNS from firewall or connection reset. A failed scan does not publish partial results; last committed data remains in SQLite.

DEBUG can generate many lines across namespaces/pages. INFO retains lifecycle, per-scan summaries and failures. Logs are streamed to stdout, not accumulated in an application memory buffer or SQLite history. Token values, Authorization headers, CA contents, pagination tokens, raw response bodies and raw exception messages are not logged. Only explicit operational fields are allowed by the formatter; strings are bounded and JSON-escaped.

## Build and update existing deployment

```bash
docker build --platform linux/amd64 -f Dockerfile.central -t quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-agent:0.7.1 .
docker push quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-agent:0.7.1
```

Use an image/environment update on the existing Deployment, preserving its cluster config, token/CA mounts and PVC. Do not apply the generic example config to an existing multi-cluster setup. Real cluster connectivity is only verified by logs from your target environment; unit tests use synthetic API responses.
