# Central Patch Monitor 0.6.0

One application pod in a management/test cluster reads the local Kubernetes API and configured remote APIs. No remote agent, agent registration, HMAC control transport, or LLM call is used by this entrypoint. The legacy 0.5.1 entrypoints remain available for migration; do not apply the old agent/master manifests for a central installation.

## Run the demo

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
PATCH_CONFIG_FILE=config/central.demo.yaml uvicorn app.central:create_app --factory --host 0.0.0.0 --port 8080 --workers 1
```

Open `http://localhost:8080`. Choose target `1.4.1`, click **Baseline al**, wait for `BASELINE_READY`, then click **İzlemeyi başlat**. No cluster credentials or network calls are used in demo mode. Two synthetic clusters contain a healthy workload that crashes after changing image, a previously crashing workload that recovers, a healthy migrated workload, and one remaining on `1.4.0`. Data is prominently marked DEMO. Stop the session before taking another baseline. `/tmp/patch-monitor-demo/patch.db` stores demo history separately from deployment data.

## Flow configuration

`config/central.example.yaml` is the canonical example. Its contents are also embedded in `deploy/central/01-configmap.yaml`. `PATCH_CONFIG_FILE` selects the file; configuration is not changed via the UI/API. A session chooses a named flow, target tag, cluster subset, and duration. Flow definitions control:

| Field | Effect |
|---|---|
| `interval_seconds` | Desired scan start interval per cluster; scans never overlap on the same cluster |
| `request_timeout_seconds` | Connect/read timeout for each API request |
| `scan_timeout_seconds` | Whole scan budget, checked between reads and objects; blocking reads can extend it by a request timeout |
| `page_size` | Kubernetes LIST `limit` and maximum database write batch (capped at 200) |
| `max_page_bytes` | Maximum decompressed JSON response per page |
| `max_objects` | Maximum resources visited per cluster scan, including namespace/workload/ReplicaSet/pod objects |
| `max_rows` | Maximum regular-container and desired-placeholder rows per scan |
| `session_max_minutes` | Maximum selectable session duration |
| `failure_backoff_max_seconds` | Cap on exponential retry backoff |
| `stale_after_seconds` | Age after which the UI marks data stale |
| `resources` | Workload template readers: deployments, statefulsets, daemonsets |

ConfigMap updates are validated and reloaded every `config_reload_seconds`. Invalid files retain the last valid configuration and expose a reload error. **An existing session freezes the complete configuration, including its flow.** Create a new session to adopt new scope or cadence; this prevents silently changing a patch baseline's comparison scope. `stream_seconds` and the connection cap apply at runtime. Storage/demo changes require an application restart. Mount files as directories without `subPath` so Kubernetes volume projection can update them.

## Local HTTPS versus an HTTP proxy

The standard local API is `https://kubernetes.default.svc:443`, authenticated with the mounted ServiceAccount token and verified with the mounted ServiceAccount CA. It stays inside the cluster. Port 443 on this Service and port 6443 on an external API endpoint both carry HTTPS; an internal Service does not imply plaintext HTTP.

If your platform explicitly supplies an HTTP API proxy, set `api_url: http://your-proxy.namespace.svc:8080` and `allow_http: true`. Only use this with a trusted, deliberately provisioned proxy: the bearer credential travels in plaintext on that hop. There is no automatic TLS-to-HTTP fallback or `verify=false`. Merely changing the standard Kubernetes Service URL to HTTP will not make the API server accept HTTP.

Reference: https://kubernetes.io/docs/tasks/run-application/access-api-from-pod/

## Deploy one pod

1. Build `Dockerfile.central` and push an image to your own registry:

```bash
podman build -f Dockerfile.central -t REGISTRY/PROJECT/patch-monitor:0.6.0 .
podman push REGISTRY/PROJECT/patch-monitor:0.6.0
```

2. Edit the image in `deploy/central/02-deployment.yaml`. Edit cluster IDs, namespace regex and registry prefixes in `deploy/central/01-configmap.yaml`. Keep tokens out of this file.
3. Apply only the central manifests:

```bash
oc apply -f deploy/central/00-rbac.yaml
oc apply -f deploy/central/01-configmap.yaml
oc apply -f deploy/central/02-deployment.yaml
oc rollout status deployment/patch-monitor -n patch-monitor --timeout=180s
oc port-forward -n patch-monitor svc/patch-monitor 8080:8080
```

The PVC needs a working default StorageClass or an explicitly configured `storageClassName`. OpenShift's SCC/volume group allocation must make `/data` writable to the assigned non-root UID. Exactly one replica and one Uvicorn worker are required; the Recreate strategy prevents concurrent SQLite owners during rollout. There is no built-in user authentication; apply `03-route.yaml` only behind your organization's private/authenticated ingress. Route TLS terminates at the router and the Service-to-application hop is HTTP/8080. This is separate from application-to-Kubernetes API HTTPS.

## Add a remote cluster

Apply `00-rbac.yaml` on the remote cluster as well (namespace + reader account + read-only RBAC only). Do **not** deploy any workload there. Supply a time-limited reader token valid for the target API and its CA bundle. On the central cluster:

```bash
oc create secret generic patch-remote-test-token -n patch-monitor --from-file=token=/SECURE/PATH/remote-token
oc create configmap patch-remote-test-ca -n patch-monitor --from-file=ca.crt=/PATH/remote-api-ca.crt
```

Uncomment/configure the remote cluster entry in the ConfigMap and its matching Secret/ConfigMap volume mounts in the Deployment. API, token and CA paths are cluster-specific; the local token cannot authenticate to another cluster. DNS and TCP/6443 must be reachable from the central pod's egress path. Configure your corporate FW/egress rules accordingly.

The app re-reads the token for every page request and opens a new TLS connection pool each scan, so replacing mounted token/CA files takes effect without a pod restart. **It does not mint or automatically renew remote tokens.** Your existing credential automation must renew remote short-lived tokens before expiry and update the Secret. `401`, `403`, TLS errors and timeouts are shown as cluster errors; they never clear the last successful snapshot. CA files must trust the API certificate, which may use a different CA from the apps Route or LLM.

The namespace regex is a collection filter, not a security boundary: the example ClusterRole grants read access cluster-wide. To enforce narrower permissions, replace workload/pod ClusterRole permissions with RoleBindings in the allowed namespaces and retain namespace-list permission. The collector lists workloads and pods in matching namespaces only.

## API

Interactive OpenAPI docs are at `/docs`; machine-readable schema is `/openapi.json` (also exported to `docs/central-openapi.json`).

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/config` | Effective public configuration and reload status (no token content/path) |
| GET | `/api/v1/clusters` | Configured cluster endpoints |
| POST | `/api/v1/sessions` | Create a draft session with `target_tag`, `flow`, optional `clusters`, `duration_minutes` |
| GET | `/api/v1/sessions` | Latest 100 sessions |
| GET | `/api/v1/sessions/{id}` | State, scope, per-cluster baseline status and observation times |
| POST | `/api/v1/sessions/{id}/baseline` | Asynchronously capture missing baseline clusters; returns 202 |
| POST | `/api/v1/sessions/{id}/start` | Start only after every selected cluster has a baseline |
| POST | `/api/v1/sessions/{id}/stop` | Stop and wait for in-flight reads to exit |
| GET | `/api/v1/sessions/{id}/summary` | Health/target counts, comparison counts, freshness |
| GET | `/api/v1/sessions/{id}/images` | Left table: all actual regular pod containers |
| GET | `/api/v1/sessions/{id}/targets` | Right table: target-tag containers and missing-target-pod placeholders |
| GET | `/api/v1/sessions/{id}/changes` | Workload/container comparison against baseline |
| GET | `/api/v1/sessions/{id}/containers/{container_id}` | Current container plus up to 100 corresponding baseline replicas |
| GET | `/api/v1/sessions/{id}/stream` | SSE `revision` notifications, not full JSON snapshots |

Lists accept `limit` (1–200, default 50) and `cursor`. Image/target tables support `health`, `cluster`; image inventory also supports `target_match`. Changes support `classification`. Row cursors are seek cursors; changes use an offset. Pagination is live, not a frozen cross-request snapshot: revisions can change across pages, and the client should return to the first page for a fresh view. Pod rows and desired placeholders are explicitly distinguished by `row_type`.

A baseline is immutable once captured for a cluster. If another cluster fails, the session returns to DRAFT and only missing clusters are retried. All clusters must finish before START. If patching has already begun while an incomplete baseline is retried, stop and create a new pre-patch session instead of treating different capture times as one instant. APIs are individually consistent per completed cluster scan; Kubernetes LISTs across resource kinds are not an atomic global snapshot.

On application restart, an active capture or run becomes INTERRUPTED. Completed baseline rows survive on the PVC. An interrupted run with complete baseline can be explicitly started again (a new duration window); the application does not silently resume scanning. STOP/expiry prevents any late scan from publishing data.

## Comparison and image semantics

Image/tag is read from Pod spec; runtime `imageID`, workload desired image, Ready, phase, restart count, current reason and last termination reason remain separate. A previous OOM termination does not label a currently Ready container as crashing. A Running pod phase does not hide a waiting CrashLoopBackOff container. Exact tag matching is default; release mode uses one configured regex capture group. Digest-only images do not count as the target tag.

ReplicaSet owner references resolve pod ownership to Deployment UID. Stable comparison key: cluster + namespace + workload UID + regular container name. Deleted/recreated workloads have new UIDs and appear as removed/new resources; pod replacement during an ordinary rollout does not lose workload continuity. Standalone/other-controller pods are still shown, but template rollout may be UNKNOWN. Init containers, ephemeral containers, logs, Events and LLM analysis are outside this first central collector's scope.

Comparisons aggregate replica error/Ready counts and image sets: REGRESSION, RECOVERED, PERSISTING_ERROR, MIXED_VERSION, OLD_VERSION, IMAGE_CHANGED, NEW_RESOURCE, REMOVED, UNCHANGED. REGRESSION means the observed error count increased; RECOVERED means baseline errors disappeared and all currently observed replicas are Ready. They are evidence of changed health, not proof of image causality. Restart delta is provided only when the same pod UID exists in baseline; a new pod's restart counter is not subtracted from a different pod's counter.

## Memory, disk and limitations

The collector holds one bounded API page, compact namespace/workload ownership maps (bounded by object count), and a write batch of at most 200 normalized rows. Completed snapshots and baseline reside in indexed SQLite, not RAM. No full object/event history is accumulated. Every cluster has its own task and timeout/backoff. The UI uses bounded pages and a capped number of SSE clients with no application-level per-client event queue. Slow clients do not accumulate snapshots.

SQLite cache is 4 MiB. `max_database_mb` caps database pages; journal overhead can temporarily require approximately another database's size, hence the example 2 GiB PVC for a 512 MiB database cap. The cap includes baseline/current/staging data. Expired closed sessions are deleted by retention; pages are reused rather than shrinking the file on every scan. Current/draft/baseline-ready sessions are retained; stop abandoned drafts to allow retention. Disk/quota errors return 507 or mark cluster storage errors while preserving the last committed baseline/current state. If error recording itself fails, readiness fails and collection is cancelled. These are bounds on buffers/database, not a proven RSS target; production-sized memory/load testing is still required.

Polling can miss transitions that begin and end between scans. SSE is live UI delivery, not Kubernetes Watch. A failed/partial scan is never treated as an empty cluster. Stopped sessions display their last observation time and naturally become stale. There is no historical event replay: clients reconnect by reading the latest summary and pages. One central pod is a single point of monitoring interruption.

## Verification

```bash
python -m pip install -r requirements-dev.txt
PYTHONPATH=. python -m unittest discover -s tests -v
python scripts/smoke_central.py
```

Tests cover legacy compatibility and central baseline immutability, partial failure/retry, new crashes/recovery/old versions, pagination, token reload, API errors/response size limits, owner resolution, pending target pods, stale data, restart persistence and HTTP proxy opt-in. A real cluster and container image build must be verified in the target environment.

Implementation validation: 25 unit/integration tests passed, including quota rollback, flow snapshot isolation, expiry and cancellation. The standalone smoke script passed real HTTP and SSE checks using two demo clusters. JavaScript syntax was checked with Node. Automated visual QA could not reach the local demo URL in the browser environment; no visual-pass claim is made. Container build and actual Kubernetes API connectivity were not tested in this workspace.
