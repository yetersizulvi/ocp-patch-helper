# Central Patch Monitor 0.7.4

## Problem and diagnosis

A generic DRAFT baseline instruction did not explain why a selected cluster could not finish capture. Meanwhile periodic cluster_state logs represented the latest stored session for each cluster, without including its session ID. A reader could compare a DRAFT screen with a STOPPED log for a different session and infer an inconsistent state.

The 0.7.3 renderer already had a distinct STOPPED message; this release does not claim every reported DRAFT/STOPPED discrepancy was caused by that renderer. Separate session and summary HTTP calls could also span a status/revision update. The UI now retries that refresh when their status/revision values differ instead of rendering mixed responses.

## Changes

- Status labels identify the session ID, making it comparable to API URLs and logs.
- DRAFT and INTERRUPTED baseline instructions identify missing clusters and explain recorded errors. Successful cluster baselines remain preserved during retry.
- 401 is explained as failed API authentication with possible expired, invalid or wrong-cluster token; 403 points to RBAC; TLS and credential-file failures include relevant checks. No exact token-expiry claim is made without token evidence.
- STOPPED/COMPLETED with missing baseline records explains that comparison is incomplete. STOPPED directs the user to create a new session, not retry a closed baseline.
- Cluster cards retain the last scan error with a readable explanation even after the session stops.
- Periodic cluster_state JSON includes the session_id of the stored record. It remains historical scan state, not an independent live connectivity check.
- A filter on uvicorn.access suppresses only HTTP 2xx requests to /health/live and /health/ready (including query strings). Health failures, non-health paths and normal application logs remain visible. The filter is installed during central app configuration and is safe to reinstall.

## Authentication recovery

An upstream API_HTTP_401 is not fixed by changing a UI message or rebuilding the image. Validate/replace the affected cluster's token in its centrally mounted Secret and confirm the configured token_file identifies that file. Token expiration, revocation, wrong audience, wrong-cluster credentials or an authenticating intermediary require investigation. Do not expose token contents in screenshots/logs, and do not bypass CA verification to address a 401.

For a DRAFT with partial baseline, fix the credential and retry baseline; only missing clusters are collected. For a STOPPED session, create a new session after fixing credentials. Token contents are read on each API request, but Secret volume projection can take time. Frozen session paths do not change when ConfigMap paths change: create a new session for path/cluster configuration changes.

## Deployment

Build the central Dockerfile with tag 0.7.4 and update the existing monitor container image. Keep deployed configuration, Secret/CA mounts and PVC. No new YAML keys, schema migration or extra environment variable is required for the log filter. Refresh the browser after rollout. A restart interrupts active monitoring; stop planned monitoring before upgrade when possible.

```bash
docker build --platform linux/amd64 -f Dockerfile.central -t quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-helper:0.7.4 .
docker push quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-helper:0.7.4
oc set image deployment/patch-monitor monitor=quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-helper:0.7.4 -n patch-monitor
oc rollout status deployment/patch-monitor -n patch-monitor --timeout=180s
```

Use the namespace, registry image name and worker architecture from your actual deployment.

## Verification

44 Python tests pass. Targeted checks cover health-access filtering, filter reinstallation, the existing lifecycle/collection suite, DRAFT 401 instructions, STOPPED incomplete-baseline instructions with no retry action, and visible session IDs. The real HTTP smoke test checks successful health probes are absent from logs while an HTTP 405 health request and normal API access remain present. Synthetic HTTP/SSE and DOM tests do not certify real remote-cluster credentials or network connectivity. Screenshot-based visual testing and actual cluster rollout were not performed.
