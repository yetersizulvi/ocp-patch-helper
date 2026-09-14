# Central Patch Monitor 0.7.3

This maintenance release builds on the clean 0.7.2 source and documentation. No database migration or new configuration keys are required. The 14-day configurable closed-session retention remains unchanged.

## Saved-flow deletion

Each saved design card has a separate **Sil** button. The UI asks for confirmation, removes the saved design, refreshes the list, and clears the selected design name/description if that design was selected. Cancelling confirmation makes no request. Existing sessions, their frozen configuration, current rows, and baseline rows remain intact. Deletion does not stop collection.

New endpoint:

```http
DELETE /api/v1/flows/designs/{name}
```

URL-encode the complete design name. The route accepts names containing slashes as well as Unicode. Successful response is HTTP 200 with `{"name":"example","deleted":true}`; a missing name returns HTTP 404. Retrying after a successful deletion therefore returns 404. Database deletion uses a bound SQL parameter. Cross-origin write protection also applies to DELETE. This does not add API authentication; upstream access control is still required.

Only user-saved designs are deleted. ConfigMap flow templates are managed in ConfigMap and remain visible through GET /api/v1/flows. Deleting a design frees one of the 50 saved-design slots.

## Fixed defects

- Empty `cluster=` and `namespace=` query values are normalized to no filter. Previously the UI's All clusters selection could show summary counts but empty image tables because the inventory query tested cluster equality against an empty string.
- A successful save keeps the saved design selected. Reloading the library preserves selection by name rather than stale array index.
- Selecting New design clears the prior saved name/description, preventing an accidental overwrite under the old name.
- API-created designs that inherit matching mode use the configured mode when loaded into the UI, rather than always falling back to exact.
- Selecting a design whose template is missing shows an explicit message.
- Save is disabled during the request to prevent overlapping submissions from repeated clicks.
- Stop is checked under the coordinator lock. A second request while STOPPING returns 409. It cannot proceed to clear task state after the first stop has completed and a new session starts.
- Collector ownership is tracked by session ID. A historical session cannot cancel a different active session's workers. Stop of an already STOPPED/COMPLETED session is harmless and returns the stored state.

## API and storage compatibility

Existing POST flow/session endpoints are unchanged. Deletion only touches the designs table; no foreign-key relationship to sessions is introduced. Existing serialized configurations remain readable. Runtime OpenAPI and docs/central-openapi.json report 0.7.3. The configuration schema from 0.7.2 is unchanged.

The new stop response 409 for an in-progress duplicate is intentional. Consumers should GET session state and wait for STOPPED rather than launching competing stop operations. Multiple independent simultaneous monitoring sessions remain unsupported.

## Upgrade

Build from the updated project root, replacing the registry namespace and selecting the architecture matching cluster workers:

```bash
docker build --platform linux/amd64 -f Dockerfile.central -t quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-helper:0.7.3 .
docker push quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-helper:0.7.3
```

For an existing deployment, use its actual namespace and update the image only:

```bash
oc set image deployment/patch-monitor monitor=quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-helper:0.7.3 -n patch-monitor
oc rollout status deployment/patch-monitor -n patch-monitor --timeout=180s
```

Preserve deployed ConfigMap, remote token/CA mounts, and PVC. Stop active monitoring before planned rollout. Refresh the browser after upgrade to load the new UI. Generic deployment examples are not a replacement for environment configuration.

## Verification and limits

42 Python tests pass, including delete with encoded names, cross-origin rejection, preserved baseline/session records, empty-filter inventory, duplicate stop and historical-session cancellation protection. Real demo HTTP/SSE smoke and jsdom interactions pass. DOM scenarios cover save/selection, confirmation cancellation, confirmed deletion with retained session, all-cluster inventory, filtering, comparison, history and stop.

This is targeted regression testing, not proof that every defect is eliminated. A real OpenShift deployment, image build/push, production-sized load test and screenshot-based browser validation were not performed in this environment. The existing single-process, external authentication, non-replayable SSE and no durable event-timeline constraints still apply.
