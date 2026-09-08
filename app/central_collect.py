"""Bounded paginated GETs. Never shells out or connects to the LLM."""
from __future__ import annotations
import hashlib
import json
import re
import time
from pathlib import Path
import requests
from app.central_logging import emit
from app.namespace_patterns import namespace_matches

class ScanError(Exception):
    pass


def image_parts(image):
    name, _, digest = image.partition('@')
    tag = name.rsplit(':', 1)[1] if ':' in name.rsplit('/', 1)[-1] else None
    return tag, digest or None


def matches(image, target, policy):
    tag, _ = image_parts(image)
    if not tag:
        return False
    if policy.tag_match_mode == 'release':
        match = re.fullmatch(policy.release_pattern, tag)
        tag = match.group(1) if match else tag
    return tag == target


def health(pod, status):
    if pod.get('metadata', {}).get('deletionTimestamp'):
        return 'TERMINATING', 'Deleting'
    state = status.get('state', {})
    waiting = state.get('waiting', {})
    terminated = state.get('terminated', {})
    reason = waiting.get('reason') or terminated.get('reason') or ''
    if reason == 'CrashLoopBackOff':
        return 'CRASH', reason
    if reason in ('ImagePullBackOff', 'ErrImagePull'):
        return 'IMAGE_PULL_ERROR', reason
    if terminated and terminated.get('exitCode', 0) != 0:
        return 'CRASH', reason or 'NonZeroExit'
    if status.get('ready') and 'running' in state:
        return 'READY', ''
    if terminated and terminated.get('exitCode') == 0:
        return 'COMPLETED', reason
    if reason in ('CreateContainerConfigError', 'CreateContainerError', 'RunContainerError'):
        return 'ERROR', reason
    if pod.get('status', {}).get('phase') == 'Failed':
        return 'ERROR', pod.get('status', {}).get('reason', 'Failed')
    return 'NOT_READY', reason or pod.get('status', {}).get('phase', 'Pending')


class Collector:
    def __init__(self, cluster, flow, policy, target, cancelled, scope=None, session_id=None, scan_id=None):
        self.cluster, self.flow, self.policy, self.target = cluster, flow, policy, target
        self.cancelled = cancelled
        self.scope = scope
        self.deadline = time.monotonic() + flow.scan_timeout_seconds
        self.count = 0
        self.pages = 0
        self.last_path = None
        self.connected = False
        self.session_id, self.scan_id = session_id, scan_id

    def check(self):
        if self.cancelled.is_set():
            raise ScanError('CANCELLED')
        if time.monotonic() > self.deadline:
            raise ScanError('SCAN_TIMEOUT')

    def listed(self, http, path):
        self.last_path = path
        context = dict(cluster=self.cluster.id, session_id=self.session_id, scan_id=self.scan_id, path=path,
                       resource=path.rsplit("/",1)[-1], namespace=path.split("/namespaces/")[1].split("/")[0] if "/namespaces/" in path else "")
        continuation = None
        seen = set()
        while True:
            self.check()
            began = time.monotonic()
            emit("DEBUG","api_request_started",**context)
            params = {'limit': self.flow.page_size}
            if continuation:
                params['continue'] = continuation
            # Reload both token and CA for each request, including projected volume rotation.
            try:
                token = Path(self.cluster.token_file).read_text().strip()
            except OSError:
                raise ScanError("TOKEN_FILE_UNREADABLE") from None
            if not token:
                raise ScanError('EMPTY_TOKEN')
            verify = self.cluster.ca_file if self.cluster.api_url.startswith('https:') else True
            try:
                with http.get(self.cluster.api_url.rstrip('/') + path, params=params,
                              headers={'Authorization': 'Bearer ' + token}, verify=verify,
                              timeout=(self.flow.request_timeout_seconds, self.flow.request_timeout_seconds),
                              stream=True, allow_redirects=False) as response:
                    if response.status_code != 200:
                        emit('ERROR','api_request_failed',status=response.status_code,**context)
                        raise ScanError('API_HTTP_' + str(response.status_code))
                    data = bytearray()
                    for chunk in response.iter_content(65536):
                        self.check()
                        data.extend(chunk)
                        if len(data) > self.flow.max_page_bytes:
                            raise ScanError('PAGE_BYTE_LIMIT')
                    page = json.loads(data)
                    if not isinstance(page,dict) or not isinstance(page.get('items'),list):
                        raise ScanError('INVALID_API_RESPONSE')
                    self.pages += 1
                    if not self.connected:
                        emit('INFO','api_connected',api_url=self.cluster.api_url,status=200,**context)
                        self.connected = True
                    emit('DEBUG','api_page_received',status=200,objects=len(page['items']),pages=self.pages,
                         duration_ms=round((time.monotonic()-began)*1000),**context)
            except (ValueError, TypeError):
                raise ScanError('INVALID_API_RESPONSE') from None
            except OSError as exc:
                if isinstance(exc, requests.exceptions.RequestException):
                    if isinstance(exc, requests.exceptions.SSLError):
                        raise ScanError('TLS_ERROR') from None
                    if isinstance(exc, requests.exceptions.Timeout):
                        raise ScanError('API_TIMEOUT') from None
                    raise ScanError('CONNECTION_ERROR') from None
                raise ScanError('CA_FILE_UNREADABLE') from None
            for item in page.get('items', []):
                self.check()
                self.count += 1
                if self.count > self.flow.max_objects:
                    raise ScanError('OBJECT_LIMIT')
                yield item
            continuation = page.get('metadata', {}).get('continue')
            if not continuation:
                break
            if continuation in seen:
                raise ScanError('INVALID_PAGINATION')
            seen.add(continuation)

    def rows(self):
        # A pool is scoped to one scan so replaced CA files take effect on the next scan.
        with requests.Session() as http:
            http.trust_env = False
            namespaces = [n['metadata']['name'] for n in self.listed(http, '/api/v1/namespaces')
                          if re.search(self.cluster.namespace_pattern, n['metadata']['name'])
                          and (self.scope is None or (namespace_matches(n['metadata']['name'], self.scope.namespace_glob)
                               and (not self.scope.namespaces or n['metadata']['name'] in self.scope.namespaces)))]
            emit('DEBUG','namespace_scope_selected',cluster=self.cluster.id,session_id=self.session_id,scan_id=self.scan_id,namespace_count=len(namespaces))
            row_count = 0
            for namespace in namespaces:
                owners, desired, observed_targets = {}, {}, set()
                for resource in self.flow.resources:
                    for obj in self.listed(http, f'/apis/apps/v1/namespaces/{namespace}/{resource}'):
                        meta, spec, status = obj['metadata'], obj.get('spec', {}), obj.get('status', {})
                        kind = {'deployments': 'Deployment', 'statefulsets': 'StatefulSet', 'daemonsets': 'DaemonSet'}[resource]
                        uid = meta['uid']
                        owners[uid] = (kind, meta['name'], uid)
                        generation_ok = status.get('observedGeneration', 0) >= meta.get('generation', 0)
                        wanted = spec.get('replicas', 1) if kind != 'DaemonSet' else status.get('desiredNumberScheduled', 0)
                        updated = status.get('updatedReplicas', 0) if kind != 'DaemonSet' else status.get('updatedNumberScheduled', 0)
                        ready = status.get('readyReplicas', 0) if kind != 'DaemonSet' else status.get('numberReady', 0)
                        rollout = 'COMPLETE' if generation_ok and updated == wanted and ready == wanted else 'PROGRESSING'
                        if any(c.get('type') == 'Progressing' and c.get('status') == 'False' for c in status.get('conditions', [])):
                            rollout = 'FAILED'
                        for c in spec.get('template', {}).get('spec', {}).get('containers', []):
                            desired[(uid, c['name'])] = (c['image'], rollout, kind, meta['name'])
                if 'deployments' in self.flow.resources:
                    for rs in self.listed(http, f'/apis/apps/v1/namespaces/{namespace}/replicasets'):
                        ref = next((r for r in rs['metadata'].get('ownerReferences', []) if r.get('controller')), {})
                        if ref.get('uid') in owners:
                            owners[rs['metadata']['uid']] = owners[ref['uid']]
                for pod in self.listed(http, f'/api/v1/namespaces/{namespace}/pods'):
                    meta = pod['metadata']
                    ref = next((r for r in meta.get('ownerReferences', []) if r.get('controller')), {})
                    kind, name, uid = owners.get(ref.get('uid'), (ref.get('kind', 'Pod'), ref.get('name', meta['name']), ref.get('uid', meta['uid'])))
                    statuses = {c['name']: c for c in pod.get('status', {}).get('containerStatuses', [])}
                    for c in pod.get('spec', {}).get('containers', []):
                        img = c['image']
                        if not self.allowed(img):
                            continue
                        target = matches(img, self.target, self.policy)
                        if target:
                            observed_targets.add((uid, c['name']))
                        requested, rollout, _, _ = desired.get((uid, c['name']), ('', 'UNKNOWN', '', ''))
                        state = statuses.get(c['name'], {})
                        h, reason = health(pod, state)
                        row = self.row(namespace, kind, name, uid, c['name'], img, requested, rollout)
                        row.update(id=hashlib.sha256(f"{self.cluster.id}/{meta['uid']}/{c['name']}".encode()).hexdigest()[:32],
                                   pod=meta['name'], pod_uid=meta['uid'], ready=h == 'READY', health=h,
                                   reason=reason, phase=pod.get('status', {}).get('phase', 'Unknown'),
                                   restart_count=state.get('restartCount', 0), runtime_image_id=state.get('imageID', ''),
                                   previous_termination=state.get('lastState', {}).get('terminated', {}).get('reason', ''))
                        row_count += 1
                        if row_count > self.flow.max_rows:
                            raise ScanError('ROW_LIMIT')
                        yield row
                for (uid, container), (img, rollout, kind, name) in desired.items():
                    if self.allowed(img) and matches(img, self.target, self.policy) and (uid, container) not in observed_targets:
                        row = self.row(namespace, kind, name, uid, container, img, img, rollout)
                        row.update(id=hashlib.sha256(f'{self.cluster.id}/{uid}/{container}/desired'.encode()).hexdigest()[:32],
                                   row_type='desired', health='WAITING_FOR_POD', reason='No target-image pod observed')
                        row_count += 1
                        if row_count > self.flow.max_rows:
                            raise ScanError('ROW_LIMIT')
                        yield row

    def allowed(self, image):
        return not self.policy.registry_prefixes or any(image.startswith(p) for p in self.policy.registry_prefixes)

    def row(self, namespace, kind, workload, uid, container, image, desired, rollout):
        return dict(cluster=self.cluster.id, namespace=namespace, workload=workload, workload_kind=kind,
                    workload_uid=uid, container=container, image=image, image_tag=image_parts(image)[0],
                    desired_image=desired, rollout=rollout, target_match=matches(image, self.target, self.policy),
                    row_type='pod', pod='', pod_uid='', ready=False, restart_count=0, runtime_image_id='',
                    health='NOT_READY', reason='', phase='', previous_termination='')


def demo_rows(cluster, target, tick):
    """Repeatable fixtures: old version, regression, recovery, mixed and waiting."""
    for i, (name, old_crash) in enumerate([('payments', False), ('catalog', True), ('legacy', False), ('checkout', False)]):
        new = tick > 0 and name != 'legacy'
        image = 'registry.example.com/' + name + ':' + (target if new else '1.4.0')
        h = 'CRASH' if (old_crash and not new) or (name == 'payments' and new) else 'READY'
        yield dict(id=f'{cluster.id}-{name}-'+('new' if new else 'old'), cluster=cluster.id, namespace='test-demo',
                   workload=name, workload_kind='Deployment', workload_uid=name, container='app', image=image,
                   image_tag=image_parts(image)[0], desired_image=image, target_match=new, rollout='PROGRESSING' if h=='CRASH' else 'COMPLETE',
                   row_type='pod', pod=name+'-'+('new' if new else 'old'), pod_uid=name+str(new), ready=h=='READY',
                   health=h, reason='CrashLoopBackOff' if h=='CRASH' else '', phase='Running', restart_count=tick if h=='CRASH' else 0,
                   runtime_image_id='demo://sha256/example', previous_termination='')
