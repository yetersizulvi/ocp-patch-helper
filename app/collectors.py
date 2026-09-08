from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from typing import Any, Callable

from kubernetes import client, config

from app.models import CollectorConfig


CRASH_REASONS = {
    "CrashLoopBackOff",
    "ImagePullBackOff",
    "ErrImagePull",
    "CreateContainerConfigError",
    "CreateContainerError",
    "RunContainerError",
    "OOMKilled",
}


def normalize_tag(tag: str | None) -> str | None:
    return tag.strip().split("-", 1)[0] if tag else None


def parse_image(reference: str) -> dict[str, str | None]:
    value = reference.strip()
    digest = None
    without_digest = value
    if "@" in value:
        without_digest, digest = value.rsplit("@", 1)
    slash = without_digest.rfind("/")
    colon = without_digest.rfind(":")
    tag = without_digest[colon + 1 :] if colon > slash else None
    repository = without_digest[:colon] if colon > slash else without_digest
    return {
        "reference": value,
        "repository": repository,
        "tag": tag,
        "normalized_tag": normalize_tag(tag),
        "digest": digest,
    }


def fingerprint(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


class KubeDataSource:
    """Shared short-lived cache prevents multiple flow steps from relisting resources."""

    def __init__(
        self,
        namespace_pattern: str,
        request_timeout_seconds: int,
        cache_seconds: int,
    ) -> None:
        config.load_incluster_config()
        api_client = client.ApiClient()
        self.core = client.CoreV1Api(api_client)
        self.apps = client.AppsV1Api(api_client)
        self.custom = client.CustomObjectsApi(api_client)
        self.namespace_regex = re.compile(namespace_pattern)
        self.timeout = request_timeout_seconds
        self.cache_seconds = cache_seconds
        self._cache: dict[str, tuple[float, Any]] = {}
        self._lock = threading.RLock()

    def set_cache_seconds(self, seconds: int) -> None:
        self.cache_seconds = seconds

    def _cached(self, key: str, loader: Callable[[], Any]) -> Any:
        now = time.monotonic()
        with self._lock:
            cached = self._cache.get(key)
            if cached and now - cached[0] <= self.cache_seconds:
                return cached[1]
        value = loader()
        with self._lock:
            self._cache[key] = (time.monotonic(), value)
        return value

    def namespaces(self) -> list[str]:
        items = self._cached(
            "namespaces",
            lambda: self.core.list_namespace(_request_timeout=self.timeout).items,
        )
        return sorted(
            item.metadata.name
            for item in items
            if item.metadata.name and self.namespace_regex.match(item.metadata.name)
        )

    def pods(self) -> list[Any]:
        allowed = set(self.namespaces())
        items = self._cached(
            "pods",
            lambda: self.core.list_pod_for_all_namespaces(
                _request_timeout=self.timeout
            ).items,
        )
        return [item for item in items if item.metadata.namespace in allowed]

    def deployments(self) -> list[Any]:
        allowed = set(self.namespaces())
        items = self._cached(
            "deployments",
            lambda: self.apps.list_deployment_for_all_namespaces(
                _request_timeout=self.timeout
            ).items,
        )
        return [item for item in items if item.metadata.namespace in allowed]

    def stateful_sets(self) -> list[Any]:
        allowed = set(self.namespaces())
        items = self._cached(
            "statefulsets",
            lambda: self.apps.list_stateful_set_for_all_namespaces(
                _request_timeout=self.timeout
            ).items,
        )
        return [item for item in items if item.metadata.namespace in allowed]

    def daemon_sets(self) -> list[Any]:
        allowed = set(self.namespaces())
        items = self._cached(
            "daemonsets",
            lambda: self.apps.list_daemon_set_for_all_namespaces(
                _request_timeout=self.timeout
            ).items,
        )
        return [item for item in items if item.metadata.namespace in allowed]

    def nodes(self) -> list[Any]:
        return self._cached(
            "nodes", lambda: self.core.list_node(_request_timeout=self.timeout).items
        )

    def cluster_operators(self) -> list[dict[str, Any]]:
        response = self._cached(
            "clusteroperators",
            lambda: self.custom.list_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="clusteroperators",
                _request_timeout=self.timeout,
            ),
        )
        return response.get("items", [])

    def cluster_version(self) -> dict[str, Any] | None:
        return self._cached(
            "clusterversion-version",
            lambda: self.custom.get_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="clusterversions",
                name="version",
                _request_timeout=self.timeout,
            ),
        )

    def machine_config_pools(self) -> list[dict[str, Any]]:
        response = self._cached(
            "machineconfigpools",
            lambda: self.custom.list_cluster_custom_object(
                group="machineconfiguration.openshift.io",
                version="v1",
                plural="machineconfigpools",
                _request_timeout=self.timeout,
            ),
        )
        return response.get("items", [])

    def warning_events_for_pod(self, namespace: str, pod_name: str, limit: int) -> list[dict[str, Any]]:
        response = self.core.list_namespaced_event(
            namespace=namespace,
            field_selector=f"involvedObject.name={pod_name},type=Warning",
            _request_timeout=self.timeout,
        )
        result = []
        for event in sorted(
            response.items,
            key=lambda item: str(
                item.last_timestamp or item.event_time or item.metadata.creation_timestamp
            ),
            reverse=True,
        )[:limit]:
            result.append(
                {
                    "reason": event.reason,
                    "message": (event.message or "")[:500],
                    "count": _int(event.count),
                    "last_timestamp": str(
                        event.last_timestamp
                        or event.event_time
                        or event.metadata.creation_timestamp
                    ),
                }
            )
        return result


class CollectorEngine:
    def __init__(self, source: KubeDataSource, registry_prefix: str | None) -> None:
        self.source = source
        self.registry_prefix = registry_prefix

    def collect(
        self,
        collector: CollectorConfig,
        target_tag: str,
    ) -> dict[str, Any]:
        if collector.type.value == "image_rollout":
            return self.image_rollout(target_tag, collector.options)
        if collector.type.value == "health_errors":
            return self.health_errors(collector.options)
        if collector.type.value == "crash_triage":
            return self.crash_triage(collector.options)
        raise ValueError(f"unsupported collector type: {collector.type.value}")

    def image_rollout(self, target_tag: str, options: dict[str, Any]) -> dict[str, Any]:
        normalized_target = normalize_tag(target_tag)
        workloads: list[dict[str, Any]] = []
        workload_objects = [
            ("Deployment", item) for item in self.source.deployments()
        ] + [("StatefulSet", item) for item in self.source.stateful_sets()] + [
            ("DaemonSet", item) for item in self.source.daemon_sets()
        ]
        total = 0
        matching = 0
        for kind, item in workload_objects:
            images = []
            containers = item.spec.template.spec.containers or []
            for container in containers:
                if self.registry_prefix and not container.image.startswith(
                    self.registry_prefix
                ):
                    continue
                parsed = parse_image(container.image)
                parsed["container"] = container.name
                parsed["matches_target"] = (
                    parsed["normalized_tag"] == normalized_target
                )
                total += 1
                matching += int(bool(parsed["matches_target"]))
                images.append(parsed)
            if not images:
                continue
            workloads.append(
                {
                    "key": (
                        f"{item.metadata.namespace}|{kind}|{item.metadata.name}"
                    ),
                    "namespace": item.metadata.namespace,
                    "kind": kind,
                    "name": item.metadata.name,
                    "images": images,
                }
            )
        mismatches = [
            {
                "workload": workload["key"],
                "container": image["container"],
                "tag": image["tag"],
                "normalized_tag": image["normalized_tag"],
                "reference": image["reference"],
            }
            for workload in workloads
            for image in workload["images"]
            if not image["matches_target"]
        ]
        max_items = max(1, min(1000, int(options.get("max_mismatches", 200))))
        return {
            "target_tag": target_tag,
            "normalized_target_tag": normalized_target,
            "image_total": total,
            "image_matching": matching,
            "coverage_percent": round((matching / total * 100) if total else 0.0, 2),
            "mismatch_total": len(mismatches),
            "mismatches": mismatches[:max_items],
            "truncated": len(mismatches) > max_items,
        }

    def health_errors(self, options: dict[str, Any]) -> dict[str, Any]:
        errors: list[dict[str, Any]] = []
        max_items = max(10, min(1000, int(options.get("max_errors", 300))))

        for pod in self.source.pods():
            issues = self._pod_issues(pod)
            if issues:
                errors.append(
                    {
                        "type": "pod",
                        "namespace": pod.metadata.namespace,
                        "name": pod.metadata.name,
                        "phase": pod.status.phase,
                        "issues": issues,
                        "restart_count": sum(
                            _int(status.restart_count)
                            for status in (pod.status.container_statuses or [])
                        ),
                    }
                )

        for deployment in self.source.deployments():
            desired = _int(deployment.spec.replicas)
            ready = _int(deployment.status.ready_replicas)
            updated = _int(deployment.status.updated_replicas)
            unavailable = _int(deployment.status.unavailable_replicas)
            if ready != desired or updated != desired or unavailable:
                errors.append(
                    {
                        "type": "workload",
                        "kind": "Deployment",
                        "namespace": deployment.metadata.namespace,
                        "name": deployment.metadata.name,
                        "desired": desired,
                        "ready": ready,
                        "updated": updated,
                        "unavailable": unavailable,
                    }
                )

        for stateful_set in self.source.stateful_sets():
            desired = _int(stateful_set.spec.replicas)
            ready = _int(stateful_set.status.ready_replicas)
            updated = _int(stateful_set.status.updated_replicas)
            unavailable = max(0, desired - ready)
            if ready != desired or updated != desired:
                errors.append(
                    {
                        "type": "workload",
                        "kind": "StatefulSet",
                        "namespace": stateful_set.metadata.namespace,
                        "name": stateful_set.metadata.name,
                        "desired": desired,
                        "ready": ready,
                        "updated": updated,
                        "unavailable": unavailable,
                    }
                )

        for daemon_set in self.source.daemon_sets():
            desired = _int(daemon_set.status.desired_number_scheduled)
            ready = _int(daemon_set.status.number_ready)
            updated = _int(daemon_set.status.updated_number_scheduled)
            unavailable = _int(daemon_set.status.number_unavailable)
            if ready != desired or updated != desired or unavailable:
                errors.append(
                    {
                        "type": "workload",
                        "kind": "DaemonSet",
                        "namespace": daemon_set.metadata.namespace,
                        "name": daemon_set.metadata.name,
                        "desired": desired,
                        "ready": ready,
                        "updated": updated,
                        "unavailable": unavailable,
                    }
                )

        for node in self.source.nodes():
            conditions = {item.type: item.status for item in node.status.conditions or []}
            if conditions.get("Ready") != "True":
                errors.append(
                    {"type": "node", "name": node.metadata.name, "issue": "NotReady"}
                )
            for pressure in ("MemoryPressure", "DiskPressure", "PIDPressure"):
                if conditions.get(pressure) == "True":
                    errors.append(
                        {"type": "node", "name": node.metadata.name, "issue": pressure}
                    )

        for operator in self.source.cluster_operators():
            conditions = {
                item.get("type"): item
                for item in operator.get("status", {}).get("conditions", [])
            }
            degraded = conditions.get("Degraded", {}).get("status") == "True"
            available = conditions.get("Available", {}).get("status") == "True"
            if degraded or not available:
                condition = conditions.get("Degraded" if degraded else "Available", {})
                errors.append(
                    {
                        "type": "cluster_operator",
                        "name": operator.get("metadata", {}).get("name"),
                        "degraded": degraded,
                        "available": available,
                        "reason": condition.get("reason"),
                        "message": (condition.get("message") or "")[:500],
                    }
                )

        cluster_version = self.source.cluster_version()
        if cluster_version:
            conditions = {
                item.get("type"): item
                for item in cluster_version.get("status", {}).get("conditions", [])
            }
            failing = conditions.get("Failing", {}).get("status") == "True"
            if failing:
                condition = conditions.get("Failing", {})
                errors.append(
                    {
                        "type": "cluster_version",
                        "name": "version",
                        "failing": True,
                        "reason": condition.get("reason"),
                        "message": (condition.get("message") or "")[:500],
                    }
                )

        for pool in self.source.machine_config_pools():
            conditions = {
                item.get("type"): item.get("status")
                for item in pool.get("status", {}).get("conditions", [])
            }
            degraded = any(
                conditions.get(name) == "True"
                for name in ("Degraded", "NodeDegraded", "RenderDegraded")
            )
            unavailable = _int(pool.get("status", {}).get("unavailableMachineCount"))
            if degraded or unavailable:
                errors.append(
                    {
                        "type": "machine_config_pool",
                        "name": pool.get("metadata", {}).get("name"),
                        "degraded": degraded,
                        "unavailable": unavailable,
                    }
                )
        return {
            "error_total": len(errors),
            "errors": errors[:max_items],
            "truncated": len(errors) > max_items,
        }

    def crash_triage(self, options: dict[str, Any]) -> dict[str, Any]:
        max_pods = max(1, min(100, int(options.get("max_crash_pods", 20))))
        max_events = max(0, min(50, int(options.get("max_events_per_pod", 10))))
        crash_pods = []
        crash_total = 0
        for pod in self.source.pods():
            issues = sorted(set(self._pod_issues(pod)) & CRASH_REASONS)
            if not issues:
                continue
            crash_total += 1
            if len(crash_pods) < max_pods:
                crash_pods.append(
                    self._describe_crash_pod(pod, issues, max_events)
                )
        return {
            "crash_total": crash_total,
            "crashes": crash_pods,
            "truncated": crash_total > len(crash_pods),
        }

    @staticmethod
    def _pod_issues(pod: Any) -> list[str]:
        issues: set[str] = set()
        if pod.status.phase not in {"Running", "Succeeded"}:
            issues.add(pod.status.phase or "UnknownPhase")
        ready = next(
            (item for item in pod.status.conditions or [] if item.type == "Ready"),
            None,
        )
        if pod.status.phase == "Running" and (not ready or ready.status != "True"):
            issues.add("NotReady")
        statuses = list(pod.status.init_container_statuses or []) + list(
            pod.status.container_statuses or []
        )
        for status in statuses:
            waiting = getattr(getattr(status, "state", None), "waiting", None)
            if waiting and waiting.reason:
                issues.add(waiting.reason)
            terminated = getattr(getattr(status, "last_state", None), "terminated", None)
            if terminated and terminated.reason:
                issues.add(terminated.reason)
        return sorted(issues)

    def _describe_crash_pod(
        self,
        pod: Any,
        issues: list[str],
        max_events: int,
    ) -> dict[str, Any]:
        statuses = {
            item.name: item for item in (pod.status.container_statuses or [])
        }
        containers = []
        for container in pod.spec.containers or []:
            status = statuses.get(container.name)
            waiting = getattr(getattr(status, "state", None), "waiting", None)
            last_terminated = getattr(
                getattr(status, "last_state", None), "terminated", None
            )
            image = parse_image(container.image)
            containers.append(
                {
                    "name": container.name,
                    "image": image,
                    "ready": bool(status and status.ready),
                    "restart_count": _int(status.restart_count) if status else 0,
                    "waiting_reason": waiting.reason if waiting else None,
                    "waiting_message": (waiting.message or "")[:500] if waiting else None,
                    "last_terminated_reason": (
                        last_terminated.reason if last_terminated else None
                    ),
                    "last_exit_code": (
                        last_terminated.exit_code if last_terminated else None
                    ),
                }
            )
        conditions = [
            {
                "type": item.type,
                "status": item.status,
                "reason": item.reason,
                "message": (item.message or "")[:300],
            }
            for item in pod.status.conditions or []
            if item.status != "True"
        ]
        owners = [
            {"kind": item.kind, "name": item.name}
            for item in pod.metadata.owner_references or []
        ]
        events = (
            self.source.warning_events_for_pod(
                pod.metadata.namespace, pod.metadata.name, max_events
            )
            if max_events
            else []
        )
        return {
            "namespace": pod.metadata.namespace,
            "pod": pod.metadata.name,
            "uid": str(pod.metadata.uid) if pod.metadata.uid else None,
            "phase": pod.status.phase,
            "node": pod.spec.node_name,
            "issues": issues,
            "owners": owners,
            "containers": containers,
            "conditions": conditions,
            "warning_events": events,
        }
