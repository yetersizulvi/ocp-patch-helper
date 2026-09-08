import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase, TestCase

from app.collectors import CollectorEngine, normalize_tag, parse_image
from app.config import AgentSettings, load_flows
from app.models import (
    CollectorConfig,
    CollectorType,
    FlowDocument,
    FlowMetadata,
    FlowSpec,
    ProcessInstruction,
    DesiredState,
    RuntimeState,
)
from app.runtime import AgentRuntime
from app.scheduler import FlowRunner
from app.security import sign_request


def ns(**values):
    return SimpleNamespace(**values)


class FlowConfigTests(TestCase):
    def test_intervals_are_loaded_from_yaml(self) -> None:
        content = """
flows:
  - apiVersion: assistant.example.io/v1alpha1
    kind: PatchAgentFlow
    metadata:
      name: monthly
    spec:
      control:
        control_poll_seconds: 7
        heartbeat_seconds: 40
        default_lease_seconds: 120
        flow_reload_seconds: 30
        shared_cache_seconds: 3
      collectors:
        - id: images
          type: image_rollout
          interval_seconds: 4
        - id: errors
          type: health_errors
          interval_seconds: 45
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "flows.yaml"
            path.write_text(content, encoding="utf-8")
            flow = load_flows(path)["monthly"]
        self.assertEqual(7, flow.spec.control.control_poll_seconds)
        self.assertEqual(4, flow.spec.collectors[0].interval_seconds)
        self.assertEqual(45, flow.spec.collectors[1].interval_seconds)

    def test_image_tag_normalization_uses_first_hyphen(self) -> None:
        self.assertEqual("1.4.1", normalize_tag("1.4.1-a81bc223"))
        image = parse_image("registry.example.com/team/api:1.4.1-928472")
        self.assertEqual("1.4.1", image["normalized_tag"])

    def test_request_signature_binds_method_path_and_body(self) -> None:
        first = sign_request("token", "123", "POST", "/one", b"{}")
        second = sign_request("token", "123", "POST", "/two", b"{}")
        self.assertNotEqual(first, second)


class FakeCrashSource:
    def pods(self):
        waiting = ns(reason="CrashLoopBackOff", message="back-off restarting")
        container_status = ns(
            name="api",
            ready=False,
            restart_count=6,
            state=ns(waiting=waiting),
            last_state=ns(terminated=ns(reason="Error", exit_code=1)),
        )
        crashing = ns(
            metadata=ns(
                namespace="test-api",
                name="api-123",
                uid="uid-1",
                owner_references=[ns(kind="ReplicaSet", name="api-abc")],
            ),
            spec=ns(
                node_name="worker-0",
                containers=[
                    ns(
                        name="api",
                        image="registry.example.com/team/api:1.4.1-hash",
                    )
                ],
            ),
            status=ns(
                phase="Running",
                conditions=[
                    ns(
                        type="Ready",
                        status="False",
                        reason="ContainersNotReady",
                        message="container is not ready",
                    )
                ],
                init_container_statuses=[],
                container_statuses=[container_status],
            ),
        )
        healthy_status = ns(
            name="healthy",
            ready=True,
            restart_count=0,
            state=ns(waiting=None),
            last_state=ns(terminated=None),
        )
        healthy = ns(
            metadata=ns(
                namespace="test-api",
                name="healthy-123",
                uid="uid-2",
                owner_references=[],
            ),
            spec=ns(
                node_name="worker-1",
                containers=[ns(name="healthy", image="repo/healthy:1.4.1")],
            ),
            status=ns(
                phase="Running",
                conditions=[ns(type="Ready", status="True", reason=None, message=None)],
                init_container_statuses=[],
                container_statuses=[healthy_status],
            ),
        )
        return [crashing, healthy]

    def warning_events_for_pod(self, namespace, pod_name, limit):
        return [
            {
                "reason": "BackOff",
                "message": "Back-off restarting failed container",
                "count": 6,
                "last_timestamp": "2026-08-31T10:00:00Z",
            }
        ][:limit]


class CollectorTests(TestCase):
    def test_crash_triage_describes_only_crashing_pods(self) -> None:
        engine = CollectorEngine(FakeCrashSource(), "registry.example.com/")
        data = engine.crash_triage(
            {"max_crash_pods": 20, "max_events_per_pod": 10}
        )
        self.assertEqual(1, data["crash_total"])
        self.assertEqual("api-123", data["crashes"][0]["pod"])
        self.assertEqual(
            "1.4.1", data["crashes"][0]["containers"][0]["image"]["normalized_tag"]
        )
        self.assertEqual("BackOff", data["crashes"][0]["warning_events"][0]["reason"])


class FakeEngine:
    def collect(self, collector, target_tag):
        return {
            "crash_total": 1,
            "crashes": [{"pod": "api-123", "reason": "CrashLoopBackOff"}],
        }


class FakeLLM:
    def __init__(self):
        self.calls = 0

    def analyze(self, *args):
        self.calls += 1
        return {"available": True, "analysis": {"summary": "crash"}}


class FakeMaster:
    def __init__(self):
        self.events = []
        self.acks = []

    def send_event(self, event):
        self.events.append(event)

    def acknowledge(self, event):
        self.acks.append(event)


class FakeSource:
    def __init__(self):
        self.cache_seconds = None

    def set_cache_seconds(self, seconds):
        self.cache_seconds = seconds


class SchedulerTests(IsolatedAsyncioTestCase):
    async def test_llm_and_event_are_emitted_only_when_crash_changes(self) -> None:
        settings = AgentSettings(
            cluster_id="cluster-a",
            environment="test",
            namespace_pattern="^test-",
            master_url="https://master.example",
            agent_token="token",
            master_ca_file=None,
            master_verify_tls=True,
            request_timeout_seconds=5,
            flow_config_path="/tmp/not-used",
            llm_url="https://llm.example.com/v1/chat/completions",
            llm_token="llm-token",
            llm_model="model",
            llm_ca_file=None,
            llm_timeout_seconds=30,
            registry_prefix="registry.example.com/",
            log_level="INFO",
        )
        collector = CollectorConfig(
            id="crash-triage",
            type=CollectorType.CRASH_TRIAGE,
            interval_seconds=30,
            emit_only_changes=True,
            options={"llm_enabled": True},
        )
        flow = FlowDocument(
            metadata=FlowMetadata(name="monthly"),
            spec=FlowSpec(collectors=[collector]),
        )
        instruction = ProcessInstruction(
            command_id="cmd-1",
            desired_state=DesiredState.RUNNING,
            process_id="process-1",
            flow_name="monthly",
            target_tag="1.4.1",
            lease_seconds=120,
            issued_at=datetime.now(timezone.utc),
        )
        llm = FakeLLM()
        master = FakeMaster()
        runner = FlowRunner(
            settings, instruction, flow, FakeEngine(), llm, master
        )
        await runner._run_once(collector)
        await runner._run_once(collector)
        self.assertEqual(1, llm.calls)
        self.assertEqual(1, len(master.events))
        self.assertIn("llm", master.events[0].data)

    async def test_master_instruction_starts_and_stops_collectors(self) -> None:
        settings = AgentSettings(
            cluster_id="cluster-a",
            environment="test",
            namespace_pattern="^test-",
            master_url="https://master.example",
            agent_token="token",
            master_ca_file=None,
            master_verify_tls=True,
            request_timeout_seconds=5,
            flow_config_path="/tmp/not-used",
            llm_url="https://llm.example.com/v1/chat/completions",
            llm_token="llm-token",
            llm_model="model",
            llm_ca_file=None,
            llm_timeout_seconds=30,
            registry_prefix="registry.example.com/",
            log_level="INFO",
        )
        collector = CollectorConfig(
            id="images",
            type=CollectorType.IMAGE_ROLLOUT,
            interval_seconds=30,
            initial_delay_seconds=300,
        )
        flow = FlowDocument(
            metadata=FlowMetadata(name="monthly"),
            spec=FlowSpec(collectors=[collector]),
        )
        master = FakeMaster()
        runtime = AgentRuntime(
            settings,
            master=master,
            source=FakeSource(),
            llm=FakeLLM(),
        )
        runtime.flows = {"monthly": flow}
        runtime.state = RuntimeState.IDLE
        start = ProcessInstruction(
            command_id="start-1",
            desired_state=DesiredState.RUNNING,
            process_id="process-1",
            flow_name="monthly",
            target_tag="1.4.1",
            lease_seconds=120,
            issued_at=datetime.now(timezone.utc),
        )
        await runtime._apply_instruction(start)
        self.assertEqual(RuntimeState.RUNNING, runtime.state)
        self.assertIsNotNone(runtime.lease_expires_at)

        stop = ProcessInstruction(
            command_id="stop-1",
            desired_state=DesiredState.STOPPED,
            process_id="process-1",
            issued_at=datetime.now(timezone.utc),
        )
        await runtime._apply_instruction(stop)
        self.assertEqual(RuntimeState.IDLE, runtime.state)
        self.assertIsNone(runtime.runner)
        self.assertEqual(2, len(master.acks))
