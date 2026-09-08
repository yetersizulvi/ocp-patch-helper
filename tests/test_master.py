import os
from datetime import datetime, timedelta, timezone
from unittest import IsolatedAsyncioTestCase, TestCase

os.environ.setdefault("AGENT_TOKEN", "unit-test-token")

from app import master
from app.master import DASHBOARD_HTML, app


class MasterUITests(TestCase):
    def test_dashboard_is_packaged_without_external_assets(self) -> None:
        self.assertIn("OpenShift AI Assistant", DASHBOARD_HTML)
        self.assertIn("/api/v1/summary", DASHBOARD_HTML)
        self.assertIn("/api/v1/start", DASHBOARD_HTML)
        self.assertIn("EventSource('/api/v1/stream')", DASHBOARD_HTML)
        self.assertIn('id="durationMinutes"', DASHBOARD_HTML)
        self.assertIn('id="remainingTime"', DASHBOARD_HTML)
        self.assertIn('id="agentList"', DASHBOARD_HTML)
        self.assertIn("openDetails: new Set()", DASHBOARD_HTML)
        self.assertIn("Görünümü duraklat", DASHBOARD_HTML)
        self.assertNotIn("<script src=", DASHBOARD_HTML)
        self.assertNotIn("<link rel=", DASHBOARD_HTML)

    def test_master_routes_cover_ui_and_agent_contract(self) -> None:
        paths = {route.path for route in app.routes}
        expected = {
            "/",
            "/health",
            "/api/v1/summary",
            "/api/v1/state",
            "/api/v1/start",
            "/api/v1/stop",
            "/api/v1/events",
            "/api/v1/runs",
            "/api/v1/agents",
            "/api/v1/stream",
            "/internal/v1/agents/register",
            "/internal/v1/agents/{cluster_id}/control",
            "/internal/v1/agents/{cluster_id}/heartbeat",
            "/internal/v1/agents/{cluster_id}/processes/{process_id}/events",
        }
        self.assertTrue(expected.issubset(paths))


class FakeJSONRequest:
    def __init__(self, payload):
        self.payload = payload

    async def json(self):
        return self.payload


class MonitoringSessionTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        master.STATE.update(
            {
                "desired_state": "IDLE",
                "instruction": None,
                "registration": None,
                "heartbeat": None,
                "events": [],
                "acks": [],
                "runs": [],
                "monitoring": None,
            }
        )

    async def test_start_creates_sixty_minute_monitoring_session(self) -> None:
        instruction = await master.start_analysis(
            FakeJSONRequest({"target_tag": "1.4.1", "duration_minutes": 60})
        )
        self.assertEqual("RUNNING", instruction["desired_state"])
        self.assertEqual(3600, instruction["parameters"]["monitoring_duration_seconds"])
        snapshot = master._monitoring_snapshot_locked()
        self.assertEqual("RUNNING", snapshot["status"])
        self.assertEqual(3600, snapshot["duration_seconds"])

    async def test_expired_session_generates_stop_instruction(self) -> None:
        await master.start_analysis(
            FakeJSONRequest({"target_tag": "1.4.1", "duration_minutes": 1})
        )
        master.STATE["monitoring"]["ends_at"] = (
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).isoformat()
        master._expire_monitoring_if_needed_locked()
        self.assertEqual("STOPPED", master.STATE["desired_state"])
        self.assertEqual("COMPLETED", master.STATE["monitoring"]["status"])
        self.assertEqual(
            "monitoring_duration_completed",
            master.STATE["instruction"]["parameters"]["reason"],
        )
