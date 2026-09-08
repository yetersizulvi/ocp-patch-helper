from __future__ import annotations

import json
import time
from typing import Any

import requests

from app.config import AgentSettings
from app.models import (
    AckEvent,
    AgentRegistration,
    CollectorEvent,
    ProcessInstruction,
)
from app.security import sign_request


class MasterProtocolError(RuntimeError):
    pass


class MasterClient:
    def __init__(self, settings: AgentSettings) -> None:
        self.settings = settings

    def register(self, registration: AgentRegistration) -> None:
        self._request("POST", "/internal/v1/agents/register", registration)

    def poll_control(self) -> ProcessInstruction | None:
        path = f"/internal/v1/agents/{self.settings.cluster_id}/control"
        response = self._request("GET", path, None, allow_no_content=True)
        return ProcessInstruction.model_validate(response) if response else None

    def acknowledge(self, event: AckEvent) -> None:
        path = (
            f"/internal/v1/agents/{self.settings.cluster_id}/commands/"
            f"{event.command_id}/ack"
        )
        self._request("POST", path, event)

    def send_event(self, event: CollectorEvent) -> None:
        path = (
            f"/internal/v1/agents/{self.settings.cluster_id}/processes/"
            f"{event.process_id}/events"
        )
        self._request("POST", path, event)

    def heartbeat(self, payload: dict[str, Any]) -> None:
        path = f"/internal/v1/agents/{self.settings.cluster_id}/heartbeat"
        self._request("POST", path, payload)

    def _request(
        self,
        method: str,
        path: str,
        payload: Any,
        allow_no_content: bool = False,
    ) -> dict[str, Any] | None:
        if payload is None:
            body = b""
        else:
            raw = payload.model_dump(mode="json") if hasattr(payload, "model_dump") else payload
            body = json.dumps(
                raw, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        timestamp = str(int(time.time()))
        signature = sign_request(
            self.settings.agent_token, timestamp, method, path, body
        )
        verify: bool | str = self.settings.master_verify_tls
        if self.settings.master_ca_file:
            verify = self.settings.master_ca_file
        response = requests.request(
            method,
            self.settings.master_url + path,
            data=body or None,
            headers={
                "Content-Type": "application/json",
                "X-Agent-Id": self.settings.cluster_id,
                "X-Agent-Timestamp": timestamp,
                "X-Agent-Signature": signature,
            },
            timeout=self.settings.request_timeout_seconds,
            verify=verify,
        )
        if allow_no_content and response.status_code == 204:
            return None
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise MasterProtocolError(
                f"master returned HTTP {response.status_code}"
            ) from exc
        if not response.content:
            return None
        try:
            value = response.json()
        except ValueError as exc:
            raise MasterProtocolError("master response is not JSON") from exc
        if not isinstance(value, dict):
            raise MasterProtocolError("master response must be a JSON object")
        return value
