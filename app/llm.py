from __future__ import annotations

import json
from typing import Any

import requests

from app.config import AgentSettings


SYSTEM_PROMPT = """
You are openshift-crash-triage-v1, a bounded OpenShift patch diagnostic agent.
Analyze only the supplied failing pod evidence. Do not invent facts, do not
claim you ran a command, and do not recommend automatic mutation. Return strict
JSON in Turkish with keys: summary, incident_groups, likely_causes,
read_only_checks, operator_note, confidence. Keep the answer concise. The
deterministic collector remains authoritative.
""".strip()


class CrashLLMClient:
    def __init__(self, settings: AgentSettings) -> None:
        self.url = settings.llm_url
        self.token = settings.llm_token
        self.model = settings.llm_model
        self.timeout = settings.llm_timeout_seconds
        self.ca_file = settings.llm_ca_file

    @property
    def configured(self) -> bool:
        return bool(self.url and self.token and self.model)

    def analyze(
        self,
        cluster_id: str,
        environment: str,
        process_id: str,
        target_tag: str,
        crash_data: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.configured:
            return {
                "available": False,
                "error": "LLM_URL, LLM_TOKEN and LLM_MODEL must be configured",
            }
        compact = {
            "cluster_id": cluster_id,
            "environment": environment,
            "process_id": process_id,
            "target_tag": target_tag,
            "crash_total": crash_data.get("crash_total", 0),
            "crashes": crash_data.get("crashes", []),
        }
        verify: bool | str = self.ca_file if self.ca_file else True
        try:
            response = requests.post(
                self.url,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "temperature": 0.1,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {
                            "role": "user",
                            "content": json.dumps(compact, ensure_ascii=False),
                        },
                    ],
                },
                timeout=self.timeout,
                verify=verify,
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                lines = content.splitlines()
                content = "\n".join(lines[1:-1])
                if content.startswith("json\n"):
                    content = content[5:]
            parsed = json.loads(content)
            if not isinstance(parsed, dict):
                raise ValueError("LLM response must be a JSON object")
            return {"available": True, "analysis": parsed}
        except (
            requests.RequestException,
            KeyError,
            IndexError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            return {
                "available": False,
                "error": f"{exc.__class__.__name__}: LLM request failed",
            }
