from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import ValidationError

from app.models import FlowDocument


class ConfigurationError(RuntimeError):
    pass


VALID_ENVIRONMENTS = {"test", "beta", "prod"}


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigurationError(f"{name} is required")
    return value


def _bool_env(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    return max(minimum, min(maximum, value))


@dataclass(frozen=True)
class AgentSettings:
    cluster_id: str
    environment: str
    namespace_pattern: str
    master_url: str
    agent_token: str
    master_ca_file: str | None
    master_verify_tls: bool
    request_timeout_seconds: int
    flow_config_path: str
    llm_url: str
    llm_token: str | None
    llm_model: str
    llm_ca_file: str | None
    llm_timeout_seconds: int
    registry_prefix: str | None
    log_level: str

    @classmethod
    def from_env(cls) -> "AgentSettings":
        environment = _required("PATCH_AGENT_ENVIRONMENT").lower()
        if environment not in VALID_ENVIRONMENTS:
            raise ConfigurationError(
                "PATCH_AGENT_ENVIRONMENT must be test, beta or prod"
            )
        namespace_pattern = _required("PATCH_AGENT_NAMESPACE_PATTERN")
        try:
            re.compile(namespace_pattern)
        except re.error as exc:
            raise ConfigurationError(
                f"invalid PATCH_AGENT_NAMESPACE_PATTERN: {exc}"
            ) from exc

        master_url = _required("PATCH_AGENT_MASTER_URL").rstrip("/")
        allow_http = _bool_env("PATCH_AGENT_ALLOW_HTTP", False)
        if not master_url.startswith("https://") and not (
            allow_http and master_url.startswith("http://")
        ):
            raise ConfigurationError("PATCH_AGENT_MASTER_URL must use https")

        return cls(
            cluster_id=_required("PATCH_AGENT_CLUSTER_ID"),
            environment=environment,
            namespace_pattern=namespace_pattern,
            master_url=master_url,
            agent_token=_required("PATCH_AGENT_TOKEN"),
            master_ca_file=os.getenv("PATCH_AGENT_MASTER_CA_FILE"),
            master_verify_tls=not _bool_env("PATCH_AGENT_MASTER_TLS_INSECURE", False),
            request_timeout_seconds=_int_env(
                "PATCH_AGENT_REQUEST_TIMEOUT_SECONDS", 5, 1, 30
            ),
            flow_config_path=os.getenv(
                "PATCH_AGENT_FLOW_CONFIG", "/etc/patch-agent/flow/flows.yaml"
            ),
            llm_url=os.getenv("LLM_URL", ""),
            llm_token=os.getenv("LLM_TOKEN"),
            llm_model=os.getenv("LLM_MODEL", ""),
            llm_ca_file=os.getenv("LLM_CA_FILE"),
            llm_timeout_seconds=_int_env("LLM_TIMEOUT_SECONDS", 30, 1, 120),
            registry_prefix=os.getenv("PATCH_AGENT_REGISTRY_PREFIX") or None,
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )


def load_flows(path: str | Path) -> dict[str, FlowDocument]:
    config_path = Path(path)
    if not config_path.is_file():
        raise ConfigurationError(f"flow config not found: {config_path}")
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        documents = raw.get("flows", [])
        flows = [FlowDocument.model_validate(item) for item in documents]
    except (OSError, yaml.YAMLError, ValidationError) as exc:
        raise ConfigurationError(f"invalid flow config: {exc}") from exc
    if not flows:
        raise ConfigurationError("flow config must contain at least one flow")
    result = {flow.metadata.name: flow for flow in flows}
    if len(result) != len(flows):
        raise ConfigurationError("flow names must be unique")
    return result
