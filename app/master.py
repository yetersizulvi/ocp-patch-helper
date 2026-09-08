from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse


CLUSTER_ID = os.getenv("MASTER_CLUSTER_ID", "local-cluster")
AGENT_TOKEN = os.getenv("AGENT_TOKEN", "")
MAX_CLOCK_SKEW_SECONDS = int(os.getenv("MASTER_MAX_CLOCK_SKEW_SECONDS", "120"))
MAX_EVENTS = int(os.getenv("MASTER_MAX_EVENTS", "2000"))
DEFAULT_DURATION_MINUTES = int(os.getenv("MASTER_DEFAULT_DURATION_MINUTES", "60"))
MAX_DURATION_MINUTES = int(os.getenv("MASTER_MAX_DURATION_MINUTES", "720"))
TARGET_TAG_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

LOCK = RLock()
STATE: dict[str, Any] = {
    "desired_state": "IDLE",
    "instruction": None,
    "registration": None,
    "heartbeat": None,
    "events": [],
    "acks": [],
    "runs": [],
    "monitoring": None,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def verify_agent_request(request: Request, body: bytes) -> None:
    if not AGENT_TOKEN:
        raise HTTPException(status_code=503, detail="AGENT_TOKEN is not configured")

    agent_id = request.headers.get("X-Agent-Id", "")
    timestamp = request.headers.get("X-Agent-Timestamp", "")
    signature = request.headers.get("X-Agent-Signature", "")

    if agent_id != CLUSTER_ID:
        raise HTTPException(status_code=401, detail="invalid agent id")

    try:
        timestamp_number = int(timestamp)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="invalid timestamp") from exc

    if abs(int(time.time()) - timestamp_number) > MAX_CLOCK_SKEW_SECONDS:
        raise HTTPException(status_code=401, detail="expired timestamp")

    body_digest = hashlib.sha256(body).hexdigest()
    canonical = "\n".join(
        (timestamp, request.method.upper(), request.url.path, body_digest)
    ).encode("utf-8")
    expected = hmac.new(
        AGENT_TOKEN.encode("utf-8"), canonical, hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=401, detail="invalid signature")


def _latest_event(collector_type: str) -> dict[str, Any] | None:
    for event in reversed(STATE["events"]):
        if event.get("collector_type") == collector_type:
            return event
    return None


def _mark_active_run(status: str) -> None:
    for run in reversed(STATE["runs"]):
        if run.get("status") == "RUNNING":
            run["status"] = status
            run["finished_at"] = utc_now()
            return


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _expire_monitoring_if_needed_locked() -> None:
    monitoring = STATE["monitoring"]
    if STATE["desired_state"] != "RUNNING" or not monitoring:
        return
    ends_at = _parse_timestamp(monitoring.get("ends_at"))
    if ends_at is None or datetime.now(timezone.utc) < ends_at:
        return

    previous = STATE["instruction"] or {}
    stopped_at = utc_now()
    STATE["desired_state"] = "STOPPED"
    STATE["instruction"] = {
        "command_id": new_id("cmd"),
        "desired_state": "STOPPED",
        "process_id": previous.get("process_id"),
        "lease_seconds": 120,
        "issued_at": stopped_at,
        "parameters": {"reason": "monitoring_duration_completed"},
    }
    monitoring["status"] = "COMPLETED"
    monitoring["stopped_at"] = stopped_at
    _mark_active_run("COMPLETED")


def _monitoring_snapshot_locked() -> dict[str, Any]:
    monitoring = STATE["monitoring"]
    if not monitoring:
        return {
            "status": "IDLE",
            "duration_seconds": 0,
            "elapsed_seconds": 0,
            "remaining_seconds": 0,
            "progress_percent": 0.0,
            "started_at": None,
            "ends_at": None,
            "stopped_at": None,
        }

    started_at = _parse_timestamp(monitoring.get("started_at"))
    ends_at = _parse_timestamp(monitoring.get("ends_at"))
    stopped_at = _parse_timestamp(monitoring.get("stopped_at"))
    now = datetime.now(timezone.utc)
    duration_seconds = max(1, int(monitoring.get("duration_seconds", 1)))
    elapsed_until = stopped_at or now
    elapsed_seconds = (
        max(0, min(duration_seconds, int((elapsed_until - started_at).total_seconds())))
        if started_at
        else 0
    )
    remaining_seconds = (
        max(0, int((ends_at - now).total_seconds()))
        if ends_at and STATE["desired_state"] == "RUNNING"
        else 0
    )
    return {
        **monitoring,
        "duration_seconds": duration_seconds,
        "elapsed_seconds": elapsed_seconds,
        "remaining_seconds": remaining_seconds,
        "progress_percent": round(elapsed_seconds / duration_seconds * 100, 2),
    }


def _agent_inventory_locked() -> list[dict[str, Any]]:
    registration = STATE["registration"]
    if not registration:
        return []
    heartbeat = STATE["heartbeat"] or {}
    received_at = heartbeat.get("received_at")
    received = _parse_timestamp(received_at)
    heartbeat_age_seconds = (
        max(0, int((datetime.now(timezone.utc) - received).total_seconds()))
        if received
        else None
    )
    if heartbeat_age_seconds is None:
        connection_status = "REGISTERED"
    elif heartbeat_age_seconds <= 45:
        connection_status = "ONLINE"
    else:
        connection_status = "STALE"
    return [
        {
            "agent_id": registration.get("cluster_id", CLUSTER_ID),
            "environment": registration.get("environment"),
            "namespace_pattern": registration.get("namespace_pattern"),
            "agent_version": registration.get("agent_version"),
            "flow_names": registration.get("flow_names", []),
            "capabilities": registration.get("capabilities", []),
            "registered_at": registration.get("received_at"),
            "last_heartbeat_at": received_at,
            "heartbeat_age_seconds": heartbeat_age_seconds,
            "connection_status": connection_status,
            "runtime_state": heartbeat.get("state", "UNKNOWN"),
            "process_id": heartbeat.get("process_id"),
            "target_tag": heartbeat.get("target_tag"),
            "last_error": heartbeat.get("last_error"),
            "collector_status": heartbeat.get("collector_status", {}),
        }
    ]


app = FastAPI(
    title="OpenShift AI Assistant Master",
    version="0.5.1",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return DASHBOARD_HTML


@app.get("/health")
async def health() -> dict[str, Any]:
    with LOCK:
        _expire_monitoring_if_needed_locked()
        return {
            "status": "ok",
            "cluster_id": CLUSTER_ID,
            "desired_state": STATE["desired_state"],
            "registered": STATE["registration"] is not None,
            "event_count": len(STATE["events"]),
            "monitoring": _monitoring_snapshot_locked(),
            "time": utc_now(),
        }


@app.get("/api/v1/state")
async def master_state() -> dict[str, Any]:
    with LOCK:
        _expire_monitoring_if_needed_locked()
        return {
            "cluster_id": CLUSTER_ID,
            "desired_state": STATE["desired_state"],
            "instruction": STATE["instruction"],
            "registration": STATE["registration"],
            "heartbeat": STATE["heartbeat"],
            "event_count": len(STATE["events"]),
            "ack_count": len(STATE["acks"]),
            "run_count": len(STATE["runs"]),
            "monitoring": _monitoring_snapshot_locked(),
        }


@app.get("/api/v1/summary")
async def summary() -> dict[str, Any]:
    with LOCK:
        _expire_monitoring_if_needed_locked()
        heartbeat = STATE["heartbeat"] or {}
        received_at = heartbeat.get("received_at")
        agent_status = "OFFLINE"
        age_seconds = None
        if received_at:
            try:
                received = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
                age_seconds = max(
                    0, int((datetime.now(timezone.utc) - received).total_seconds())
                )
                agent_status = "ONLINE" if age_seconds <= 45 else "STALE"
            except (TypeError, ValueError):
                agent_status = "UNKNOWN"

        image_event = _latest_event("image_rollout") or {}
        health_event = _latest_event("health_errors") or {}
        crash_event = _latest_event("crash_triage") or {}
        image_data = image_event.get("data", {})
        health_data = health_event.get("data", {})
        crash_data = crash_event.get("data", {})
        llm = crash_data.get("llm", {})

        return {
            "cluster_id": CLUSTER_ID,
            "agent_status": agent_status,
            "heartbeat_age_seconds": age_seconds,
            "runtime_state": heartbeat.get("state", STATE["desired_state"]),
            "process_id": heartbeat.get("process_id"),
            "target_tag": heartbeat.get("target_tag"),
            "image_total": image_data.get("image_total", 0),
            "image_matching": image_data.get("image_matching", 0),
            "coverage_percent": image_data.get("coverage_percent", 0),
            "mismatch_total": image_data.get("mismatch_total", 0),
            "health_error_total": health_data.get("error_total", 0),
            "crash_total": crash_data.get("crash_total", 0),
            "llm_available": llm.get("available"),
            "event_count": len(STATE["events"]),
            "last_heartbeat": received_at,
            "monitoring": _monitoring_snapshot_locked(),
        }


@app.post("/api/v1/start")
async def start_analysis(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid JSON") from exc

    target_tag = str(payload.get("target_tag", "")).strip()
    if not TARGET_TAG_PATTERN.fullmatch(target_tag):
        raise HTTPException(status_code=422, detail="valid target_tag is required")

    raw_duration = payload.get("duration_minutes", DEFAULT_DURATION_MINUTES)
    try:
        duration_minutes = int(raw_duration)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="duration_minutes must be an integer") from exc
    if isinstance(raw_duration, bool) or not 1 <= duration_minutes <= MAX_DURATION_MINUTES:
        raise HTTPException(
            status_code=422,
            detail=f"duration_minutes must be between 1 and {MAX_DURATION_MINUTES}",
        )

    process_id = str(
        payload.get("process_id") or f"{CLUSTER_ID}-{uuid.uuid4().hex[:12]}"
    )
    started = datetime.now(timezone.utc)
    ends = started + timedelta(minutes=duration_minutes)
    raw_parameters = payload.get("parameters", {})
    if not isinstance(raw_parameters, dict):
        raise HTTPException(status_code=422, detail="parameters must be an object")
    parameters = dict(raw_parameters)
    parameters["monitoring_duration_seconds"] = duration_minutes * 60
    instruction = {
        "command_id": new_id("cmd"),
        "desired_state": "RUNNING",
        "process_id": process_id,
        "flow_name": "monthly-ocp-patch",
        "target_tag": target_tag,
        "lease_seconds": 120,
        "issued_at": started.isoformat(),
        "parameters": parameters,
    }

    with LOCK:
        _mark_active_run("REPLACED")
        if STATE["monitoring"]:
            STATE["monitoring"]["status"] = "REPLACED"
            STATE["monitoring"]["stopped_at"] = started.isoformat()
        STATE["desired_state"] = "RUNNING"
        STATE["instruction"] = instruction
        STATE["events"] = []
        STATE["acks"] = []
        STATE["monitoring"] = {
            "status": "RUNNING",
            "duration_minutes": duration_minutes,
            "duration_seconds": duration_minutes * 60,
            "started_at": started.isoformat(),
            "ends_at": ends.isoformat(),
            "stopped_at": None,
        }
        STATE["runs"].append(
            {
                "process_id": process_id,
                "target_tag": target_tag,
                "flow_name": "monthly-ocp-patch",
                "status": "RUNNING",
                "duration_minutes": duration_minutes,
                "started_at": started.isoformat(),
                "ends_at": ends.isoformat(),
                "finished_at": None,
            }
        )
        STATE["runs"] = STATE["runs"][-100:]
    return instruction


@app.post("/api/v1/stop")
async def stop_analysis() -> dict[str, Any]:
    with LOCK:
        previous = STATE["instruction"] or {}
        instruction = {
            "command_id": new_id("cmd"),
            "desired_state": "STOPPED",
            "process_id": previous.get("process_id"),
            "lease_seconds": 120,
            "issued_at": utc_now(),
            "parameters": {},
        }
        STATE["desired_state"] = "STOPPED"
        STATE["instruction"] = instruction
        if STATE["monitoring"]:
            STATE["monitoring"]["status"] = "STOPPED"
            STATE["monitoring"]["stopped_at"] = instruction["issued_at"]
        _mark_active_run("STOPPED")
    return instruction


@app.get("/api/v1/events")
async def events() -> dict[str, Any]:
    with LOCK:
        return {"count": len(STATE["events"]), "events": list(STATE["events"])}


@app.get("/api/v1/heartbeats")
async def heartbeats() -> dict[str, Any]:
    with LOCK:
        return {"cluster_id": CLUSTER_ID, "heartbeat": STATE["heartbeat"]}


@app.get("/api/v1/acks")
async def acknowledgements() -> dict[str, Any]:
    with LOCK:
        return {"count": len(STATE["acks"]), "acks": list(STATE["acks"])}


@app.get("/api/v1/runs")
async def runs() -> dict[str, Any]:
    with LOCK:
        _expire_monitoring_if_needed_locked()
        return {"count": len(STATE["runs"]), "runs": list(reversed(STATE["runs"]))}


@app.get("/api/v1/agents")
async def agents() -> dict[str, Any]:
    with LOCK:
        inventory = _agent_inventory_locked()
        return {"count": len(inventory), "agents": inventory}


@app.get("/api/v1/stream")
async def live_stream(request: Request) -> StreamingResponse:
    async def generate():
        sequence = 0
        yield "retry: 3000\n\n"
        while not await request.is_disconnected():
            sequence += 1
            summary_payload = await summary()
            state_payload = await master_state()
            events_payload = await events()
            runs_payload = await runs()
            agents_payload = await agents()
            events_payload["events"] = events_payload["events"][-200:]
            events_payload["count"] = len(events_payload["events"])
            payload = {
                "summary": summary_payload,
                "state": state_payload,
                "events": events_payload,
                "runs": runs_payload,
                "agents": agents_payload,
                "streamed_at": utc_now(),
            }
            encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            yield f"id: {sequence}\nevent: snapshot\ndata: {encoded}\n\n"
            await asyncio.sleep(2)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/internal/v1/agents/register")
async def register_agent(request: Request) -> dict[str, str]:
    body = await request.body()
    verify_agent_request(request, body)
    payload = json.loads(body or b"{}")
    if payload.get("cluster_id") != CLUSTER_ID:
        raise HTTPException(status_code=409, detail="cluster_id mismatch")
    with LOCK:
        STATE["registration"] = {"received_at": utc_now(), **payload}
    return {"status": "registered", "cluster_id": CLUSTER_ID}


@app.get("/internal/v1/agents/{cluster_id}/control")
async def agent_control(cluster_id: str, request: Request) -> Any:
    verify_agent_request(request, b"")
    if cluster_id != CLUSTER_ID:
        raise HTTPException(status_code=404, detail="unknown cluster")
    with LOCK:
        _expire_monitoring_if_needed_locked()
        instruction = STATE["instruction"]
    return instruction if instruction else Response(status_code=204)


@app.post("/internal/v1/agents/{cluster_id}/commands/{command_id}/ack")
async def agent_ack(
    cluster_id: str, command_id: str, request: Request
) -> dict[str, str]:
    body = await request.body()
    verify_agent_request(request, body)
    if cluster_id != CLUSTER_ID:
        raise HTTPException(status_code=404, detail="unknown cluster")
    payload = json.loads(body or b"{}")
    with LOCK:
        STATE["acks"].append(
            {"received_at": utc_now(), "path_command_id": command_id, **payload}
        )
        STATE["acks"] = STATE["acks"][-100:]
    return {"status": "accepted"}


@app.post("/internal/v1/agents/{cluster_id}/heartbeat")
async def agent_heartbeat(cluster_id: str, request: Request) -> dict[str, str]:
    body = await request.body()
    verify_agent_request(request, body)
    if cluster_id != CLUSTER_ID:
        raise HTTPException(status_code=404, detail="unknown cluster")
    payload = json.loads(body or b"{}")
    with LOCK:
        STATE["heartbeat"] = {"received_at": utc_now(), **payload}
    return {"status": "accepted"}


@app.post("/internal/v1/agents/{cluster_id}/processes/{process_id}/events")
async def agent_event(
    cluster_id: str, process_id: str, request: Request
) -> dict[str, str]:
    body = await request.body()
    verify_agent_request(request, body)
    if cluster_id != CLUSTER_ID:
        raise HTTPException(status_code=404, detail="unknown cluster")
    payload = json.loads(body or b"{}")
    with LOCK:
        STATE["events"].append(
            {"received_at": utc_now(), "path_process_id": process_id, **payload}
        )
        STATE["events"] = STATE["events"][-MAX_EVENTS:]
    return {"status": "accepted"}


DASHBOARD_HTML = r'''<!doctype html>
<html lang="tr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OpenShift AI Assistant</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #081018;
      --panel: #101a24;
      --panel-2: #14212d;
      --line: #263746;
      --text: #edf5f7;
      --muted: #8fa5b2;
      --cyan: #41d7d0;
      --blue: #4aa5ff;
      --green: #40d68b;
      --yellow: #f4c95d;
      --red: #ff6b72;
      --shadow: 0 18px 50px rgba(0, 0, 0, .28);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at 15% -20%, rgba(65,215,208,.16), transparent 35%),
        radial-gradient(circle at 90% 5%, rgba(74,165,255,.12), transparent 28%),
        var(--bg);
      color: var(--text);
      font: 14px/1.5 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    button, input, select { font: inherit; }
    .shell { max-width: 1500px; margin: 0 auto; padding: 28px; }
    header { display: flex; justify-content: space-between; gap: 24px; align-items: center; margin-bottom: 24px; }
    .brand { display: flex; align-items: center; gap: 14px; }
    .logo {
      width: 44px; height: 44px; display: grid; place-items: center;
      border: 1px solid rgba(65,215,208,.5); border-radius: 13px;
      background: linear-gradient(135deg, rgba(65,215,208,.25), rgba(74,165,255,.1));
      color: var(--cyan); font-weight: 800; letter-spacing: -.06em;
    }
    h1 { margin: 0; font-size: 21px; letter-spacing: -.02em; }
    .subtitle, .muted { color: var(--muted); }
    .status-line { display: flex; align-items: center; gap: 9px; color: var(--muted); }
    .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--muted); box-shadow: 0 0 0 4px rgba(143,165,178,.1); }
    .dot.online { background: var(--green); box-shadow: 0 0 0 4px rgba(64,214,139,.12); }
    .dot.stale { background: var(--yellow); }
    .dot.offline { background: var(--red); }
    .grid { display: grid; gap: 16px; }
    .metrics { grid-template-columns: repeat(7, minmax(0, 1fr)); margin-bottom: 16px; }
    .main-grid { grid-template-columns: minmax(300px, .75fr) minmax(0, 1.65fr); }
    .card {
      background: linear-gradient(180deg, rgba(20,33,45,.96), rgba(13,24,34,.96));
      border: 1px solid var(--line); border-radius: 16px; box-shadow: var(--shadow);
    }
    .metric { padding: 17px; min-height: 112px; }
    .metric-label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }
    .metric-value { margin-top: 8px; font-size: 27px; font-weight: 750; letter-spacing: -.04em; overflow-wrap: anywhere; }
    .metric-note { margin-top: 3px; color: var(--muted); font-size: 12px; }
    .section { padding: 20px; }
    .section + .section { border-top: 1px solid var(--line); }
    .section-title { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 16px; }
    h2 { margin: 0; font-size: 15px; letter-spacing: -.01em; }
    label { display: block; color: var(--muted); margin: 12px 0 6px; font-size: 12px; }
    input, select {
      width: 100%; padding: 11px 12px; color: var(--text); background: #09131c;
      border: 1px solid var(--line); border-radius: 9px; outline: none;
    }
    input:focus, select:focus { border-color: var(--cyan); box-shadow: 0 0 0 3px rgba(65,215,208,.1); }
    .actions { display: flex; gap: 10px; margin-top: 16px; }
    button {
      border: 0; border-radius: 9px; padding: 10px 14px; cursor: pointer;
      color: #061014; background: var(--cyan); font-weight: 700;
    }
    button.secondary { color: var(--text); background: #263746; }
    button.danger { color: white; background: #9f3542; }
    button:disabled { opacity: .5; cursor: wait; }
    .pill { display: inline-flex; align-items: center; padding: 4px 8px; border-radius: 999px; background: #22313e; color: var(--muted); font-size: 11px; font-weight: 700; }
    .pill.good { color: var(--green); background: rgba(64,214,139,.1); }
    .pill.warn { color: var(--yellow); background: rgba(244,201,93,.1); }
    .pill.bad { color: var(--red); background: rgba(255,107,114,.1); }
    .tabs { display: flex; gap: 6px; border-bottom: 1px solid var(--line); padding: 0 20px; overflow-x: auto; }
    .tab { background: transparent; color: var(--muted); padding: 14px 10px; border-radius: 0; border-bottom: 2px solid transparent; white-space: nowrap; }
    .tab.active { color: var(--cyan); border-color: var(--cyan); }
    .tab-panel { display: none; padding: 20px; }
    .tab-panel.active { display: block; }
    .list { display: grid; gap: 9px; }
    .item { padding: 13px; border: 1px solid var(--line); border-radius: 11px; background: rgba(6,14,21,.45); }
    .item-head { display: flex; justify-content: space-between; gap: 14px; align-items: start; }
    .item-title { font-weight: 700; overflow-wrap: anywhere; }
    .item-meta { color: var(--muted); font-size: 12px; margin-top: 3px; }
    .empty { padding: 36px 16px; text-align: center; color: var(--muted); border: 1px dashed var(--line); border-radius: 12px; }
    .progress { height: 7px; background: #071119; border-radius: 999px; overflow: hidden; margin-top: 13px; }
    .progress > span { display: block; height: 100%; width: 0; background: linear-gradient(90deg, var(--blue), var(--cyan)); transition: width .35s ease; }
    .stream-state { display: inline-flex; align-items: center; gap: 7px; padding: 4px 8px; border: 1px solid var(--line); border-radius: 999px; font-size: 11px; }
    .stream-state.live { color: var(--green); border-color: rgba(64,214,139,.35); }
    .stream-state.retrying { color: var(--yellow); border-color: rgba(244,201,93,.35); }
    .live-list .item:first-child { border-color: rgba(65,215,208,.45); box-shadow: inset 3px 0 0 var(--cyan); }
    .panel-tools { display: flex; gap: 10px; align-items: center; margin-bottom: 14px; }
    .panel-tools select { max-width: 260px; }
    .insight { margin-bottom: 13px; padding: 14px; border: 1px solid var(--line); border-left: 4px solid var(--blue); border-radius: 10px; background: rgba(8,18,27,.7); }
    .insight.good { border-left-color: var(--green); }
    .insight.warn { border-left-color: var(--yellow); }
    .insight.bad { border-left-color: var(--red); }
    .insight-title { font-weight: 750; }
    .insight-text { color: var(--muted); margin-top: 3px; }
    .detail-actions { display: flex; justify-content: flex-end; margin: 8px 0; }
    .detail-actions button { padding: 6px 9px; font-size: 11px; }
    .analysis-grid { display: grid; gap: 10px; margin: 12px 0; }
    .analysis-block { padding: 12px; border-radius: 9px; background: #09131c; border: 1px solid var(--line); }
    .analysis-block h3 { margin: 0 0 6px; font-size: 12px; color: var(--cyan); text-transform: uppercase; letter-spacing: .06em; }
    .analysis-block ul { margin: 5px 0 0; padding-left: 20px; }
    .tag-list { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px; }
    .tag { padding: 3px 7px; border-radius: 999px; color: var(--muted); background: #22313e; font-size: 11px; }
    details { margin-top: 10px; }
    summary { cursor: pointer; color: var(--blue); }
    pre { max-height: 330px; overflow: auto; padding: 13px; border-radius: 9px; background: #071018; color: #c9d8df; font-size: 12px; white-space: pre-wrap; overflow-wrap: anywhere; }
    .toast { position: fixed; right: 24px; bottom: 24px; max-width: 420px; padding: 13px 16px; border-radius: 10px; background: #172632; border: 1px solid var(--line); box-shadow: var(--shadow); display: none; }
    .toast.show { display: block; }
    .toast.error { border-color: rgba(255,107,114,.7); color: #ffd4d6; }
    @media (max-width: 1100px) { .metrics { grid-template-columns: repeat(4, 1fr); } .main-grid { grid-template-columns: 1fr; } }
    @media (max-width: 650px) { .shell { padding: 16px; } header { align-items: flex-start; flex-direction: column; } .metrics { grid-template-columns: repeat(2, 1fr); } .metric-value { font-size: 22px; } .panel-tools { align-items: stretch; flex-direction: column; } .panel-tools select { max-width: none; } }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div class="brand">
        <div class="logo">OC</div>
        <div>
          <h1>OpenShift AI Assistant</h1>
          <div class="subtitle">Patch operasyon görünürlüğü ve read-only cluster analizi</div>
        </div>
      </div>
      <div class="status-line"><span id="streamState" class="stream-state retrying">STREAM bağlanıyor</span><span id="statusDot" class="dot"></span><span id="statusText">Bağlanıyor</span><span>•</span><span id="lastRefresh">—</span></div>
    </header>

    <section class="grid metrics">
      <div class="card metric"><div class="metric-label">Cluster</div><div id="clusterId" class="metric-value">—</div><div class="metric-note">Agent kaydı</div></div>
      <div class="card metric"><div class="metric-label">Agent</div><div id="runtimeState" class="metric-value">—</div><div id="heartbeatAge" class="metric-note">Heartbeat bekleniyor</div></div>
      <div class="card metric"><div class="metric-label">Kalan süre</div><div id="remainingTime" class="metric-value">00:00:00</div><div id="timeNote" class="metric-note">İzleme başlamadı</div><div class="progress"><span id="timeProgressBar"></span></div></div>
      <div class="card metric"><div class="metric-label">Image coverage</div><div id="coverage" class="metric-value">0%</div><div id="coverageNote" class="metric-note">0 / 0 image</div><div class="progress"><span id="coverageBar"></span></div></div>
      <div class="card metric"><div class="metric-label">Uyumsuz image</div><div id="mismatches" class="metric-value">0</div><div class="metric-note">Target tag dışında</div></div>
      <div class="card metric"><div class="metric-label">Health hatası</div><div id="healthErrors" class="metric-value">0</div><div class="metric-note">Pod, workload ve cluster</div></div>
      <div class="card metric"><div class="metric-label">Crash pod</div><div id="crashTotal" class="metric-value">0</div><div id="llmState" class="metric-note">LLM sonucu yok</div></div>
    </section>

    <section class="grid main-grid">
      <div class="card">
        <div class="section">
          <div class="section-title"><h2>Analiz kontrolü</h2><span id="desiredState" class="pill">IDLE</span></div>
          <label for="targetTag">Hedef image tag</label>
          <input id="targetTag" placeholder="Örnek: 1.4.1" autocomplete="off">
          <label for="processId">Process ID (opsiyonel)</label>
          <input id="processId" placeholder="Otomatik oluşturulur" autocomplete="off">
          <label for="durationMinutes">Canlı izleme süresi (dakika)</label>
          <input id="durationMinutes" type="number" min="1" max="720" step="1" value="60">
          <div class="actions">
            <button id="startButton" type="button">Analizi başlat</button>
            <button id="stopButton" class="danger" type="button">Durdur</button>
            <button id="exportButton" class="secondary" type="button">JSON indir</button>
          </div>
        </div>
        <div class="section">
          <div class="section-title"><h2>Aktif çalışma</h2></div>
          <div id="activeRun" class="empty">Aktif çalışma yok</div>
        </div>
        <div class="section">
          <div class="section-title"><h2>Son çalışmalar</h2><button id="refreshButton" class="secondary" type="button">Yenile</button></div>
          <div id="runs" class="list"></div>
        </div>
      </div>

      <div class="card">
        <div class="tabs">
          <button class="tab active" data-tab="live" type="button">Canlı akış</button>
          <button class="tab" data-tab="images" type="button">Image rollout</button>
          <button class="tab" data-tab="health" type="button">Health</button>
          <button class="tab" data-tab="crashes" type="button">Crash & LLM</button>
          <button class="tab" data-tab="agents" type="button">Agentlar</button>
          <button class="tab" data-tab="events" type="button">Tüm eventler</button>
        </div>
        <div id="live" class="tab-panel active">
          <div class="panel-tools">
            <select id="liveFilter" aria-label="Canlı akış filtresi">
              <option value="all">Tüm collectorlar</option>
              <option value="image_rollout">Image rollout</option>
              <option value="health_errors">Health</option>
              <option value="crash_triage">Crash & LLM</option>
            </select>
            <button id="pauseButton" class="secondary" type="button">Görünümü duraklat</button>
          </div>
          <div id="liveList" class="list live-list"></div>
        </div>
        <div id="images" class="tab-panel"><div id="imageList" class="list"></div></div>
        <div id="health" class="tab-panel"><div id="healthList" class="list"></div></div>
        <div id="crashes" class="tab-panel"><div id="crashList" class="list"></div></div>
        <div id="agents" class="tab-panel"><div id="agentList" class="list"></div></div>
        <div id="events" class="tab-panel"><div id="eventList" class="list"></div></div>
      </div>
    </section>
  </div>
  <div id="toast" class="toast"></div>

  <script>
    const byId = id => document.getElementById(id);
    const state = {
      events: [], busy: false, snapshot: null, streamConnected: false,
      renderPaused: false, pendingSnapshot: null, openDetails: new Set(),
      eventSignature: '', runSignature: '', agentSignature: ''
    };

    const COLLECTOR_LABELS = {
      image_rollout: 'Image rollout',
      health_errors: 'Platform health',
      crash_triage: 'Crash & LLM'
    };

    function text(value, fallback = '—') {
      return value === null || value === undefined || value === '' ? fallback : String(value);
    }

    function dateText(value) {
      if (!value) return '—';
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString('tr-TR');
    }

    function durationText(value) {
      const seconds = Math.max(0, Number(value || 0));
      const hours = Math.floor(seconds / 3600);
      const minutes = Math.floor((seconds % 3600) / 60);
      const rest = Math.floor(seconds % 60);
      return [hours, minutes, rest].map(part => String(part).padStart(2, '0')).join(':');
    }

    function badge(value, tone = '') {
      const span = document.createElement('span');
      span.className = `pill ${tone}`;
      span.textContent = text(value);
      return span;
    }

    function empty(message) {
      const div = document.createElement('div');
      div.className = 'empty';
      div.textContent = message;
      return div;
    }

    function insight(title, message, tone = '') {
      const root = document.createElement('div');
      root.className = `insight ${tone}`;
      const heading = document.createElement('div');
      heading.className = 'insight-title';
      heading.textContent = title;
      const content = document.createElement('div');
      content.className = 'insight-text';
      content.textContent = message;
      root.append(heading, content);
      return root;
    }

    function jsonDetails(payload, detailKey) {
      const details = document.createElement('details');
      const summary = document.createElement('summary');
      const actions = document.createElement('div');
      const copyButton = document.createElement('button');
      const pre = document.createElement('pre');
      details.dataset.detailKey = detailKey;
      details.open = state.openDetails.has(detailKey);
      summary.textContent = 'JSON detayını göster';
      actions.className = 'detail-actions';
      copyButton.type = 'button';
      copyButton.className = 'secondary';
      copyButton.textContent = 'JSON kopyala';
      copyButton.addEventListener('click', async event => {
        event.preventDefault();
        try {
          await navigator.clipboard.writeText(JSON.stringify(payload, null, 2));
          showToast('JSON panoya kopyalandı.');
        } catch (_) {
          showToast('Tarayıcı panoya erişemedi.', true);
        }
      });
      pre.textContent = JSON.stringify(payload, null, 2);
      actions.append(copyButton);
      details.addEventListener('toggle', () => {
        if (details.open) state.openDetails.add(detailKey);
        else state.openDetails.delete(detailKey);
      });
      details.append(summary, actions, pre);
      return details;
    }

    function item(title, meta, payload, tone = '', detailKey = '') {
      const root = document.createElement('div');
      root.className = 'item';
      const head = document.createElement('div');
      head.className = 'item-head';
      const titleWrap = document.createElement('div');
      const titleNode = document.createElement('div');
      titleNode.className = 'item-title';
      titleNode.textContent = title;
      const metaNode = document.createElement('div');
      metaNode.className = 'item-meta';
      metaNode.textContent = meta;
      titleWrap.append(titleNode, metaNode);
      head.append(titleWrap, badge(tone || 'detay', tone === 'OK' ? 'good' : tone ? 'warn' : ''));
      root.append(head);
      if (payload) root.append(jsonDetails(payload, detailKey || `${title}|${meta}`));
      return root;
    }

    function appendAnalysisBlock(target, title, value) {
      if (value === null || value === undefined || value === '' || (Array.isArray(value) && !value.length)) return;
      const block = document.createElement('div');
      const heading = document.createElement('h3');
      heading.textContent = title;
      block.className = 'analysis-block';
      block.append(heading);
      if (Array.isArray(value)) {
        const list = document.createElement('ul');
        value.forEach(entry => {
          const row = document.createElement('li');
          row.textContent = typeof entry === 'string' ? entry : JSON.stringify(entry);
          list.append(row);
        });
        block.append(list);
      } else {
        const content = document.createElement('div');
        content.textContent = typeof value === 'string' ? value : JSON.stringify(value);
        block.append(content);
      }
      target.append(block);
    }

    function showToast(message, isError = false) {
      const toast = byId('toast');
      toast.textContent = message;
      toast.className = `toast show${isError ? ' error' : ''}`;
      clearTimeout(showToast.timer);
      showToast.timer = setTimeout(() => toast.className = 'toast', 5000);
    }

    async function api(path, options = {}) {
      const response = await fetch(path, {
        ...options,
        headers: { 'Content-Type': 'application/json', ...(options.headers || {}) }
      });
      const raw = await response.text();
      let payload = {};
      if (raw) {
        try { payload = JSON.parse(raw); } catch (_) { payload = { detail: raw }; }
      }
      if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
      return payload;
    }

    function latest(type) {
      return [...state.events].reverse().find(event => event.collector_type === type);
    }

    function renderSummary(summary, masterState) {
      const monitoring = summary.monitoring || masterState.monitoring || {};
      byId('clusterId').textContent = text(summary.cluster_id);
      byId('runtimeState').textContent = text(summary.runtime_state);
      byId('heartbeatAge').textContent = summary.heartbeat_age_seconds === null
        ? 'Heartbeat alınmadı'
        : `${summary.heartbeat_age_seconds} sn önce heartbeat`;
      byId('coverage').textContent = `${Number(summary.coverage_percent || 0).toFixed(1)}%`;
      byId('coverageNote').textContent = `${summary.image_matching || 0} / ${summary.image_total || 0} image`;
      byId('coverageBar').style.width = `${Math.min(100, Math.max(0, summary.coverage_percent || 0))}%`;
      byId('mismatches').textContent = summary.mismatch_total || 0;
      byId('healthErrors').textContent = summary.health_error_total || 0;
      byId('crashTotal').textContent = summary.crash_total || 0;
      byId('llmState').textContent = summary.llm_available === true ? 'LLM analizi hazır' : summary.llm_available === false ? 'LLM çağrısı başarısız' : 'LLM sonucu yok';
      byId('remainingTime').textContent = durationText(monitoring.remaining_seconds);
      byId('timeNote').textContent = monitoring.status === 'RUNNING'
        ? `${durationText(monitoring.elapsed_seconds)} geçti • ${monitoring.duration_minutes || 0} dk oturum`
        : monitoring.status === 'COMPLETED' ? 'İzleme süresi tamamlandı' : 'İzleme aktif değil';
      byId('timeProgressBar').style.width = `${Math.min(100, Math.max(0, monitoring.progress_percent || 0))}%`;
      byId('desiredState').textContent = masterState.desired_state || 'IDLE';
      byId('desiredState').className = `pill ${masterState.desired_state === 'RUNNING' ? 'good' : ''}`;
      byId('statusText').textContent = `${summary.agent_status} • ${summary.cluster_id}`;
      byId('statusDot').className = `dot ${String(summary.agent_status || 'offline').toLowerCase()}`;
      byId('lastRefresh').textContent = new Date().toLocaleTimeString('tr-TR');

      const active = byId('activeRun');
      active.replaceChildren();
      if (masterState.instruction) {
        active.className = 'list';
        active.append(item(
          text(masterState.instruction.process_id, 'Process bekleniyor'),
          `Target: ${text(masterState.instruction.target_tag)} • ${text(masterState.instruction.desired_state)} • kalan ${durationText(monitoring.remaining_seconds)}`,
          masterState.instruction,
          masterState.instruction.desired_state,
          `active:${text(masterState.instruction.process_id)}`
        ));
      } else {
        active.className = 'empty';
        active.textContent = 'Aktif çalışma yok';
      }
    }

    function renderLive() {
      const target = byId('liveList');
      target.replaceChildren();
      const filter = byId('liveFilter').value;
      const visible = [...state.events].reverse().filter(event => filter === 'all' || event.collector_type === filter).slice(0, 50);
      target.append(insight(
        'Canlı operasyon akışı',
        state.renderPaused ? 'Görünüm duraklatıldı. Yeni veriler arka planda alınmaya devam ediyor.' : 'Yeni collector sonuçları geldikçe liste güncellenir. Açtığınız JSON paneli açık kalır.',
        state.renderPaused ? 'warn' : 'good'
      ));
      if (!visible.length) return target.append(empty('Seçilen collector için veri bekleniyor.'));
      visible.forEach(event => {
        const data = event.data || {};
        const summary = event.collector_type === 'image_rollout'
          ? `${data.image_matching || 0}/${data.image_total || 0} image uyumlu • ${data.mismatch_total || 0} uyumsuz`
          : event.collector_type === 'health_errors'
            ? `${data.error_total || 0} health problemi`
            : `${data.crash_total || 0} crash pod • LLM ${data.llm?.available === true ? 'hazır' : 'bekliyor'}`;
        target.append(item(
          `${COLLECTOR_LABELS[event.collector_type] || text(event.collector_type)} • ölçüm ${text(event.sequence)}`,
          `${dateText(event.received_at || event.collected_at)} • ${summary}`,
          event,
          event.event_type === 'collector_error' ? 'ERROR' : 'LIVE',
          `live:${text(event.process_id)}:${text(event.collector_id)}:${text(event.sequence)}:${text(event.fingerprint)}`
        ));
      });
    }

    function renderImages() {
      const target = byId('imageList');
      target.replaceChildren();
      const event = latest('image_rollout');
      const data = event?.data || {};
      const mismatches = event?.data?.mismatches || [];
      if (!event) return target.append(empty('Henüz image rollout eventi alınmadı.'));
      if (!data.image_total) {
        target.append(insight('Takip edilecek application image bulunamadı', 'Registry prefix ile eşleşen Deployment, StatefulSet veya DaemonSet container image bulunamadı. PATCH_AGENT_REGISTRY_PREFIX değerini kontrol edin.', 'warn'));
        return;
      }
      target.append(insight(
        mismatches.length ? 'Rollout henüz tamamlanmadı' : 'Tüm application image’ları hedef sürümde',
        `Hedef ${text(data.target_tag || event.target_tag)}. ${data.image_matching || 0}/${data.image_total || 0} image uyumlu, coverage %${Number(data.coverage_percent || 0).toFixed(1)}.`,
        mismatches.length ? 'warn' : 'good'
      ));
      if (!mismatches.length) return;
      mismatches.forEach(row => target.append(item(
        (row.workload || 'Bilinmeyen workload').split('|').join(' / '),
        `${text(row.container)} container’ı halen ${text(row.tag)} kullanıyor; beklenen ${text(event.target_tag)}.`,
        row,
        'BEKLİYOR',
        `image:${text(row.workload)}:${text(row.container)}:${text(row.reference)}`
      )));
    }

    function healthMessage(row) {
      if (row.type === 'pod') return `Pod hazır değil: ${(row.issues || []).join(', ')}. Restart sayısı ${row.restart_count || 0}.`;
      if (row.type === 'workload') return `${text(row.kind)} replica durumu ${row.ready || 0}/${row.desired || 0}; unavailable ${row.unavailable || 0}.`;
      if (row.type === 'node') return `Node problemi: ${text(row.issue)}.`;
      if (row.type === 'cluster_operator') return `Operator available=${row.available}, degraded=${row.degraded}. ${text(row.reason, '')}`;
      if (row.type === 'cluster_version') return `ClusterVersion failing durumda. ${text(row.reason, '')}`;
      if (row.type === 'machine_config_pool') return `MachineConfigPool degraded=${row.degraded}; unavailable machine ${row.unavailable || 0}.`;
      return text(row.issue || row.reason || row.message, 'Detay için JSON kaydını açın.');
    }

    function renderHealth() {
      const target = byId('healthList');
      target.replaceChildren();
      const event = latest('health_errors');
      const errors = event?.data?.errors || [];
      if (!event) return target.append(empty('Henüz health eventi alınmadı.'));
      if (!errors.length) {
        target.append(insight('Platform health temiz', 'İzlenen pod, workload, node, ClusterOperator, ClusterVersion ve MachineConfigPool kaynaklarında problem bulunmadı.', 'good'));
        return;
      }
      const counts = errors.reduce((result, row) => {
        result[row.type] = (result[row.type] || 0) + 1;
        return result;
      }, {});
      const distribution = Object.entries(counts).map(([type, count]) => `${type}: ${count}`).join(' • ');
      target.append(insight('Platform problemi tespit edildi', `${errors.length} problem: ${distribution}. Önce node/operator/MCP, ardından workload ve pod kayıtlarını inceleyin.`, 'bad'));
      errors.forEach(row => target.append(item(
        `${text(row.type).replaceAll('_', ' ')} • ${text(row.namespace, 'cluster')}/${text(row.name)}`,
        healthMessage(row),
        row,
        ['node', 'cluster_operator', 'cluster_version', 'machine_config_pool'].includes(row.type) ? 'KRİTİK' : 'UYARI',
        `health:${text(row.type)}:${text(row.namespace, 'cluster')}:${text(row.name)}:${text(row.issue || row.reason)}`
      )));
    }

    function renderCrashes() {
      const target = byId('crashList');
      target.replaceChildren();
      const event = latest('crash_triage');
      const crashes = event?.data?.crashes || [];
      const llm = event?.data?.llm;
      if (!event) return target.append(empty('Henüz crash triage eventi alınmadı.'));

      if (llm) {
        const analysis = llm.analysis || {};
        const llmItem = item(
          llm.available ? 'LLM değerlendirmesi hazır' : 'LLM çağrısı başarısız',
          analysis.summary || llm.error || 'LLM cevabı alınamadı. Deterministik crash verisi aşağıda gösterilmeye devam eder.',
          llm,
          llm.available ? 'LLM' : 'ERROR',
          `llm:${text(event.process_id)}:${text(event.fingerprint)}`
        );
        if (llm.available) {
          const grid = document.createElement('div');
          grid.className = 'analysis-grid';
          appendAnalysisBlock(grid, 'Özet', analysis.summary);
          appendAnalysisBlock(grid, 'Incident grupları', analysis.incident_groups);
          appendAnalysisBlock(grid, 'Olası nedenler', analysis.likely_causes);
          appendAnalysisBlock(grid, 'Read-only kontroller', analysis.read_only_checks);
          appendAnalysisBlock(grid, 'Operatör notu', analysis.operator_note);
          appendAnalysisBlock(grid, 'Güven seviyesi', analysis.confidence);
          llmItem.insertBefore(grid, llmItem.querySelector('details'));
        }
        target.append(llmItem);
      }
      if (!crashes.length) {
        target.append(insight('Crash pod bulunmadı', 'CrashLoopBackOff, ImagePullBackOff, container creation error veya OOMKilled durumu tespit edilmedi.', 'good'));
        return;
      }
      target.append(insight('Crash müdahalesi gerekiyor', `${crashes.length} crash pod bulundu. LLM yorumu yardımcıdır; pod status ve Warning Event verileri otoritatif kaynaktır.`, 'bad'));
      crashes.forEach(row => target.append(item(
        `${text(row.namespace)}/${text(row.pod)}`,
        `${(row.issues || []).join(', ') || 'Crash'} • node ${text(row.node)} • restart ${sumRestarts(row.containers || [])}`,
        row,
        'CRASH',
        `crash:${text(row.uid, `${text(row.namespace)}/${text(row.pod)}`)}`
      )));
    }

    function sumRestarts(containers) {
      return containers.reduce((total, container) => total + Number(container.restart_count || 0), 0);
    }

    function renderAgents(agentsPayload) {
      const target = byId('agentList');
      target.replaceChildren();
      const agents = agentsPayload?.agents || [];
      if (!agents.length) return target.append(insight('Bağlı agent yok', 'Master henüz bir agent registration isteği almadı.', 'warn'));
      target.append(insight('Agent envanteri', `${agents.length} agent kayıtlı. Bu sürüm tek-cluster master modelini kullanır; registration ve heartbeat bilgileri burada izlenir.`, 'good'));
      agents.forEach(agent => {
        const agentItem = item(
          `${text(agent.agent_id)} • ${text(agent.environment).toUpperCase()}`,
          `${text(agent.connection_status)} • ${text(agent.runtime_state)} • heartbeat ${agent.heartbeat_age_seconds === null ? 'bekleniyor' : `${agent.heartbeat_age_seconds} sn önce`}`,
          agent,
          agent.connection_status === 'ONLINE' ? 'ONLINE' : agent.connection_status,
          `agent:${text(agent.agent_id)}`
        );
        const tags = document.createElement('div');
        tags.className = 'tag-list';
        [
          `version ${text(agent.agent_version)}`,
          `namespace ${text(agent.namespace_pattern)}`,
          `target ${text(agent.target_tag)}`,
          ...(agent.flow_names || []).map(flow => `flow ${flow}`),
          ...(agent.capabilities || []).map(capability => capability)
        ].forEach(value => {
          const tag = document.createElement('span');
          tag.className = 'tag';
          tag.textContent = value;
          tags.append(tag);
        });
        agentItem.insertBefore(tags, agentItem.querySelector('details'));
        target.append(agentItem);
      });
    }

    function renderEvents() {
      const target = byId('eventList');
      target.replaceChildren();
      if (!state.events.length) return target.append(empty('Henüz event alınmadı.'));
      [...state.events].reverse().forEach(event => target.append(item(
        `${COLLECTOR_LABELS[event.collector_type] || text(event.collector_type)} • ölçüm ${text(event.sequence)}`,
        `${dateText(event.received_at || event.collected_at)} • ${text(event.process_id)}`,
        event,
        event.event_type === 'collector_error' ? 'ERROR' : 'OK',
        `event:${text(event.process_id)}:${text(event.collector_id)}:${text(event.sequence)}:${text(event.fingerprint)}`
      )));
    }

    function renderRuns(runs) {
      const target = byId('runs');
      target.replaceChildren();
      if (!runs.length) return target.append(empty('Henüz çalışma başlatılmadı.'));
      runs.slice(0, 8).forEach(run => target.append(item(
        run.process_id,
        `Target: ${run.target_tag} • ${run.duration_minutes || '—'} dk • ${dateText(run.started_at)}`,
        run,
        run.status,
        `run:${text(run.process_id)}`
      )));
    }

    function applySnapshot(payload) {
      if (state.renderPaused) {
        state.pendingSnapshot = payload;
        return;
      }
      state.snapshot = payload;
      renderSummary(payload.summary || {}, payload.state || {});
      const nextEvents = payload.events?.events || [];
      const lastEvent = nextEvents.length ? nextEvents[nextEvents.length - 1] : {};
      const nextEventSignature = `${nextEvents.length}:${text(lastEvent.fingerprint, '')}:${text(lastEvent.received_at, '')}`;
      if (nextEventSignature !== state.eventSignature) {
        state.events = nextEvents;
        state.eventSignature = nextEventSignature;
        renderLive();
        renderImages();
        renderHealth();
        renderCrashes();
        renderEvents();
      }
      const nextRuns = payload.runs?.runs || [];
      const nextRunSignature = JSON.stringify(nextRuns);
      if (nextRunSignature !== state.runSignature) {
        state.runSignature = nextRunSignature;
        renderRuns(nextRuns);
      }
      const nextAgents = payload.agents || { count: 0, agents: [] };
      const nextAgentSignature = JSON.stringify(nextAgents);
      if (nextAgentSignature !== state.agentSignature) {
        state.agentSignature = nextAgentSignature;
        renderAgents(nextAgents);
      }
    }

    async function refresh() {
      if (state.busy) return;
      state.busy = true;
      try {
        const [summary, masterState, events, runs, agents] = await Promise.all([
          api('/api/v1/summary'), api('/api/v1/state'), api('/api/v1/events'), api('/api/v1/runs'), api('/api/v1/agents')
        ]);
        applySnapshot({ summary, state: masterState, events, runs, agents, streamed_at: new Date().toISOString() });
      } catch (error) {
        byId('statusText').textContent = 'Master API erişilemiyor';
        byId('statusDot').className = 'dot offline';
        showToast(error.message, true);
      } finally {
        state.busy = false;
      }
    }

    async function startRun() {
      const targetTag = byId('targetTag').value.trim();
      const processId = byId('processId').value.trim();
      const durationMinutes = Number(byId('durationMinutes').value);
      if (!targetTag) return showToast('Target tag zorunlu.', true);
      if (!Number.isInteger(durationMinutes) || durationMinutes < 1 || durationMinutes > 720) return showToast('İzleme süresi 1-720 dakika olmalı.', true);
      try {
        byId('startButton').disabled = true;
        await api('/api/v1/start', {
          method: 'POST',
          body: JSON.stringify({ target_tag: targetTag, duration_minutes: durationMinutes, ...(processId ? { process_id: processId } : {}) })
        });
        showToast('Analiz başlatma komutu oluşturuldu.');
        await refresh();
      } catch (error) { showToast(error.message, true); }
      finally { byId('startButton').disabled = false; }
    }

    function exportReport() {
      const report = state.pendingSnapshot || state.snapshot;
      if (!report) return showToast('İndirilecek veri henüz yok.', true);
      const blob = new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      const processId = report.state?.instruction?.process_id || 'snapshot';
      link.href = url;
      link.download = `openshift-analysis-${processId}.json`;
      document.body.append(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
      showToast('JSON raporu indirildi.');
    }

    function updateStreamLabel() {
      if (state.renderPaused) {
        byId('streamState').textContent = 'GÖRÜNÜM duraklatıldı';
        byId('streamState').className = 'stream-state retrying';
      } else if (state.streamConnected) {
        byId('streamState').textContent = 'STREAM canlı';
        byId('streamState').className = 'stream-state live';
      } else {
        byId('streamState').textContent = 'STREAM yeniden bağlanıyor';
        byId('streamState').className = 'stream-state retrying';
      }
    }

    function togglePause() {
      state.renderPaused = !state.renderPaused;
      byId('pauseButton').textContent = state.renderPaused ? 'Görünümü devam ettir' : 'Görünümü duraklat';
      updateStreamLabel();
      if (!state.renderPaused && state.pendingSnapshot) {
        const pending = state.pendingSnapshot;
        state.pendingSnapshot = null;
        applySnapshot(pending);
      }
      renderLive();
    }

    function connectStream() {
      const stream = new EventSource('/api/v1/stream');
      stream.addEventListener('snapshot', event => {
        try {
          state.streamConnected = true;
          updateStreamLabel();
          applySnapshot(JSON.parse(event.data));
        } catch (error) {
          showToast(`Stream verisi okunamadı: ${error.message}`, true);
        }
      });
      stream.onopen = () => {
        state.streamConnected = true;
        updateStreamLabel();
      };
      stream.onerror = () => {
        state.streamConnected = false;
        updateStreamLabel();
      };
    }

    async function stopRun() {
      try {
        byId('stopButton').disabled = true;
        await api('/api/v1/stop', { method: 'POST', body: '{}' });
        showToast('Durdurma komutu oluşturuldu.');
        await refresh();
      } catch (error) { showToast(error.message, true); }
      finally { byId('stopButton').disabled = false; }
    }

    document.querySelectorAll('.tab').forEach(tab => tab.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(node => node.classList.remove('active'));
      document.querySelectorAll('.tab-panel').forEach(node => node.classList.remove('active'));
      tab.classList.add('active');
      byId(tab.dataset.tab).classList.add('active');
    }));
    byId('startButton').addEventListener('click', startRun);
    byId('stopButton').addEventListener('click', stopRun);
    byId('refreshButton').addEventListener('click', refresh);
    byId('exportButton').addEventListener('click', exportReport);
    byId('pauseButton').addEventListener('click', togglePause);
    byId('liveFilter').addEventListener('change', renderLive);
    refresh();
    connectStream();
    setInterval(() => { if (!state.streamConnected) refresh(); }, 15000);
  </script>
</body>
</html>'''
