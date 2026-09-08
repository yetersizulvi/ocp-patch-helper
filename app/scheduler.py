from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from app.collectors import CollectorEngine, fingerprint
from app.config import AgentSettings
from app.llm import CrashLLMClient
from app.models import (
    CollectorConfig,
    CollectorEvent,
    CollectorType,
    FlowDocument,
    ProcessInstruction,
    utc_now,
)
from app.transport import MasterClient


LOGGER = logging.getLogger(__name__)


class FlowRunner:
    def __init__(
        self,
        settings: AgentSettings,
        instruction: ProcessInstruction,
        flow: FlowDocument,
        engine: CollectorEngine,
        llm: CrashLLMClient,
        master: MasterClient,
    ) -> None:
        assert instruction.process_id
        assert instruction.flow_name
        assert instruction.target_tag
        self.settings = settings
        self.instruction = instruction
        self.flow = flow
        self.engine = engine
        self.llm = llm
        self.master = master
        self.process_id = instruction.process_id
        self.flow_name = instruction.flow_name
        self.target_tag = instruction.target_tag
        self._tasks: list[asyncio.Task[None]] = []
        self._stop = asyncio.Event()
        self._sequence = 0
        self._last_sent_fingerprint: dict[str, str] = {}
        self.status: dict[str, dict[str, Any]] = {}

    def start(self) -> None:
        if self._tasks:
            return
        for collector in self.flow.spec.collectors:
            if not collector.enabled:
                continue
            self.status[collector.id] = {
                "type": collector.type.value,
                "interval_seconds": collector.interval_seconds,
                "state": "scheduled",
                "last_started_at": None,
                "last_finished_at": None,
                "last_duration_ms": None,
                "last_error": None,
                "last_changed": None,
            }
            self._tasks.append(
                asyncio.create_task(
                    self._collector_loop(collector),
                    name=f"collector-{collector.id}",
                )
            )

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    async def _collector_loop(self, collector: CollectorConfig) -> None:
        try:
            if collector.initial_delay_seconds:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=collector.initial_delay_seconds
                )
                return
        except asyncio.TimeoutError:
            pass

        while not self._stop.is_set():
            started = time.monotonic()
            state = self.status[collector.id]
            state["state"] = "running"
            state["last_started_at"] = utc_now().isoformat()
            try:
                await self._run_once(collector)
                state["last_error"] = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state["last_error"] = f"{exc.__class__.__name__}: collector failed"
                LOGGER.warning(
                    "Collector %s failed: %s", collector.id, exc.__class__.__name__
                )
                try:
                    await self._send_collector_error(collector, exc)
                except Exception:
                    LOGGER.warning("Collector %s error event could not be sent", collector.id)
            duration_ms = int((time.monotonic() - started) * 1000)
            state["state"] = "scheduled"
            state["last_finished_at"] = utc_now().isoformat()
            state["last_duration_ms"] = duration_ms
            delay = max(0.1, collector.interval_seconds - duration_ms / 1000)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def _run_once(self, collector: CollectorConfig) -> None:
        started = time.monotonic()
        data = await asyncio.to_thread(
            self.engine.collect, collector, self.target_tag
        )
        current_fingerprint = fingerprint(data)
        previous = self._last_sent_fingerprint.get(collector.id)
        changed = previous != current_fingerprint
        self.status.setdefault(
            collector.id,
            {
                "type": collector.type.value,
                "interval_seconds": collector.interval_seconds,
                "state": "running",
            },
        )["last_changed"] = changed

        if (
            collector.type == CollectorType.CRASH_TRIAGE
            and changed
            and data.get("crash_total", 0) > 0
            and bool(collector.options.get("llm_enabled", True))
        ):
            data["llm"] = await asyncio.to_thread(
                self.llm.analyze,
                self.settings.cluster_id,
                self.settings.environment,
                self.process_id,
                self.target_tag,
                data,
            )

        if collector.emit_only_changes and not changed:
            return
        self._sequence += 1
        event = CollectorEvent(
            event_type="collector_result",
            cluster_id=self.settings.cluster_id,
            environment=self.settings.environment,
            process_id=self.process_id,
            flow_name=self.flow_name,
            collector_id=collector.id,
            collector_type=collector.type,
            sequence=self._sequence,
            duration_ms=int((time.monotonic() - started) * 1000),
            fingerprint=current_fingerprint,
            changed=changed,
            target_tag=self.target_tag,
            data=data,
        )
        await asyncio.to_thread(self.master.send_event, event)
        self._last_sent_fingerprint[collector.id] = current_fingerprint

    async def _send_collector_error(
        self, collector: CollectorConfig, exc: Exception
    ) -> None:
        data = {"collector_error": exc.__class__.__name__}
        error_fingerprint = fingerprint(data)
        if (
            collector.emit_only_changes
            and self._last_sent_fingerprint.get(collector.id) == error_fingerprint
        ):
            return
        self._sequence += 1
        event = CollectorEvent(
            event_type="collector_error",
            cluster_id=self.settings.cluster_id,
            environment=self.settings.environment,
            process_id=self.process_id,
            flow_name=self.flow_name,
            collector_id=collector.id,
            collector_type=collector.type,
            sequence=self._sequence,
            fingerprint=error_fingerprint,
            changed=True,
            target_tag=self.target_tag,
            data=data,
            errors=[f"{exc.__class__.__name__}: collection failed"],
        )
        await asyncio.to_thread(self.master.send_event, event)
        self._last_sent_fingerprint[collector.id] = error_fingerprint
